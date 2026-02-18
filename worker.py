# ==============================
# worker.py (الكود الكامل مع التحسينات)
# ==============================
import os
import asyncio
import re
import sys
import logging
import unicodedata
from collections import defaultdict
from datetime import datetime
from telethon import TelegramClient, events, types
from telethon.sessions import StringSession
from telethon.tl.functions.messages import ImportChatInviteRequest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ------------------------------
# الإعدادات
# ------------------------------
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
CHANNELS = os.environ.get("CHANNELS", "https://t.me/ShoofFilm,https://t.me/shoofcima")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
STRING_SESSION = os.environ.get("STRING_SESSION", "")
IMPORT_HISTORY = os.environ.get("IMPORT_HISTORY", "false").lower() == "true"
CHECK_DELETED_MESSAGES = os.environ.get("CHECK_DELETED_MESSAGES", "true").lower() == "true"
DEBUG_MODE = os.environ.get("DEBUG_MODE", "false").lower() == "true"
RESET_DATABASE = os.environ.get("RESET_DATABASE", "false").lower() == "true"
SYNC_LIMIT = int(os.environ.get("SYNC_LIMIT", "10000"))  # 0 = غير محدود
MOVIE_THRESHOLD = int(os.environ.get("MOVIE_THRESHOLD", "3"))   # إذا كان عدد الفيديوهات <= هذا الرقم، فيلم
SERIES_THRESHOLD = int(os.environ.get("SERIES_THRESHOLD", "5")) # إذا كان عدد الفيديوهات >= هذا الرقم، مسلسل

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG if DEBUG_MODE else logging.INFO
)
logger = logging.getLogger(__name__)

if not all([API_ID, API_HASH, DATABASE_URL, STRING_SESSION]):
    logger.error("❌ متغيرات مفقودة")
    sys.exit(1)

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

CHANNEL_LIST = [chan.strip() for chan in CHANNELS.split(',') if chan.strip()]

# ------------------------------
# اتصال قاعدة البيانات وإعادة التعيين إذا لزم الأمر
# ------------------------------
try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("✅ اتصال بقاعدة البيانات")

    if RESET_DATABASE:
        logger.warning("⚠️ جاري إعادة تعيين قاعدة البيانات...")
        with engine.begin() as conn:
            conn.execute(text("DROP SCHEMA public CASCADE"))
            conn.execute(text("CREATE SCHEMA public"))
        logger.info("✅ تم إعادة تعيين قاعدة البيانات")
except Exception as e:
    logger.error(f"❌ فشل الاتصال: {e}")
    sys.exit(1)

# ------------------------------
# إنشاء الجداول
# ------------------------------
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS series (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            type VARCHAR(10) DEFAULT 'series',
            normalized_name VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS episodes (
            id SERIAL PRIMARY KEY,
            series_id INTEGER REFERENCES series(id),
            season INTEGER DEFAULT 1,
            episode_number INTEGER NOT NULL,
            telegram_message_id INTEGER UNIQUE NOT NULL,
            telegram_channel_id VARCHAR(255),
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS channel_context (
            channel_id VARCHAR(255) PRIMARY KEY,
            series_name VARCHAR(255) NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_series_normalized_name ON series(normalized_name)"))
    conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_episodes_msg_id ON episodes(telegram_message_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_episodes_channel_id ON episodes(telegram_channel_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_episodes_added_at ON episodes(added_at)"))
logger.info("✅ الجداول جاهزة")

# ------------------------------
# دوال التنظيف والتطبيع
# ------------------------------
def normalize_arabic(text):
    if not text:
        return ''
    text = unicodedata.normalize('NFKD', text)
    text = re.sub(r'[\u064B-\u065F]', '', text)  # إزالة التشكيل
    text = text.replace('إ', 'ا').replace('أ', 'ا').replace('آ', 'ا').replace('ى', 'ا')
    text = text.replace('ة', 'ه')
    return text

def normalize_series_name(name):
    if not name:
        return ''
    # إزالة الكلمات الدخيلة من البداية والنهاية
    name = re.sub(r'^(مسلسل|فيلم)\s+', '', name, flags=re.UNICODE)
    name = re.sub(r'\s+(الحلقة|الموسم|الجزء)$', '', name, flags=re.UNICODE)
    name = re.sub(r'\s+\d+$', '', name)
    name = normalize_arabic(name)
    name = re.sub(r'\s+', ' ', name).strip().lower()
    return name

def clean_name_for_series(name):
    name = re.sub(r'^مسلسل\s+', '', name, flags=re.UNICODE)
    name = re.sub(r'\s+(الحلقة|الموسم)$', '', name, flags=re.UNICODE)
    name = re.sub(r'\s+\d+$', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def clean_name_for_movie(name):
    name = re.sub(r'^فيلم\s+', '', name, flags=re.UNICODE)
    name = re.sub(r'\s+الجزء\s*\d*$', '', name, flags=re.UNICODE)
    name = re.sub(r'\s+\d+$', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

# ------------------------------
# دوال مساعدة للسياق
# ------------------------------
def load_channel_context():
    context = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT channel_id, series_name FROM channel_context")).fetchall()
            for row in rows:
                context[row[0]] = row[1]
        logger.info(f"📂 تم تحميل سياق {len(context)} قناة من قاعدة البيانات")
    except Exception as e:
        logger.error(f"❌ فشل تحميل السياق: {e}")
    return context

def save_channel_context(channel_id, series_name):
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO channel_context (channel_id, series_name)
                    VALUES (:chan, :name)
                    ON CONFLICT (channel_id) DO UPDATE SET series_name = :name, updated_at = CURRENT_TIMESTAMP
                """),
                {"chan": channel_id, "name": series_name}
            )
        logger.debug(f"💾 تم حفظ السياق للقناة {channel_id}: {series_name}")
    except Exception as e:
        logger.error(f"❌ فشل حفظ السياق: {e}")

# ------------------------------
# دالة متطورة للكشف عن الفيديو
# ------------------------------
def has_video_media(msg):
    """التحقق مما إذا كانت الرسالة تحتوي على فيديو حقيقي"""
    if msg.video:
        return True
    if msg.document:
        mime = msg.document.mime_type or ''
        if mime.startswith('video/'):
            return True
        if msg.document.attributes:
            for attr in msg.document.attributes:
                if isinstance(attr, types.DocumentAttributeFilename):
                    ext = os.path.splitext(attr.file_name)[-1].lower()
                    if ext in ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.3gp']:
                        return True
                elif isinstance(attr, types.DocumentAttributeVideo):
                    return True
        if msg.document.size > 5 * 1024 * 1024 and 'octet-stream' in mime:
            return True
    if msg.media and hasattr(msg.media, 'document'):
        doc = msg.media.document
        mime = doc.mime_type or ''
        if mime.startswith('video/'):
            return True
        for attr in doc.attributes:
            if isinstance(attr, types.DocumentAttributeFilename):
                ext = os.path.splitext(attr.file_name)[-1].lower()
                if ext in ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.3gp']:
                    return True
            elif isinstance(attr, types.DocumentAttributeVideo):
                return True
        if doc.size > 5 * 1024 * 1024 and 'octet-stream' in mime:
            return True
    return False

# ------------------------------
# دالة لتحديد نوع المحتوى بناءً على النص وعدد الحلقات الموجودة
# ------------------------------
def determine_content_type(name, existing_count=0):
    """
    تحديد نوع المحتوى بناءً على:
    - وجود كلمات مفتاحية في الاسم (مسلسل، فيلم، الموسم، الحلقة، الجزء)
    - عدد الحلقات الموجودة مسبقاً لهذا الاسم
    """
    lower_name = name.lower()
    # كلمات مفتاحية للمسلسل
    series_keywords = ['مسلسل', 'الحلقة', 'الموسم', 'حلقة', 'موسم']
    # كلمات مفتاحية للفيلم
    movie_keywords = ['فيلم', 'الجزء', 'جزء']

    # التحقق من وجود كلمات صريحة
    has_series_word = any(kw in lower_name for kw in series_keywords)
    has_movie_word = any(kw in lower_name for kw in movie_keywords)

    if has_series_word and not has_movie_word:
        return 'series'
    if has_movie_word and not has_series_word:
        return 'movie'

    # إذا كانت الكلمات غير حاسمة، نستخدم عدد الحلقات الموجودة
    if existing_count >= SERIES_THRESHOLD:
        return 'series'
    elif existing_count <= MOVIE_THRESHOLD:
        return 'movie'
    else:
        # في المنطقة الرمادية (4-5 حلقات) نفضل المسلسل
        return 'series'

# ------------------------------
# دالة التحليل الذكية
# ------------------------------
def parse_content_info(msg_text, channel_id, has_video, existing_count=0):
    """
    تحليل نص الرسالة لاستخراج اسم المحتوى ورقم الموسم والحلقة.
    تعيد (name, content_type, season, episode) أو (None, None, None, None) إذا لم يكن هناك فيديو.
    """
    if not msg_text or not has_video:
        return None, None, None, None

    original_text = msg_text.strip()
    text = original_text

    logger.debug(f"محاولة تحليل: {original_text[:100]}")

    # إزالة الكلمات الشائعة من البداية
    common_prefixes = ['مشاهدة', 'تحميل', 'الآن', 'مسلسل', 'فيلم', 'شاهد', 'مترجم', 'حلقة', 'المسلسل']
    for prefix in common_prefixes:
        if text.startswith(prefix):
            text = text[len(prefix):].strip()
            text = re.sub(r'^[\s:-]+', '', text)

    season = 1
    episode = 1
    name = text
    content_type = 'movie'  # مؤقت

    # محاولة استخراج الموسم والحلقة بأنماط Regex
    patterns = [
        (r'^(.*?)\s*[Ss](\d+)[Ee](\d+)$', lambda m: (m.group(1).strip(), int(m.group(2)), int(m.group(3)))),
        (r'^(.*?)\s*[Ss](\d+)[Ee](\d+)', lambda m: (m.group(1).strip(), int(m.group(2)), int(m.group(3)))),
        (r'(.*?)\s*الموسم\s*[:_-]?\s*(\d+)\s*الحلقة\s*[:_-]?\s*(\d+)', lambda m: (m.group(1).strip(), int(m.group(2)), int(m.group(3)))),
        (r'(.*?)\s*الحلقة\s*[:_-]?\s*(\d+)\s*من\s*الموسم\s*[:_-]?\s*(\d+)', lambda m: (m.group(1).strip(), int(m.group(3)), int(m.group(2)))),
        (r'(.*?)\s*الموسم\s*[:_-]?\s*(\d+)\s*-\s*(\d+)', lambda m: (m.group(1).strip(), int(m.group(2)), int(m.group(3)))),
        (r'(.*?)\s*م(\d+)\s*ح(\d+)', lambda m: (m.group(1).strip(), int(m.group(2)), int(m.group(3)))),
        (r'(.*?)\s*الحلقة\s*[:_-]?\s*(\d+)', lambda m: (m.group(1).strip(), 1, int(m.group(2)))),
        (r'^(.*?)\s+(\d+)[-\s]+(\d+)$', lambda m: (m.group(1).strip(), int(m.group(2)), int(m.group(3)))),
        (r'^(.*?)\s+(\d+)$', lambda m: (m.group(1).strip(), 1, int(m.group(2)))),
        (r'(.*?)\s*الجزء\s*[:_-]?\s*(\d+)', lambda m: (m.group(1).strip(), int(m.group(2)), 1)),
    ]

    for pattern, extractor in patterns:
        match = re.search(pattern, text, re.UNICODE)
        if match:
            try:
                name, season, episode = extractor(match)
                # إذا تم العثور على نمط، فهو مسلسل غالباً
                content_type = 'series'
                logger.debug(f"نمط مطابق: {pattern} -> {name} م{season} ح{episode}")
                break
            except:
                continue

    # تنظيف الاسم
    name = re.sub(r'\s+', ' ', name).strip()
    name = re.sub(r'\s+\d+$', '', name)  # إزالة أرقام زائدة

    if not name:
        name = original_text[:200]

    # تحديد النوع باستخدام الدالة المساعدة
    content_type = determine_content_type(name, existing_count)

    logger.debug(f"نتيجة التحليل: '{original_text[:50]}...' -> {name}, {content_type}, S{season}E{episode}")
    return name, content_type, season, episode

# ------------------------------
# دوال التعامل مع القنوات
# ------------------------------
async def get_channel_entity(client, channel_input):
    try:
        return await client.get_entity(channel_input)
    except Exception as e:
        logger.warning(f"⚠️ لا يمكن الوصول {channel_input}: {e}")
        if isinstance(channel_input, str) and channel_input.startswith('https://t.me/+'):
            try:
                invite = channel_input.split('+')[-1]
                await client(ImportChatInviteRequest(invite))
                return await client.get_entity(channel_input)
            except:
                return None
        return None

def get_existing_episode_count(name, content_type=None):
    """الحصول على عدد الحلقات الموجودة لاسم معين (باستخدام normalized_name)"""
    try:
        normalized = normalize_series_name(name)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM series WHERE normalized_name = :norm"),
                {"norm": normalized}
            ).fetchone()
            if row:
                count = conn.execute(
                    text("SELECT COUNT(*) FROM episodes WHERE series_id = :sid"),
                    {"sid": row[0]}
                ).scalar()
                return count
        return 0
    except Exception as e:
        logger.error(f"خطأ في get_existing_episode_count: {e}")
        return 0

def save_to_database(name, content_type, season, episode, msg_id, channel_id):
    try:
        normalized = normalize_series_name(name)
        with engine.begin() as conn:
            # البحث عن مسلسل موجود بنفس normalized_name
            row = conn.execute(
                text("SELECT id FROM series WHERE normalized_name = :norm AND type = :typ"),
                {"norm": normalized, "typ": content_type}
            ).fetchone()
            if not row:
                # قد يكون موجوداً بنوع مختلف، نحاول البحث بدون نوع
                row = conn.execute(
                    text("SELECT id, type FROM series WHERE normalized_name = :norm"),
                    {"norm": normalized}
                ).fetchone()
                if row:
                    # إذا وجد بنوع مختلف، قد نحتاج لتحديث النوع إذا كان هناك تضارب
                    # سنستخدم النوع الجديد إذا كان أكثر دقة (مثلاً إذا كان القديم movie والجديد series)
                    old_type = row[1]
                    if old_type != content_type:
                        # إذا كان القديم movie والجديد series (أو العكس) نختار الأكثر شيوعاً
                        # هنا نعطي الأولوية لـ series إذا كان عدد الحلقات كبيراً
                        existing_count = conn.execute(
                            text("SELECT COUNT(*) FROM episodes WHERE series_id = :sid"),
                            {"sid": row[0]}
                        ).scalar()
                        if content_type == 'series' and existing_count >= SERIES_THRESHOLD:
                            # تحديث النوع
                            conn.execute(
                                text("UPDATE series SET type = :typ WHERE id = :sid"),
                                {"typ": content_type, "sid": row[0]}
                            )
                            logger.info(f"🔄 تحديث نوع {name} من {old_type} إلى {content_type}")
                        # نستخدم نفس الـ id
                        row = (row[0],)
                else:
                    # إنشاء جديد
                    row = conn.execute(
                        text("INSERT INTO series (name, normalized_name, type) VALUES (:name, :norm, :typ) RETURNING id"),
                        {"name": name, "norm": normalized, "typ": content_type}
                    ).fetchone()
            sid = row[0]

            # إدراج الحلقة
            inserted = conn.execute(
                text("""
                    INSERT INTO episodes (series_id, season, episode_number, telegram_message_id, telegram_channel_id)
                    VALUES (:sid, :season, :ep, :msg, :chan)
                    ON CONFLICT (telegram_message_id) DO NOTHING
                    RETURNING id
                """),
                {"sid": sid, "season": season, "ep": episode, "msg": msg_id, "chan": channel_id}
            ).fetchone()
            if inserted:
                logger.info(f"✅ جديد: {name} - م{season} ح{episode} من {channel_id}")
                return True
            else:
                logger.debug(f"⚠️ موجودة: {msg_id}")
                return False
    except Exception as e:
        logger.exception(f"❌ خطأ في الحفظ للرسالة {msg_id}: {e}")
        return False

def delete_from_database(msg_id):
    try:
        with engine.begin() as conn:
            ep = conn.execute(
                text("SELECT series_id FROM episodes WHERE telegram_message_id = :msg"),
                {"msg": msg_id}
            ).fetchone()
            if not ep:
                return False
            sid = ep[0]
            conn.execute(text("DELETE FROM episodes WHERE telegram_message_id = :msg"), {"msg": msg_id})
            remaining = conn.execute(
                text("SELECT COUNT(*) FROM episodes WHERE series_id = :sid"),
                {"sid": sid}
            ).scalar()
            if remaining == 0:
                conn.execute(text("DELETE FROM series WHERE id = :sid"), {"sid": sid})
                logger.info(f"🗑️ تم حذف المسلسل بالكامل (آخر حلقة {msg_id})")
            else:
                logger.info(f"🗑️ تم حذف حلقة {msg_id}")
            return True
    except Exception as e:
        logger.error(f"❌ خطأ في الحذف: {e}")
        return False

def clean_orphan_series():
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                DELETE FROM series
                WHERE id NOT IN (SELECT DISTINCT series_id FROM episodes)
                RETURNING id, name, type
            """)).fetchall()
            if result:
                for r in result:
                    logger.info(f"🧹 تم حذف {r[2]} بدون حلقات: {r[1]} (ID: {r[0]})")
                logger.info(f"✅ تم تنظيف {len(result)} مسلسل/فيلم بدون حلقات")
    except Exception as e:
        logger.error(f"❌ خطأ في تنظيف السلسلة اليتيمة: {e}")

def fix_misclassified_series():
    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT s.id, s.name, COUNT(e.id) as ep_count
                FROM series s
                JOIN episodes e ON s.id = e.series_id
                WHERE s.type = 'movie'
                GROUP BY s.id, s.name
                HAVING COUNT(e.id) > 1
            """)).fetchall()
            if rows:
                for row in rows:
                    sid, name, count = row
                    conn.execute(
                        text("UPDATE series SET type = 'series' WHERE id = :sid"),
                        {"sid": sid}
                    )
                    logger.info(f"🔄 تم تصحيح {name} (ID: {sid}) من فيلم إلى مسلسل (لديه {count} حلقات)")
                logger.info(f"✅ تم تصحيح {len(rows)} مسلسل كان مصنف خطأ كفيلم")
    except Exception as e:
        logger.error(f"❌ خطأ في تصحيح التصنيف: {e}")

# ------------------------------
# مزامنة القنوات
# ------------------------------
async def sync_channel_messages(client, channel):
    """مزامنة آخر SYNC_LIMIT رسالة"""
    chan_id = f"@{channel.username}" if channel.username else str(channel.id)
    limit = None if SYNC_LIMIT <= 0 else SYNC_LIMIT
    logger.info(f"\n🔄 مزامنة {channel.title} ({chan_id})" + (f" بحد أقصى {SYNC_LIMIT} رسالة" if limit else " بدون حد"))

    messages = []
    async for msg in client.iter_messages(channel, limit=limit):
        if msg.text or msg.media:
            messages.append(msg)
    messages.reverse()
    logger.info(f"📊 تم جلب {len(messages)} رسالة")

    # تجميع الرسائل حسب grouped_id لمعالجة الألبومات
    grouped = defaultdict(list)
    for msg in messages:
        if msg.grouped_id:
            grouped[msg.grouped_id].append(msg)

    with engine.connect() as conn:
        stored = conn.execute(
            text("SELECT telegram_message_id FROM episodes WHERE telegram_channel_id = :chan"),
            {"chan": chan_id}
        ).fetchall()
    stored_set = {r[0] for r in stored}

    new = 0
    skipped = 0
    failed_parse = 0
    no_video = 0
    processed_ids = set()

    # معالجة المجموعات أولاً
    for group_id, group_msgs in grouped.items():
        video_msg = next((m for m in group_msgs if has_video_media(m)), None)
        if not video_msg:
            continue
        if video_msg.id in stored_set or video_msg.id in processed_ids:
            continue
        text_msg = next((m for m in group_msgs if m.text and not has_video_media(m)), None)
        content_text = text_msg.text if text_msg else video_msg.text or ""
        has_vid = True
        # قبل التحليل، نحتاج لمعرفة عدد الحلقات الموجودة لهذا الاسم (تقريبي)
        # لكن الاسم غير معروف بعد، لذلك نمرر 0
        name, typ, season, ep = parse_content_info(content_text, chan_id, has_vid, 0)
        if not name:
            name = f"Unnamed_Group_{group_id}"
            typ = "movie"
            season = 1
            ep = 1
            logger.debug(f"استخدام اسم افتراضي للمجموعة {group_id}")
        # بعد الحصول على الاسم، نستطيع تحديث العدد
        existing = get_existing_episode_count(name)
        # يمكن إعادة التحليل بالعدد الصحيح، لكننا سنستخدم ما لدينا
        if save_to_database(name, typ, season, ep, video_msg.id, chan_id):
            new += 1
            processed_ids.add(video_msg.id)

    # معالجة الرسائل الفردية
    for msg in messages:
        if msg.id in processed_ids or msg.id in stored_set:
            skipped += 1
            continue

        has_vid = has_video_media(msg)
        if not has_vid:
            no_video += 1
            # تحديث السياق إذا كان النص يبدو كاسم مسلسل (بدون أرقام)
            if msg.text:
                name, typ, _, _ = parse_content_info(msg.text, chan_id, False, 0)
                if name and typ == 'series' and not re.search(r'\d+', name):
                    save_channel_context(chan_id, name)
            continue

        # هنا لدينا فيديو، نحتاج لمعرفة عدد الحلقات الموجودة لهذا الاسم
        # لكننا لا نعرف الاسم بعد. سنحلل أولاً بدون العدد
        name, typ, season, ep = parse_content_info(msg.text or "", chan_id, has_vid, 0)
        if name:
            # نحصل على العدد الفعلي
            existing = get_existing_episode_count(name)
            # إعادة تحليل بالعدد الصحيح (اختياري، لكننا سنستخدم النوع من التحليل الأول)
            # يمكن تحديث النوع بناءً على existing
            corrected_type = determine_content_type(name, existing)
            if corrected_type != typ:
                logger.debug(f"تصحيح النوع لـ {name} من {typ} إلى {corrected_type} بناءً على عدد الحلقات ({existing})")
                typ = corrected_type
        else:
            # فشل التحليل، نستخدم اسم افتراضي
            name = f"Unnamed_{msg.id}"
            typ = "movie"
            season = 1
            ep = 1

        if save_to_database(name, typ, season, ep, msg.id, chan_id):
            new += 1
            stored_set.add(msg.id)
        else:
            with engine.connect() as conn2:
                exists = conn2.execute(
                    text("SELECT 1 FROM episodes WHERE telegram_message_id = :mid"),
                    {"mid": msg.id}
                ).scalar()
                if exists:
                    skipped += 1
                else:
                    failed_parse += 1
                    logger.error(f"❌ فشل إدراج {msg.id} (غير معروف السبب)")

    logger.info(f"✅ {channel.title}: {new} جديدة, {skipped} موجودة, {failed_parse} فشل تحليل, {no_video} بدون فيديو")

async def import_channel_history(client, channel):
    await sync_channel_messages(client, channel)  # مع SYNC_LIMIT <= 0 ستجلب الكل

async def force_sync_all_messages(client, channel):
    """إعادة معالجة جميع الرسائل في القناة بغض النظر عن وجودها (فحص إجباري)"""
    chan_id = f"@{channel.username}" if channel.username else str(channel.id)
    logger.info(f"\n🔁 فحص إجباري لـ {channel.title} ({chan_id}) - جلب كل الرسائل")

    all_msgs = []
    async for msg in client.iter_messages(channel, limit=None):
        if msg.text or msg.media:
            all_msgs.append(msg)
    logger.info(f"📊 تم جلب {len(all_msgs)} رسالة")

    with engine.connect() as conn:
        stored = conn.execute(
            text("SELECT telegram_message_id FROM episodes WHERE telegram_channel_id = :chan"),
            {"chan": chan_id}
        ).fetchall()
    stored_set = {r[0] for r in stored}

    new = 0
    for msg in all_msgs:
        if msg.id in stored_set:
            continue
        if has_video_media(msg):
            name, typ, season, ep = parse_content_info(msg.text or "", chan_id, True, 0)
            if not name:
                name = f"Unnamed_{msg.id}"
                typ = "movie"
                season = 1
                ep = 1
            if save_to_database(name, typ, season, ep, msg.id, chan_id):
                new += 1

    logger.info(f"✅ الفحص الإجباري لـ {channel.title}: تمت إضافة {new} حلقة جديدة")

async def check_deleted_messages(client, channel):
    chan_id = f"@{channel.username}" if channel.username else str(channel.id)
    logger.info(f"\n🔍 فحص المحذوفات في {channel.title}")
    try:
        with engine.connect() as conn:
            stored = conn.execute(
                text("SELECT telegram_message_id FROM episodes WHERE telegram_channel_id = :chan"),
                {"chan": chan_id}
            ).fetchall()
        stored_ids = [r[0] for r in stored]
        if not stored_ids:
            return
        current_ids = []
        async for msg in client.iter_messages(channel, limit=1000):
            current_ids.append(msg.id)
        deleted = [sid for sid in stored_ids if sid not in current_ids]
        if deleted:
            logger.info(f"🗑️ {len(deleted)} رسالة محذوفة")
            for mid in deleted:
                delete_from_database(mid)
        else:
            logger.info("✅ لا توجد محذوفات")
    except Exception as e:
        logger.error(f"❌ خطأ في الفحص: {e}")

# ------------------------------
# الدالة الرئيسية
# ------------------------------
async def monitor_channels():
    logger.info(f"مراقبة {len(CHANNEL_LIST)} قناة")
    client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
    await client.start()
    logger.info("✅ متصل بـ Telegram")

    channels = []
    for inp in CHANNEL_LIST:
        ch = await get_channel_entity(client, inp)
        if ch:
            channels.append(ch)
            logger.info(f"✅ {ch.title}")
    if not channels:
        logger.error("لا توجد قنوات صالحة")
        return

    # تحميل السياق
    global series_context
    series_context = load_channel_context()

    # مزامنة أولية
    for ch in channels:
        await sync_channel_messages(client, ch)

    # استيراد كامل إذا مفعل
    if IMPORT_HISTORY and SYNC_LIMIT > 0:
        for ch in channels:
            await import_channel_history(client, ch)

    # فحص إجباري إذا مفعل
    if os.environ.get("FORCE_SYNC", "false").lower() == "true":
        for ch in channels:
            await force_sync_all_messages(client, ch)

    clean_orphan_series()
    fix_misclassified_series()

    if CHECK_DELETED_MESSAGES:
        for ch in channels:
            await check_deleted_messages(client, ch)

    @client.on(events.NewMessage(chats=channels))
    async def handler(event):
        msg = event.message
        if msg.text or msg.media:
            chan_id = f"@{msg.chat.username}" if msg.chat.username else str(msg.chat.id)
            has_vid = has_video_media(msg)
            if has_vid:
                # نحتاج لمعرفة عدد الحلقات الموجودة لهذا الاسم قبل التخزين
                # سنحلل بدون العدد أولاً
                name, typ, season, ep = parse_content_info(msg.text or "", chan_id, has_vid, 0)
                if name:
                    existing = get_existing_episode_count(name)
                    corrected_type = determine_content_type(name, existing)
                    if corrected_type != typ:
                        typ = corrected_type
                else:
                    name = f"Unnamed_{msg.id}"
                    typ = "movie"
                    season = 1
                    ep = 1
                save_to_database(name, typ, season, ep, msg.id, chan_id)
            else:
                # رسالة نصية بدون فيديو، قد تكون سياق
                if msg.text:
                    name, typ, _, _ = parse_content_info(msg.text, chan_id, False, 0)
                    if name and typ == 'series' and not re.search(r'\d+', name):
                        save_channel_context(chan_id, name)
                        series_context[chan_id] = name

    @client.on(events.MessageDeleted(chats=channels))
    async def delete_handler(event):
        for mid in event.deleted_ids:
            delete_from_database(mid)

    logger.info("🎯 في انتظار الأحداث...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(monitor_channels())
