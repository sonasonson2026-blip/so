import os
import asyncio
import re
import sys
import logging
import unicodedata
from collections import defaultdict
from datetime import datetime
from telethon import TelegramClient, events
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
# اتصال قاعدة البيانات
# ------------------------------
try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("✅ اتصال بقاعدة البيانات")
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
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_series_normalized_name ON series(normalized_name)"))
    conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_episodes_msg_id ON episodes(telegram_message_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_episodes_channel_id ON episodes(telegram_channel_id)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_episodes_added_at ON episodes(added_at)"))
logger.info("✅ الجداول جاهزة")

# تحديث الأسماء المقيسة للبيانات القديمة (إن وجدت)
with engine.begin() as conn:
    rows = conn.execute(text("SELECT id, name FROM series WHERE normalized_name IS NULL")).fetchall()
    for row in rows:
        norm = normalize_series_name(row[1])
        conn.execute(text("UPDATE series SET normalized_name = :norm WHERE id = :id"), {"norm": norm, "id": row[0]})
    if rows:
        logger.info(f"✅ تم تحديث {len(rows)} اسماً مقيساً")

# ------------------------------
# دوال التنظيف والتطبيع
# ------------------------------
def normalize_arabic(text):
    if not text:
        return ''
    text = unicodedata.normalize('NFKD', text)
    text = re.sub(r'[\u064B-\u065F]', '', text)
    text = text.replace('إ', 'ا').replace('أ', 'ا').replace('آ', 'ا').replace('ى', 'ا')
    text = text.replace('ة', 'ه')
    return text

def normalize_series_name(name):
    if not name:
        return ''
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
# سياق المسلسلات لكل قناة
# ------------------------------
series_context = defaultdict(lambda: None)

# ------------------------------
# دالة التحليل المتقدمة (معدلة لتجنب تعارض الاسم مع دالة SQLAlchemy text)
# ------------------------------
def parse_content_info(msg_text, channel_id, has_video):
    """
    تحليل نص الرسالة.
    - إذا كان هناك فيديو: نحاول استخراج المعلومات (مسلسل/فيلم).
    - إذا لم يكن هناك فيديو: نخزن الاسم في السياق فقط إذا كان يبدو كاسم مسلسل.
    """
    if not msg_text:
        return None, None, None, None

    msg_text = msg_text.strip()
    lower_text = msg_text.lower()

    # كلمات مفتاحية
    series_keywords = ['حلقة', 'الحلقة', 'موسم', 'الموسم', 'season', 'episode']
    movie_keywords = ['فيلم', 'الجزء', 'part']

    # تحديد وجود كلمات مسلسل/فيلم
    is_series_word = 'مسلسل' in lower_text
    is_movie_word = 'فيلم' in lower_text

    # إذا كان هناك فيديو
    if has_video:
        # محاولة التعرف على المسلسل بأنماطه
        # 1. اسم + الموسم X + الحلقة Y
        match = re.search(r'^(.*?)\s+الموسم\s+(\d+)\s+الحلقة\s+(\d+)$', msg_text, re.UNICODE)
        if match:
            name = clean_name_for_series(match.group(1))
            season = int(match.group(2))
            episode = int(match.group(3))
            return name, 'series', season, episode

        # 2. اسم + SXE
        match = re.search(r'^(.*?)\s+[Ss](\d+)[Ee](\d+)$', msg_text)
        if match:
            name = clean_name_for_series(match.group(1))
            season = int(match.group(2))
            episode = int(match.group(3))
            return name, 'series', season, episode

        # 3. اسم + الحلقة X من الموسم Y
        match = re.search(r'^(.*?)\s+الحلقة\s+(\d+)\s+من\s+الموسم\s+(\d+)$', msg_text, re.UNICODE)
        if match:
            name = clean_name_for_series(match.group(1))
            episode = int(match.group(2))
            season = int(match.group(3))
            return name, 'series', season, episode

        # 4. اسم + الموسم X - Y (حلقة)
        match = re.search(r'^(.*?)\s+الموسم\s+(\d+)[-\s]+(\d+)$', msg_text, re.UNICODE)
        if match:
            name = clean_name_for_series(match.group(1))
            season = int(match.group(2))
            episode = int(match.group(3))
            return name, 'series', season, episode

        # 5. اسم + الحلقة X (بدون موسم -> موسم 1)
        match = re.search(r'^(.*?)\s+الحلقة\s+(\d+)$', msg_text, re.UNICODE)
        if match:
            name = clean_name_for_series(match.group(1))
            episode = int(match.group(2))
            return name, 'series', 1, episode

        # 6. إذا كان النص يحتوي على كلمة "فيلم" صراحة
        if is_movie_word or any(kw in lower_text for kw in movie_keywords):
            # فيلم بأنماطه
            match = re.search(r'فيلم\s+(.+?)\s+الجزء\s+(\d+)', msg_text, re.UNICODE)
            if match:
                name = clean_name_for_movie(match.group(1))
                part = int(match.group(2))
                return name, 'movie', part, 1
            match = re.search(r'فيلم\s+(.+?)\s+(\d+)$', msg_text, re.UNICODE)
            if match:
                name = clean_name_for_movie(match.group(1))
                part = int(match.group(2))
                return name, 'movie', part, 1
            # فيلم بدون رقم
            name = clean_name_for_movie(re.sub(r'فيلم', '', msg_text, flags=re.UNICODE))
            return name, 'movie', 1, 1

        # 7. إذا كان النص يتكون أساسًا من أرقام (قد يكون حلقة من مسلسل سابق)
        numbers = re.findall(r'\d+', msg_text)
        if numbers and series_context[channel_id] is not None:
            # يوجد سياق مسلسل لهذه القناة
            if len(numbers) >= 2:
                season = int(numbers[0])
                episode = int(numbers[1])
            else:
                season = 1
                episode = int(numbers[0])
            logger.debug(f"استخدام السياق: {series_context[channel_id]} - م{season} ح{episode}")
            return series_context[channel_id], 'series', season, episode

        # 8. إذا كان النص يحتوي على كلمة "مسلسل" (حتى بدون كلمات حلقة/موسم)
        if is_series_word:
            name = clean_name_for_series(msg_text)
            return name, 'series', 1, 1

        # 9. نص عادي ينتهي برقم (قد يكون جزء من مسلسل)
        # نتحقق مما إذا كان هناك مسلسل بنفس الاسم الأساسي في قاعدة البيانات
        base_name = re.sub(r'\s+\d+$', '', msg_text).strip()
        if base_name and base_name != msg_text:
            # استعلام عن وجود مسلسل بهذا الاسم الأساسي
            with engine.connect() as conn:
                exists = conn.execute(
                    text("SELECT 1 FROM series WHERE name ILIKE :pat AND type='series' LIMIT 1"),
                    {"pat": f"%{base_name}%"}
                ).scalar()
            if exists:
                # يوجد مسلسل مشابه، نصنفها كحلقة
                name = clean_name_for_series(base_name)
                # استخراج الرقم من النهاية
                num_match = re.search(r'(\d+)$', msg_text)
                if num_match:
                    episode = int(num_match.group(1))
                    return name, 'series', 1, episode

        # 10. افتراضياً، نعتبره فيلم
        name = clean_name_for_movie(msg_text)
        return name, 'movie', 1, 1

    # إذا لم يكن هناك فيديو (بوست نصي فقط)
    else:
        # إذا كان النص لا يحتوي على كلمات مفتاحية للحلقات، قد يكون اسم مسلسل جديد
        if not any(kw in lower_text for kw in series_keywords + movie_keywords):
            # نعتبره اسماً لمسلسل (أو فيلم) سيظهر لاحقاً
            if is_series_word or (not is_movie_word and not re.search(r'\d', msg_text)):
                name = clean_name_for_series(msg_text)
                if name:
                    series_context[channel_id] = name
                    logger.info(f"📝 تم تسجيل سياق مسلسل: {name} في {channel_id}")
            # لا نرجع بيانات لأن هذا البوست ليس له فيديو
        return None, None, None, None

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

def save_to_database(name, content_type, season, episode, msg_id, channel_id):
    try:
        with engine.begin() as conn:
            normalized = normalize_series_name(name)
            # البحث عن المسلسل
            row = conn.execute(
                text("SELECT id FROM series WHERE normalized_name = :norm AND type = :typ"),
                {"norm": normalized, "typ": content_type}
            ).fetchone()
            if not row:
                # بحث باستخدام ILIKE (أول 3 كلمات)
                words = name.split()[:3]
                if words:
                    like = '%' + '%'.join(words) + '%'
                    row = conn.execute(
                        text("SELECT id FROM series WHERE name ILIKE :pat AND type = :typ LIMIT 1"),
                        {"pat": like, "typ": content_type}
                    ).fetchone()
            if not row:
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
        logger.error(f"❌ خطأ في الحفظ: {e}")
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
    """حذف المسلسلات/الأفلام التي ليس لها أي حلقات (تم إنشاؤها من بوست تعريف ولم تأتِ حلقات)"""
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
    """
    تصحيح المسلسلات التي تم تصنيفها خطأ كأفلام (movie) ولكن لديها أكثر من حلقة.
    نبحث عن أي series type='movie' لديه عدة حلقات، ونحول type إلى 'series'.
    """
    try:
        with engine.begin() as conn:
            # نجد الأفلام التي لديها أكثر من حلقة
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
    chan_id = f"@{channel.username}" if channel.username else str(channel.id)
    logger.info(f"\n🔄 مزامنة {channel.title} ({chan_id})")

    # جلب آخر 1000 رسالة
    messages = []
    async for msg in client.iter_messages(channel, limit=1000):
        if msg.text:
            messages.append(msg)
    messages.reverse()  # أقدم أولاً لبناء السياق

    # معرفات المخزنة
    with engine.connect() as conn:
        stored = conn.execute(
            text("SELECT telegram_message_id FROM episodes WHERE telegram_channel_id = :chan"),
            {"chan": chan_id}
        ).fetchall()
    stored_set = {r[0] for r in stored}

    new = 0
    skipped = 0
    failed = 0

    for msg in messages:
        if msg.id in stored_set:
            skipped += 1
            continue

        has_video = msg.video or (msg.document and msg.document.mime_type and msg.document.mime_type.startswith('video/'))
        name, typ, season, ep = parse_content_info(msg.text, chan_id, has_video)

        if name and typ and ep and has_video:
            if save_to_database(name, typ, season, ep, msg.id, chan_id):
                new += 1
                stored_set.add(msg.id)
            else:
                # فشل - تحقق من وجودها
                with engine.connect() as conn2:
                    exists = conn2.execute(
                        text("SELECT 1 FROM episodes WHERE telegram_message_id = :mid"),
                        {"mid": msg.id}
                    ).scalar()
                    if exists:
                        skipped += 1
                    else:
                        failed += 1
                        logger.error(f"❌ فشل إدراج {msg.id}")
        else:
            if not has_video:
                logger.debug(f"📝 رسالة تعريف (بدون فيديو): {msg.id}")
            else:
                failed += 1

    logger.info(f"✅ {channel.title}: {new} جديدة, {skipped} موجودة, {failed} فشل")

async def import_channel_history(client, channel):
    chan_id = f"@{channel.username}" if channel.username else str(channel.id)
    logger.info(f"\n📂 استيراد كامل {channel.title}")
    all_msgs = []
    async for msg in client.iter_messages(channel, limit=None):
        if msg.text:
            all_msgs.append(msg)
    all_msgs.reverse()
    logger.debug(f"📊 {len(all_msgs)} رسالة")

    with engine.connect() as conn:
        stored = conn.execute(
            text("SELECT telegram_message_id FROM episodes WHERE telegram_channel_id = :chan"),
            {"chan": chan_id}
        ).fetchall()
    stored_set = {r[0] for r in stored}

    new = 0
    skipped = 0
    failed = 0

    for msg in all_msgs:
        if msg.id in stored_set:
            skipped += 1
            continue

        has_video = msg.video or (msg.document and msg.document.mime_type and msg.document.mime_type.startswith('video/'))
        name, typ, season, ep = parse_content_info(msg.text, chan_id, has_video)

        if name and typ and ep and has_video:
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
                        failed += 1
                        logger.error(f"❌ فشل إدراج {msg.id}")
        else:
            if not has_video:
                logger.debug(f"📝 رسالة تعريف: {msg.id}")
            else:
                failed += 1

    logger.info(f"📥 {channel.title}: {new} جديدة, {skipped} موجودة, {failed} فشل")

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

    # إعادة تعيين السياق
    global series_context
    series_context.clear()

    # مزامنة أولية
    for ch in channels:
        await sync_channel_messages(client, ch)

    # استيراد كامل إذا مفعل
    if IMPORT_HISTORY:
        for ch in channels:
            await import_channel_history(client, ch)

    # تنظيف المسلسلات بدون حلقات
    clean_orphan_series()

    # تصحيح التصنيف الخاطئ (مسلسلات في الأفلام)
    fix_misclassified_series()

    # فحص المحذوفات
    if CHECK_DELETED_MESSAGES:
        for ch in channels:
            await check_deleted_messages(client, ch)

    # مراقبة الأحداث
    @client.on(events.NewMessage(chats=channels))
    async def handler(event):
        msg = event.message
        if msg.text:
            chan_id = f"@{msg.chat.username}" if msg.chat.username else str(msg.chat.id)
            has_video = msg.video or (msg.document and msg.document.mime_type and msg.document.mime_type.startswith('video/'))
            name, typ, season, ep = parse_content_info(msg.text, chan_id, has_video)
            if name and typ and ep and has_video:
                save_to_database(name, typ, season, ep, msg.id, chan_id)

    @client.on(events.MessageDeleted(chats=channels))
    async def delete_handler(event):
        for mid in event.deleted_ids:
            delete_from_database(mid)

    logger.info("🎯 في انتظار الأحداث...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(monitor_channels())
