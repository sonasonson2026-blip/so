import os
import asyncio
import re
import sys
import logging
import unicodedata
from datetime import datetime
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import Message, Channel
from telethon.tl.functions.messages import ImportChatInviteRequest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ==============================
# 1. إعدادات التهيئة من متغيرات البيئة
# ==============================
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
CHANNELS = os.environ.get("CHANNELS", "https://t.me/ShoofFilm,https://t.me/shoofcima")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
STRING_SESSION = os.environ.get("STRING_SESSION", "")
IMPORT_HISTORY = os.environ.get("IMPORT_HISTORY", "false").lower() == "true"
CHECK_DELETED_MESSAGES = os.environ.get("CHECK_DELETED_MESSAGES", "true").lower() == "true"
DEBUG_MODE = os.environ.get("DEBUG_MODE", "false").lower() == "true"

# إعداد logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG if DEBUG_MODE else logging.INFO
)
logger = logging.getLogger(__name__)

# تحقق من وجود المتغيرات الأساسية
if not all([API_ID, API_HASH, DATABASE_URL, STRING_SESSION]):
    logger.error("❌ واحد أو أكثر من المتغيرات التالية مفقود: API_ID, API_HASH, DATABASE_URL, STRING_SESSION")
    sys.exit(1)

# إصلاح رابط قاعدة البيانات
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# تقسيم القنوات إلى قائمة
CHANNEL_LIST = [chan.strip() for chan in CHANNELS.split(',') if chan.strip()]

# ==============================
# 2. إعداد الاتصال بقاعدة البيانات
# ==============================
try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("✅ تم الاتصال بقاعدة البيانات بنجاح.")
except Exception as e:
    logger.error(f"❌ فشل الاتصال بقاعدة البيانات: {e}")
    sys.exit(1)

# ==============================
# 3. دوال مساعدة للتطبيع والتنظيف (تُعرف قبل استخدامها)
# ==============================
def normalize_arabic(text):
    """إزالة التشكيل والحركات وتوحيد أشكال الألف."""
    if not text:
        return ''
    text = unicodedata.normalize('NFKD', text)
    # إزالة الحركات
    text = re.sub(r'[\u064B-\u065F]', '', text)
    # توحيد الألف
    text = text.replace('إ', 'ا').replace('أ', 'ا').replace('آ', 'ا').replace('ى', 'ا')
    # توحيد التاء المربوطة والهاء
    text = text.replace('ة', 'ه')
    return text

def normalize_series_name(name):
    """تطبيع اسم المسلسل/الفيلم للمقارنة."""
    if not name:
        return ''
    # إزالة الكلمات الدالة من البداية والنهاية
    name = re.sub(r'^(مسلسل|فيلم)\s+', '', name, flags=re.UNICODE)
    name = re.sub(r'\s+(الحلقة|الموسم|الجزء)$', '', name, flags=re.UNICODE)
    # إزالة الأرقام المنفردة في النهاية
    name = re.sub(r'\s+\d+$', '', name)
    # تطبيع عربي
    name = normalize_arabic(name)
    # تحويل إلى حروف صغيرة وإزالة المسافات الزائدة
    name = re.sub(r'\s+', ' ', name).strip().lower()
    return name

def clean_name_for_series(name):
    """تنظيف اسم المسلسل من الكلمات الدالة مع الاحتفاظ بالاسم للعرض."""
    name = re.sub(r'^مسلسل\s+', '', name, flags=re.UNICODE)
    name = re.sub(r'\s+(الحلقة|الموسم)$', '', name, flags=re.UNICODE)
    name = re.sub(r'\s+\d+$', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def clean_name_for_movie(name):
    """تنظيف اسم الفيلم."""
    name = re.sub(r'^فيلم\s+', '', name, flags=re.UNICODE)
    name = re.sub(r'\s+الجزء\s*\d*$', '', name, flags=re.UNICODE)
    name = re.sub(r'\s+\d+$', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

# ==============================
# 4. إنشاء الجداول وتحديثها (بشكل تدريجي)
# ==============================
try:
    with engine.begin() as conn:
        # 1. إنشاء جدول series إذا لم يكن موجوداً (بدون normalized_name)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS series (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                type VARCHAR(10) DEFAULT 'series',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        # 2. إنشاء جدول episodes
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
        # 3. إضافة عمود normalized_name إذا لم يكن موجوداً
        conn.execute(text("""
            ALTER TABLE series ADD COLUMN IF NOT EXISTS normalized_name VARCHAR(255)
        """))
        # 4. إنشاء الفهارس
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_series_normalized_name ON series(normalized_name)"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_series_name_type ON series(name, type)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_episodes_telegram_msg_id ON episodes(telegram_message_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_episodes_channel_id ON episodes(telegram_channel_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_episodes_added_at ON episodes(added_at)"))
    logger.info("✅ تم التحقق من هياكل الجداول وتحديثها.")
except Exception as e:
    logger.warning(f"⚠️ ملاحظة حول الجداول: {e}")

# 5. تحديث الأسماء المقيسة للصفوف القديمة (تتم مرة واحدة بعد إضافة العمود)
with engine.begin() as conn:
    try:
        rows = conn.execute(text("SELECT id, name FROM series WHERE normalized_name IS NULL")).fetchall()
        for row in rows:
            norm = normalize_series_name(row[1])
            conn.execute(text("UPDATE series SET normalized_name = :norm WHERE id = :id"), {"norm": norm, "id": row[0]})
        if rows:
            logger.info(f"✅ تم تحديث {len(rows)} اسماً مقيساً.")
    except Exception as e:
        logger.error(f"❌ فشل تحديث الأسماء المقيسة: {e}")

# ==============================
# 5. دوال التحليل والحفظ والحذف
# ==============================
def parse_content_info(message_text):
    """تحليل نص الرسالة لاستخراج المعلومات (محسّن)."""
    if not message_text:
        return None, None, None, None

    text = message_text.strip()
    original = text

    # كلمات مفتاحية للمسلسلات
    series_keywords = ['حلقة', 'الحلقة', 'موسم', 'الموسم', 'season', 'episode', ' s', ' e']
    # كلمات مفتاحية للأفلام
    movie_keywords = ['فيلم', 'الجزء', 'part']

    # تحديد نوع المحتوى
    is_series = any(kw in text.lower() for kw in series_keywords)
    is_movie = any(kw in text.lower() for kw in movie_keywords) and not is_series

    # ========== مسلسلات ==========
    if is_series:
        # 1. نمط: (اسم) الموسم (رقم) الحلقة (رقم)
        match = re.search(r'^(.*?)\s+الموسم\s+(\d+)\s+الحلقة\s+(\d+)$', text, re.UNICODE)
        if match:
            raw_name = match.group(1).strip()
            season = int(match.group(2))
            episode = int(match.group(3))
            name = clean_name_for_series(raw_name)
            return name, 'series', season, episode

        # 2. نمط: (اسم) S(رقم)E(رقم)
        match = re.search(r'^(.*?)\s+[Ss](\d+)[Ee](\d+)$', text)
        if match:
            raw_name = match.group(1).strip()
            season = int(match.group(2))
            episode = int(match.group(3))
            name = clean_name_for_series(raw_name)
            return name, 'series', season, episode

        # 3. نمط: (اسم) الحلقة (رقم) من الموسم (رقم)
        match = re.search(r'^(.*?)\s+الحلقة\s+(\d+)\s+من\s+الموسم\s+(\d+)$', text, re.UNICODE)
        if match:
            raw_name = match.group(1).strip()
            episode = int(match.group(2))
            season = int(match.group(3))
            name = clean_name_for_series(raw_name)
            return name, 'series', season, episode

        # 4. نمط: (اسم) الموسم (رقم) - (رقم)
        match = re.search(r'^(.*?)\s+الموسم\s+(\d+)[-\s]+(\d+)$', text, re.UNICODE)
        if match:
            raw_name = match.group(1).strip()
            season = int(match.group(2))
            episode = int(match.group(3))
            name = clean_name_for_series(raw_name)
            return name, 'series', season, episode

        # 5. نمط: (اسم) الحلقة (رقم) فقط (الموسم 1)
        match = re.search(r'^(.*?)\s+الحلقة\s+(\d+)$', text, re.UNICODE)
        if match:
            raw_name = match.group(1).strip()
            episode = int(match.group(2))
            name = clean_name_for_series(raw_name)
            return name, 'series', 1, episode

        # 6. استخراج أي رقمين من النص
        numbers = re.findall(r'\d+', text)
        if len(numbers) >= 2:
            name = re.sub(r'\d+', '', text).strip()
            name = clean_name_for_series(name)
            season = int(numbers[0])
            episode = int(numbers[1])
            return name, 'series', season, episode
        elif len(numbers) == 1:
            name = re.sub(r'\d+', '', text).strip()
            name = clean_name_for_series(name)
            return name, 'series', 1, int(numbers[0])

        # 7. نص بدون أرقام – نفترض موسم 1 حلقة 1 (قد يحدث نادراً)
        name = clean_name_for_series(text)
        return name, 'series', 1, 1

    # ========== أفلام ==========
    else:
        # 1. نمط: فيلم (الاسم) الجزء (رقم)
        match = re.search(r'فيلم\s+(.+?)\s+الجزء\s+(\d+)', text, re.UNICODE)
        if match:
            name = match.group(1).strip()
            part = int(match.group(2))
            return clean_name_for_movie(name), 'movie', part, 1

        # 2. نمط: فيلم (الاسم) (رقم)
        match = re.search(r'فيلم\s+(.+?)\s+(\d+)$', text, re.UNICODE)
        if match:
            name = match.group(1).strip()
            part = int(match.group(2))
            return clean_name_for_movie(name), 'movie', part, 1

        # 3. نمط: (الاسم) الجزء (رقم) بدون فيلم
        match = re.search(r'^(.*?)\s+الجزء\s+(\d+)$', text, re.UNICODE)
        if match:
            name = match.group(1).strip()
            part = int(match.group(2))
            return clean_name_for_movie(name), 'movie', part, 1

        # 4. نمط: (الاسم) ينتهي برقم
        match = re.search(r'^(.*?)\s+(\d+)$', text, re.UNICODE)
        if match:
            name = match.group(1).strip()
            part = int(match.group(2))
            return clean_name_for_movie(name), 'movie', part, 1

        # 5. أي نص آخر – فيلم جزء 1
        name = clean_name_for_movie(text)
        return name, 'movie', 1, 1

async def get_channel_entity(client, channel_input):
    """الحصول على كيان القناة مع معالجة أخطاء الانضمام."""
    try:
        channel = await client.get_entity(channel_input)
        return channel
    except Exception as e:
        logger.warning(f"⚠️ لم نتمكن من الوصول للقناة {channel_input}: {e}")
        if isinstance(channel_input, str) and channel_input.startswith('https://t.me/+'):
            try:
                invite_hash = channel_input.split('+')[-1]
                logger.info(f"🔄 محاولة الانضمام للقناة عبر رابط الدعوة: {invite_hash}")
                await client(ImportChatInviteRequest(invite_hash))
                logger.info(f"✅ تم الانضمام للقناة بنجاح")
                return await client.get_entity(channel_input)
            except Exception as join_error:
                logger.error(f"❌ فشل الانضمام: {join_error}")
                return None
        return None

def save_to_database(name, content_type, season_num, episode_num, telegram_msg_id, channel_id, series_id=None):
    """حفظ المحتوى في قاعدة البيانات مع استخدام normalized_name لدمج المتشابهات."""
    try:
        with engine.begin() as conn:
            if not series_id:
                # حساب الاسم المقيس
                normalized = normalize_series_name(name)
                # البحث باستخدام الاسم المقيس
                result = conn.execute(
                    text("SELECT id FROM series WHERE normalized_name = :norm AND type = :type"),
                    {"norm": normalized, "type": content_type}
                ).fetchone()

                if not result:
                    # إدخال جديد مع الاسم الأصلي والمقيس
                    conn.execute(
                        text("INSERT INTO series (name, normalized_name, type) VALUES (:name, :norm, :type)"),
                        {"name": name, "norm": normalized, "type": content_type}
                    )
                    # الحصول على id الجديد
                    result = conn.execute(
                        text("SELECT id FROM series WHERE normalized_name = :norm AND type = :type"),
                        {"norm": normalized, "type": content_type}
                    ).fetchone()

                series_id = result[0]

            # إدراج الحلقة (مع added_at التلقائي)
            conn.execute(
                text("""
                    INSERT INTO episodes (series_id, season, episode_number, 
                           telegram_message_id, telegram_channel_id)
                    VALUES (:sid, :season, :ep_num, :msg_id, :channel)
                    ON CONFLICT (telegram_message_id) DO NOTHING
                """),
                {
                    "sid": series_id,
                    "season": season_num,
                    "ep_num": episode_num,
                    "msg_id": telegram_msg_id,
                    "channel": channel_id
                }
            )

        # تسجيل الإضافة الجديدة (مهم)
        type_arabic = "مسلسل" if content_type == 'series' else "فيلم"
        if content_type == 'movie':
            logger.info(f"✅ فيلم جديد: {name} - الجزء {season_num} من {channel_id}")
        else:
            logger.info(f"✅ حلقة جديدة: {name} - الموسم {season_num} الحلقة {episode_num} من {channel_id}")
        return True

    except SQLAlchemyError as e:
        logger.error(f"❌ خطأ في قاعدة البيانات: {e}")
        return False

def delete_from_database(message_id):
    """حذف حلقة/جزء من قاعدة البيانات عند حذفها من القناة."""
    try:
        with engine.begin() as conn:
            episode_result = conn.execute(
                text("""
                    SELECT e.id, e.series_id, s.name, s.type, e.season, e.episode_number, e.telegram_channel_id
                    FROM episodes e
                    JOIN series s ON e.series_id = s.id
                    WHERE e.telegram_message_id = :msg_id
                """),
                {"msg_id": message_id}
            ).fetchone()

            if not episode_result:
                if DEBUG_MODE:
                    logger.debug(f"⚠️ لم يتم العثور على الحلقة {message_id} في قاعدة البيانات")
                return False

            episode_id, series_id, name, content_type, season, episode_num, channel_id = episode_result

            conn.execute(text("DELETE FROM episodes WHERE id = :episode_id"), {"episode_id": episode_id})

            remaining_episodes = conn.execute(
                text("SELECT COUNT(*) FROM episodes WHERE series_id = :series_id"),
                {"series_id": series_id}
            ).scalar()

            type_arabic = "مسلسل" if content_type == 'series' else "فيلم"

            if remaining_episodes == 0:
                conn.execute(text("DELETE FROM series WHERE id = :series_id"), {"series_id": series_id})
                logger.info(f"🗑️ تم حذف {type_arabic}: {name} بالكامل من {channel_id} (لا توجد حلقات/أجزاء متبقية)")
            else:
                if content_type == 'movie':
                    logger.info(f"🗑️ تم حذف {type_arabic}: {name} - الجزء {season} من {channel_id}")
                else:
                    logger.info(f"🗑️ تم حذف {type_arabic}: {name} - الموسم {season} الحلقة {episode_num} من {channel_id}")

            return True

    except SQLAlchemyError as e:
        logger.error(f"❌ خطأ في حذف من قاعدة البيانات: {e}")
        return False

async def sync_channel_messages(client, channel):
    """جلب آخر 1000 رسالة وإضافة الجديد منها (مع تحسين الأداء)."""
    channel_id = f"@{channel.username}" if hasattr(channel, 'username') and channel.username else str(channel.id)
    logger.info(f"\n🔄 بدء مزامنة القناة: {channel.title} (معرف: {channel_id})")

    # جلب آخر 1000 رسالة (بحد أقصى)
    messages = []
    async for msg in client.iter_messages(channel, limit=1000):
        if msg.text:
            messages.append(msg)

    logger.debug(f"📊 تم جلب {len(messages)} رسالة نصية من القناة.")

    # جلب معرفات الرسائل المخزنة مسبقاً
    with engine.connect() as conn:
        stored_ids = conn.execute(
            text("SELECT telegram_message_id FROM episodes WHERE telegram_channel_id = :channel_id"),
            {"channel_id": channel_id}
        ).fetchall()
    stored_ids_set = {row[0] for row in stored_ids}

    new_count = 0
    skipped_count = 0
    failed_parse_count = 0

    for msg in messages:
        if msg.id in stored_ids_set:
            skipped_count += 1
            continue

        name, content_type, season, episode = parse_content_info(msg.text)
        if name and content_type and episode is not None:
            if save_to_database(name, content_type, season, episode, msg.id, channel_id):
                new_count += 1
                stored_ids_set.add(msg.id)
            else:
                failed_parse_count += 1
        else:
            if DEBUG_MODE:
                logger.debug(f"⚠️ لم يتم تحليل الرسالة {msg.id}: {msg.text[:50]}...")
            failed_parse_count += 1

    logger.info(f"✅ مزامنة {channel.title} اكتملت: {new_count} جديدة، {skipped_count} موجودة، {failed_parse_count} فشل تحليل.")

async def import_channel_history(client, channel):
    """استيراد جميع الرسائل القديمة (بدون حد)."""
    logger.info(f"\n" + "="*50)
    logger.info(f"📂 بدء استيراد المحتوى القديم من القناة: {channel.title}")
    logger.info("="*50)

    imported_count = 0
    skipped_count = 0
    error_count = 0

    try:
        # جمع جميع الرسائل (قد يكون كبيراً)
        all_messages = []
        async for message in client.iter_messages(channel, limit=None):
            if message.text:
                all_messages.append(message)

        all_messages.reverse()  # ترتيب تصاعدي (الأقدم أولاً)

        logger.debug(f"📊 تم جمع {len(all_messages)} رسالة للاستيراد...")

        for message in all_messages:
            if not message.text:
                continue

            try:
                name, content_type, season_num, episode_num = parse_content_info(message.text)
                if name and content_type and episode_num is not None:
                    channel_id = f"@{message.chat.username}" if hasattr(message.chat, 'username') and message.chat.username else str(message.chat.id)
                    if save_to_database(name, content_type, season_num, episode_num, message.id, channel_id):
                        imported_count += 1
                    else:
                        skipped_count += 1
                else:
                    error_count += 1
            except Exception as e:
                logger.error(f"❌ خطأ في معالجة الرسالة {message.id}: {e}")
                error_count += 1

        logger.info("="*50)
        logger.info(f"✅ اكتمل استيراد القناة {channel.title}!")
        logger.info(f"   - تم استيراد: {imported_count} عنصر جديد")
        logger.info(f"   - تم تخطي: {skipped_count} عنصر (موجود مسبقاً)")
        logger.info(f"   - فشل تحليل: {error_count} رسالة")
        logger.info("="*50)

    except Exception as e:
        logger.error(f"❌ خطأ أثناء استيراد التاريخ من {channel.title}: {e}")

async def check_deleted_messages(client, channel):
    """التحقق من الرسائل المحذوفة."""
    channel_id = f"@{channel.username}" if hasattr(channel, 'username') and channel.username else str(channel.id)
    logger.info(f"\n🔍 التحقق من الرسائل المحذوفة في {channel.title}...")

    try:
        with engine.connect() as conn:
            stored_messages = conn.execute(
                text("SELECT telegram_message_id FROM episodes WHERE telegram_channel_id = :channel_id ORDER BY telegram_message_id"),
                {"channel_id": channel_id}
            ).fetchall()

            stored_ids = [msg[0] for msg in stored_messages]

            if not stored_ids:
                logger.info(f"   لا توجد رسائل مخزنة للقناة {channel.title}")
                return

            # جلب آخر 1000 رسالة للتحقق
            current_ids = []
            async for message in client.iter_messages(channel, limit=1000):
                current_ids.append(message.id)

            deleted_ids = [sid for sid in stored_ids if sid not in current_ids]

            if deleted_ids:
                logger.info(f"   تم العثور على {len(deleted_ids)} رسالة محذوفة في {channel.title}")
                for msg_id in deleted_ids:
                    if DEBUG_MODE:
                        logger.debug(f"   🗑️ معالجة الرسالة المحذوفة: {msg_id}")
                    delete_from_database(msg_id)
            else:
                logger.info(f"   ✅ لا توجد رسائل محذوفة في {channel.title}")

    except Exception as e:
        logger.error(f"❌ خطأ في التحقق من الرسائل المحذوفة في {channel.title}: {e}")

# ==============================
# 6. الدالة الرئيسية لمراقبة القنوات
# ==============================
async def monitor_channels():
    """مراقبة عدة قنوات."""
    logger.info("="*50)
    logger.info(f"🔍 بدء مراقبة {len(CHANNEL_LIST)} قناة:")
    for i, chan in enumerate(CHANNEL_LIST, 1):
        logger.info(f"   {i}. {chan}")
    logger.info("="*50)

    client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

    try:
        await client.start()
        logger.info("✅ تم الاتصال بـ Telegram بنجاح.")

        # الحصول على كيانات القنوات
        channel_entities = []
        for channel_input in CHANNEL_LIST:
            try:
                channel = await get_channel_entity(client, channel_input)
                if channel:
                    channel_entities.append(channel)
                    logger.info(f"✅ تمت إضافة القناة: {channel.title}")
                else:
                    logger.error(f"❌ فشل إضافة القناة: {channel_input}")
            except Exception as e:
                logger.error(f"❌ خطأ في إضافة القناة {channel_input}: {e}")

        if not channel_entities:
            logger.error("❌ لم يتم العثور على أي قناة صالحة!")
            return

        # مزامنة أولية (آخر 1000 رسالة)
        logger.info("\n🔄 بدء المزامنة الأولية...")
        for channel in channel_entities:
            await sync_channel_messages(client, channel)

        # استيراد المحتوى القديم إذا كان مفعلاً
        if IMPORT_HISTORY:
            for channel in channel_entities:
                await import_channel_history(client, channel)
        else:
            logger.info("⚠️ استيراد المحتوى القديم معطل. تمت المزامنة لآخر 1000 رسالة فقط.")

        # التحقق من المحذوفات
        if CHECK_DELETED_MESSAGES:
            for channel in channel_entities:
                await check_deleted_messages(client, channel)

        # مراقبة الرسائل الجديدة
        @client.on(events.NewMessage(chats=channel_entities))
        async def handler(event):
            message = event.message
            if message.text:
                channel_name = f"@{message.chat.username}" if hasattr(message.chat, 'username') and message.chat.username else message.chat.title
                logger.debug(f"📥 رسالة جديدة من {channel_name}: {message.text[:50]}...")

                name, content_type, season_num, episode_num = parse_content_info(message.text)
                if name and content_type and episode_num is not None:
                    channel_id = f"@{message.chat.username}" if hasattr(message.chat, 'username') and message.chat.username else str(message.chat.id)
                    save_to_database(name, content_type, season_num, episode_num, message.id, channel_id)
                else:
                    if DEBUG_MODE:
                        logger.debug(f"   ⚠️ لم يتم التعرف على المحتوى في الرسالة.")

        # مراقبة الحذف
        @client.on(events.MessageDeleted(chats=channel_entities))
        async def delete_handler(event):
            for msg_id in event.deleted_ids:
                logger.info(f"🗑️ تم حذف رسالة: {msg_id}")
                delete_from_database(msg_id)

        logger.info("\n🎯 جاهز لمراقبة القنوات:")
        for i, chan in enumerate(channel_entities, 1):
            logger.info(f"   {i}. {chan.title}")
        logger.info("   (اضغط Ctrl+C في Railway لإيقاف المراقبة)\n")

        await client.run_until_disconnected()

    except Exception as e:
        logger.error(f"❌ خطأ في تشغيل الـ Worker: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.disconnect()
        logger.info("🛑 تم إيقاف مراقبة القنوات.")

# ==============================
# 7. نقطة الدخول
# ==============================
if __name__ == "__main__":
    logger.info("🚀 بدء تشغيل Worker لمراقبة قنوات المسلسلات والأفلام...")
    logger.info(f"📡 عدد القنوات المحددة: {len(CHANNEL_LIST)}")
    asyncio.run(monitor_channels())
