import os
import asyncio
import re
import sys
import logging
import unicodedata
from datetime import datetime
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.functions.messages import ImportChatInviteRequest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ==============================
# 1. إعدادات التهيئة
# ==============================
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

# ==============================
# 2. الاتصال بقاعدة البيانات
# ==============================
try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("✅ تم الاتصال بقاعدة البيانات.")
except Exception as e:
    logger.error(f"❌ فشل الاتصال: {e}")
    sys.exit(1)

# ==============================
# 3. دوال التطبيع والتنظيف
# ==============================
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

# ==============================
# 4. إنشاء الجداول
# ==============================
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
logger.info("✅ تم إنشاء الجداول.")

# تحديث الأسماء المقيسة (إذا وجدت بيانات سابقة)
with engine.begin() as conn:
    rows = conn.execute(text("SELECT id, name FROM series WHERE normalized_name IS NULL")).fetchall()
    for row in rows:
        norm = normalize_series_name(row[1])
        conn.execute(text("UPDATE series SET normalized_name = :norm WHERE id = :id"), {"norm": norm, "id": row[0]})
    if rows:
        logger.info(f"✅ تم تحديث {len(rows)} اسماً مقيساً.")

# ==============================
# 5. دوال التحليل (الكاملة)
# ==============================
def parse_content_info(message_text):
    """تحليل نص الرسالة لاستخراج المعلومات (محسّن جداً)."""
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

        # 7. نص بدون أرقام – نفترض موسم 1 حلقة 1
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
    try:
        channel = await client.get_entity(channel_input)
        return channel
    except Exception as e:
        logger.warning(f"⚠️ لا يمكن الوصول للقناة {channel_input}: {e}")
        if isinstance(channel_input, str) and channel_input.startswith('https://t.me/+'):
            try:
                invite_hash = channel_input.split('+')[-1]
                await client(ImportChatInviteRequest(invite_hash))
                return await client.get_entity(channel_input)
            except:
                return None
        return None

def save_to_database(name, content_type, season_num, episode_num, telegram_msg_id, channel_id):
    try:
        with engine.begin() as conn:
            normalized = normalize_series_name(name)
            # البحث عن المسلسل
            result = conn.execute(
                text("SELECT id FROM series WHERE normalized_name = :norm AND type = :type"),
                {"norm": normalized, "type": content_type}
            ).fetchone()
            if not result:
                words = name.split()[:3]
                if words:
                    like = '%' + '%'.join(words) + '%'
                    result = conn.execute(
                        text("SELECT id FROM series WHERE name ILIKE :pat AND type = :type LIMIT 1"),
                        {"pat": like, "type": content_type}
                    ).fetchone()
            if not result:
                result = conn.execute(
                    text("INSERT INTO series (name, normalized_name, type) VALUES (:name, :norm, :type) RETURNING id"),
                    {"name": name, "norm": normalized, "type": content_type}
                ).fetchone()
            series_id = result[0]

            # إدراج الحلقة
            inserted = conn.execute(
                text("""
                    INSERT INTO episodes (series_id, season, episode_number, telegram_message_id, telegram_channel_id)
                    VALUES (:sid, :season, :ep, :msg, :chan)
                    ON CONFLICT (telegram_message_id) DO NOTHING
                    RETURNING id
                """),
                {"sid": series_id, "season": season_num, "ep": episode_num,
                 "msg": telegram_msg_id, "chan": channel_id}
            ).fetchone()
            if inserted:
                logger.info(f"✅ جديد: {name} - م{season_num} ح{episode_num} من {channel_id}")
                return True
            else:
                logger.debug(f"⚠️ موجودة: {telegram_msg_id}")
                return False
    except Exception as e:
        logger.error(f"❌ خطأ في الحفظ: {e}")
        return False

def delete_from_database(message_id):
    try:
        with engine.begin() as conn:
            # حذف الحلقة ومعرفة ما إذا كان المسلسل أصبح فارغاً
            ep = conn.execute(
                text("SELECT series_id FROM episodes WHERE telegram_message_id = :msg"),
                {"msg": message_id}
            ).fetchone()
            if not ep:
                return False
            series_id = ep[0]
            conn.execute(text("DELETE FROM episodes WHERE telegram_message_id = :msg"), {"msg": message_id})
            remaining = conn.execute(
                text("SELECT COUNT(*) FROM episodes WHERE series_id = :sid"),
                {"sid": series_id}
            ).scalar()
            if remaining == 0:
                conn.execute(text("DELETE FROM series WHERE id = :sid"), {"sid": series_id})
                logger.info(f"🗑️ تم حذف المسلسل بالكامل (آخر حلقة {message_id})")
            else:
                logger.info(f"🗑️ تم حذف حلقة {message_id}")
            return True
    except Exception as e:
        logger.error(f"❌ خطأ في الحذف: {e}")
        return False

async def sync_channel_messages(client, channel):
    channel_id = f"@{channel.username}" if channel.username else str(channel.id)
    logger.info(f"\n🔄 مزامنة {channel.title} ({channel_id})")
    messages = []
    async for msg in client.iter_messages(channel, limit=1000):
        if msg.text:
            messages.append(msg)
    logger.debug(f"📊 {len(messages)} رسالة نصية")

    with engine.connect() as conn:
        stored = conn.execute(
            text("SELECT telegram_message_id FROM episodes WHERE telegram_channel_id = :chan"),
            {"chan": channel_id}
        ).fetchall()
    stored_set = {r[0] for r in stored}

    new = 0
    skipped = 0
    failed = 0
    for msg in messages:
        if msg.id in stored_set:
            skipped += 1
            continue
        name, ctype, season, episode = parse_content_info(msg.text)
        if name and ctype and episode:
            if save_to_database(name, ctype, season, episode, msg.id, channel_id):
                new += 1
                stored_set.add(msg.id)
            else:
                # تحقق إذا كان موجوداً بالفعل
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
            failed += 1
    logger.info(f"✅ {channel.title}: {new} جديدة, {skipped} موجودة, {failed} فشل تحليل")

async def import_channel_history(client, channel):
    channel_id = f"@{channel.username}" if channel.username else str(channel.id)
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
            {"chan": channel_id}
        ).fetchall()
    stored_set = {r[0] for r in stored}

    new = 0
    skipped = 0
    failed = 0
    for msg in all_msgs:
        if msg.id in stored_set:
            skipped += 1
            continue
        name, ctype, season, episode = parse_content_info(msg.text)
        if name and ctype and episode:
            if save_to_database(name, ctype, season, episode, msg.id, channel_id):
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
            failed += 1
    logger.info(f"📥 {channel.title}: {new} جديدة, {skipped} موجودة, {failed} فشل")

async def check_deleted_messages(client, channel):
    channel_id = f"@{channel.username}" if channel.username else str(channel.id)
    logger.info(f"\n🔍 التحقق من المحذوفات في {channel.title}")
    try:
        with engine.connect() as conn:
            stored = conn.execute(
                text("SELECT telegram_message_id FROM episodes WHERE telegram_channel_id = :chan"),
                {"chan": channel_id}
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
        logger.error(f"❌ خطأ: {e}")

# ==============================
# 6. الدالة الرئيسية
# ==============================
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

    # مزامنة أولية
    for ch in channels:
        await sync_channel_messages(client, ch)

    # استيراد كامل إذا مطلوب
    if IMPORT_HISTORY:
        for ch in channels:
            await import_channel_history(client, ch)

    # التحقق من المحذوفات
    if CHECK_DELETED_MESSAGES:
        for ch in channels:
            await check_deleted_messages(client, ch)

    # مراقبة الأحداث
    @client.on(events.NewMessage(chats=channels))
    async def handler(event):
        msg = event.message
        if msg.text:
            name, ctype, season, episode = parse_content_info(msg.text)
            if name and ctype and episode:
                chan_id = f"@{msg.chat.username}" if msg.chat.username else str(msg.chat.id)
                save_to_database(name, ctype, season, episode, msg.id, chan_id)

    @client.on(events.MessageDeleted(chats=channels))
    async def delete_handler(event):
        for mid in event.deleted_ids:
            delete_from_database(mid)

    logger.info("🎯 في انتظار الأحداث...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(monitor_channels())
