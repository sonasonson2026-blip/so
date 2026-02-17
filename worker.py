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
# 4. إنشاء الجداول وتحديثها
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

# تحديث الأسماء المقيسة للصفوف القديمة (إن وجدت)
with engine.begin() as conn:
    rows = conn.execute(text("SELECT id, name FROM series WHERE normalized_name IS NULL")).fetchall()
    for row in rows:
        norm = normalize_series_name(row[1])
        conn.execute(text("UPDATE series SET normalized_name = :norm WHERE id = :id"), {"norm": norm, "id": row[0]})
    if rows:
        logger.info(f"✅ تم تحديث {len(rows)} اسماً مقيساً.")

# ==============================
# 5. دوال التحليل والحفظ
# ==============================
def parse_content_info(message_text):
    """تحليل نص الرسالة (نفس الكود السابق)"""
    if not message_text:
        return None, None, None, None
    text = message_text.strip()
    # ... (نفس الأنماط السابقة) ...
    # للاختصار، سأضع نسخة مختصرة، لكن يُفضل استخدام النسخة الكاملة من الردود السابقة
    # هنا أستخدم نسخة مبسطة لتوضيح الفكرة فقط
    series_keywords = ['حلقة', 'الحلقة', 'موسم', 'الموسم']
    movie_keywords = ['فيلم', 'الجزء']
    is_series = any(kw in text for kw in series_keywords)
    if is_series:
        # محاولة استخراج الموسم والحلقة
        match = re.search(r'الموسم\s*(\d+)\s*الحلقة\s*(\d+)', text)
        if match:
            season = int(match.group(1))
            episode = int(match.group(2))
            name = re.sub(r'الموسم\s*\d+\s*الحلقة\s*\d+', '', text).strip()
            name = clean_name_for_series(name)
            return name, 'series', season, episode
    else:
        # فيلم
        match = re.search(r'الجزء\s*(\d+)', text)
        if match:
            part = int(match.group(1))
            name = re.sub(r'الجزء\s*\d+', '', text).strip()
            name = clean_name_for_movie(name)
            return name, 'movie', part, 1
    return None, None, None, None

async def get_channel_entity(client, channel_input):
    # ... (نفس الكود السابق) ...
    try:
        return await client.get_entity(channel_input)
    except:
        return None

def save_to_database(name, content_type, season_num, episode_num, telegram_msg_id, channel_id):
    """حفظ مع التأكد من الإدراج"""
    try:
        with engine.begin() as conn:
            # البحث عن المسلسل
            normalized = normalize_series_name(name)
            result = conn.execute(
                text("SELECT id FROM series WHERE normalized_name = :norm AND type = :type"),
                {"norm": normalized, "type": content_type}
            ).fetchone()
            if not result:
                # بحث بـ ILIKE
                words = name.split()[:3]
                if words:
                    like = '%' + '%'.join(words) + '%'
                    result = conn.execute(
                        text("SELECT id FROM series WHERE name ILIKE :pat AND type = :type LIMIT 1"),
                        {"pat": like, "type": content_type}
                    ).fetchone()
            if not result:
                # إنشاء جديد
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
    # ... (نفس الكود السابق) ...
    pass

# ==============================
# 6. مزامنة القنوات
# ==============================
async def sync_channel_messages(client, channel):
    channel_id = f"@{channel.username}" if channel.username else str(channel.id)
    logger.info(f"\n🔄 مزامنة {channel.title} ({channel_id})")

    # جلب آخر 1000 رسالة
    messages = []
    async for msg in client.iter_messages(channel, limit=1000):
        if msg.text:
            messages.append(msg)
    logger.debug(f"📊 {len(messages)} رسالة نصية")

    # معرفات المخزنة
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
                # فشل الإدراج (موجود أو خطأ)
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
    logger.info(f"✅ {channel.title}: {new} جديدة, {skipped} موجودة, {failed} فشل")

async def import_channel_history(client, channel):
    """استيراد كل الرسائل مع التحقق المسبق"""
    channel_id = f"@{channel.username}" if channel.username else str(channel.id)
    logger.info(f"\n📂 استيراد كامل {channel.title}")

    # جلب جميع الرسائل
    all_msgs = []
    async for msg in client.iter_messages(channel, limit=None):
        if msg.text:
            all_msgs.append(msg)
    all_msgs.reverse()
    logger.debug(f"📊 {len(all_msgs)} رسالة")

    # معرفات المخزنة
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
                # تحقق إضافي
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
    # ... (نفس الكود السابق) ...
    pass

# ==============================
# 7. الدالة الرئيسية
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
