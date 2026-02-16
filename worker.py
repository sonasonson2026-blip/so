import os
import asyncio
import re
import sys
import logging
from datetime import datetime
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import Message, Channel
from telethon.tl.functions.channels import GetFullChannelRequest
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
# 3. إنشاء الجداول إذا لم تكن موجودة
# ==============================
try:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS series (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                type VARCHAR(10) DEFAULT 'series',
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
        # إنشاء فهرس لتسريع البحث
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_series_name_type ON series(name, type)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_episodes_telegram_msg_id ON episodes(telegram_message_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_episodes_channel_id ON episodes(telegram_channel_id)"))
    logger.info("✅ تم التحقق من هياكل الجداول.")
except Exception as e:
    logger.warning(f"⚠️ ملاحظة حول الجداول: {e}")

# ==============================
# 4. دوال المساعدة (التحليل والحفظ والحذف)
# ==============================
def clean_name(name):
    """تنظيف الاسم من كلمات 'مسلسل' و'فيلم' والأرقام في النهاية مع الاحتفاظ بالرموز المهمة."""
    if not name:
        return name
    
    # إزالة كلمات "مسلسل" و"فيلم" من البداية
    name = re.sub(r'^(مسلسل\s+|فيلم\s+)', '', name, flags=re.IGNORECASE)
    
    # إزالة كلمات "مسلسل" و"فيلم" من أي مكان (إذا كانت منفصلة)
    name = re.sub(r'\s+(مسلسل|فيلم)\s+', ' ', name, flags=re.IGNORECASE)
    
    # إزالة الأرقام في النهاية إذا كانت موجودة (مثل " - 13") مع الاحتفاظ بالرموز داخل الاسم
    name = re.sub(r'\s*[-_]?\s*\d+\s*$', '', name).strip()
    
    # تنظيف المسافات الزائدة
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name

def extract_numbers_from_end(text):
    """استخراج الرقم من نهاية النص (مثل 13 من 'يوم-13' أو 'الحلقة 13')"""
    match = re.search(r'[-_]?\s*(\d+)\s*$', text)
    if match:
        return int(match.group(1))
    return None

def extract_season_episode(text):
    """استخراج الموسم والحلقة من النص إذا وجدا معاً."""
    patterns = [
        r'الموسم\s*(\d+)\s*الحلقة\s*(\d+)',
        r'[Ss]eason\s*(\d+)\s*[Ee]pisode\s*(\d+)',
        r'[Ss](\d+)[Ee](\d+)',
        r'(\d+)[-](\d+)',
        r'الحلقة\s*(\d+)\s*من\s*الموسم\s*(\d+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return int(match.group(1)), int(match.group(2))
    return None, None

def parse_content_info(message_text):
    """تحليل نص الرسالة لاستخراج المعلومات (محسّن جداً للمسلسلات والأفلام)."""
    if not message_text:
        return None, None, None, None
    
    text = message_text.strip()
    original = text
    
    # كلمات مفتاحية للمسلسل
    series_keywords = ['حلقة', 'الحلقة', 'موسم', 'الموسم', 'season', 'episode']
    is_series = any(keyword in text.lower() for keyword in series_keywords)
    
    # ========== 1. معالجة المسلسلات ==========
    if is_series:
        # محاولة استخراج الموسم والحلقة بأنماط مختلفة
        patterns = [
            # نمط: (اسم) الموسم (رقم) الحلقة (رقم) – مع السماح بوجود نقطتين أو شرطات في الاسم
            r'^(.*?)\s+الموسم\s+(\d+)\s+الحلقة\s+(\d+)$',
            r'^(.*?)\s+[Ss]eason\s+(\d+)\s+[Ee]pisode\s+(\d+)$',
            r'^(.*?)\s+[Ss](\d+)[Ee](\d+)$',
            r'^(.*?)\s+الموسم\s+(\d+)[-\s]+(\d+)$',  # الموسم 4-55
            r'^(.*?)\s+الحلقة\s+(\d+)$',  # بدون موسم (افتراضي موسم 1)
            r'^(.+?)\s+(\d+)$',  # اسم مع رقم في النهاية (قد يكون حلقة)
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    raw_name = groups[0].strip()
                    season = int(groups[1])
                    episode = int(groups[2])
                elif len(groups) == 2:
                    raw_name = groups[0].strip()
                    # إذا كان هناك كلمة موسم في النص ولكن النمط لم يلتقط سوى رقم واحد، نبحث عن رقم ثانٍ
                    if 'موسم' in text.lower() or 'season' in text.lower():
                        nums = re.findall(r'\d+', text)
                        if len(nums) >= 2:
                            season = int(nums[0])
                            episode = int(nums[1])
                            # إعادة بناء الاسم بعد إزالة الأرقام
                            name = re.sub(r'\d+', '', text).strip()
                            name = re.sub(r'^مسلسل\s+', '', name, flags=re.IGNORECASE).strip()
                            name = re.sub(r'\s+', ' ', name).strip()
                            if DEBUG_MODE:
                                logger.debug(f"تحليل (مسلسل من أرقام): {name} - الموسم {season} الحلقة {episode}")
                            return name, 'series', season, episode
                    season = 1
                    episode = int(groups[1])
                else:
                    continue
                
                # تنظيف الاسم من كلمة "مسلسل" في البداية مع الاحتفاظ بالنقطتين والشرطات
                name = re.sub(r'^مسلسل\s+', '', raw_name, flags=re.IGNORECASE).strip()
                name = re.sub(r'\s+', ' ', name).strip()
                
                # إذا أصبح الاسم فارغاً، استخدم النص الأصلي منزوع الأرقام
                if not name:
                    name = re.sub(r'\d+', '', text).strip()
                    name = re.sub(r'^مسلسل\s+', '', name, flags=re.IGNORECASE).strip()
                
                if DEBUG_MODE:
                    logger.debug(f"تحليل (مسلسل): {name} - الموسم {season} الحلقة {episode}")
                return name, 'series', season, episode
        
        # إذا لم تنجح الأنماط، نحاول استخراج أي رقمين من النص
        numbers = re.findall(r'\d+', text)
        if len(numbers) >= 2:
            season = int(numbers[0])
            episode = int(numbers[1])
            name = re.sub(r'\d+', '', text).strip()
            name = re.sub(r'^مسلسل\s+', '', name, flags=re.IGNORECASE).strip()
            name = re.sub(r'\s+', ' ', name).strip()
            if name:
                if DEBUG_MODE:
                    logger.debug(f"تحليل (مسلسل بالأرقام): {name} - الموسم {season} الحلقة {episode}")
                return name, 'series', season, episode
        elif len(numbers) == 1:
            episode = int(numbers[0])
            name = re.sub(r'\d+', '', text).strip()
            name = re.sub(r'^مسلسل\s+', '', name, flags=re.IGNORECASE).strip()
            name = re.sub(r'\s+', ' ', name).strip()
            if name:
                if DEBUG_MODE:
                    logger.debug(f"تحليل (مسلسل برقم واحد): {name} - الموسم 1 الحلقة {episode}")
                return name, 'series', 1, episode
        
        if DEBUG_MODE:
            logger.debug(f"لم يتم التعرف على مسلسل: {original}")
        return None, None, None, None
    
    # ========== 2. معالجة الأفلام ==========
    else:
        # أنماط الأفلام
        film_patterns = [
            r'فيلم\s+(.+?)\s+الجزء\s+(\d+)',
            r'فيلم\s+(.+?)[-_](\d+)',
            r'فيلم\s+(.+?)\s+(\d+)',
            r'فيلم\s+(.+)',  # فيلم بدون رقم
        ]
        for pattern in film_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                groups = match.groups()
                name = groups[0].strip()
                if len(groups) >= 2:
                    part = int(groups[1])
                else:
                    # استخراج رقم من الاسم إن وجد
                    nums = re.findall(r'\d+', name)
                    if nums:
                        part = int(nums[-1])
                        name = re.sub(r'\s*\d+\s*$', '', name).strip()
                    else:
                        part = 1
                # تنظيف بسيط مع الاحتفاظ بالرموز
                name = re.sub(r'^فيلم\s+', '', name, flags=re.IGNORECASE).strip()
                name = re.sub(r'\s+', ' ', name).strip()
                if DEBUG_MODE:
                    logger.debug(f"تحليل (فيلم): {name} - الجزء {part}")
                return name, 'movie', part, 1
        
        # إذا لم يطابق أي نمط، نعتبر النص كله اسماً لفيلم (مع محاولة استخراج رقم)
        nums = re.findall(r'\d+', text)
        if nums:
            part = int(nums[-1])
            name = re.sub(r'\s*\d+\s*$', '', text).strip()
        else:
            part = 1
            name = text
        name = re.sub(r'^فيلم\s+', '', name, flags=re.IGNORECASE).strip()
        name = re.sub(r'\s+', ' ', name).strip()
        if DEBUG_MODE:
            logger.debug(f"تحليل (فيلم افتراضي): {name} - الجزء {part}")
        return name, 'movie', part, 1

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
    """حفظ المحتوى في قاعدة البيانات مع معرف القناة."""
    try:
        with engine.begin() as conn:
            if not series_id:
                result = conn.execute(
                    text("SELECT id FROM series WHERE name = :name AND type = :type"),
                    {"name": name, "type": content_type}
                ).fetchone()
                
                if not result:
                    conn.execute(
                        text("INSERT INTO series (name, type) VALUES (:name, :type)"),
                        {"name": name, "type": content_type}
                    )
                    result = conn.execute(
                        text("SELECT id FROM series WHERE name = :name AND type = :type"),
                        {"name": name, "type": content_type}
                    ).fetchone()
                
                series_id = result[0]
            
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
            
        type_arabic = "مسلسل" if content_type == 'series' else "فيلم"
        if content_type == 'movie':
            logger.info(f"✅ تمت إضافة {type_arabic}: {name} - الجزء {season_num} من {channel_id}")
        else:
            logger.info(f"✅ تمت إضافة {type_arabic}: {name} - الموسم {season_num} الحلقة {episode_num} من {channel_id}")
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

async def check_deleted_messages(client, channel):
    """التحقق من الرسائل المحذوفة في القناة."""
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
# دالة مزامنة آخر 1000 رسالة
# ==============================
async def sync_channel_messages(client, channel):
    """جلب آخر 1000 رسالة وإضافة الجديد منها."""
    channel_id = f"@{channel.username}" if hasattr(channel, 'username') and channel.username else str(channel.id)
    logger.info(f"\n🔄 بدء مزامنة القناة: {channel.title} (معرف: {channel_id})")
    
    messages = []
    async for msg in client.iter_messages(channel, limit=1000):
        if msg.text:
            messages.append(msg)
    
    logger.info(f"📊 تم جلب {len(messages)} رسالة نصية من القناة.")
    
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
                if DEBUG_MODE:
                    logger.debug(f"⚠️ فشل حفظ الرسالة {msg.id}: {msg.text[:50]}...")
                failed_parse_count += 1
        else:
            if DEBUG_MODE:
                logger.debug(f"⚠️ لم يتم تحليل الرسالة {msg.id}: {msg.text[:50]}...")
            failed_parse_count += 1
    
    logger.info(f"✅ مزامنة القناة {channel.title} اكتملت: {new_count} رسالة جديدة، {skipped_count} موجودة مسبقاً، {failed_parse_count} فشل تحليل.")

# ==============================
# دالة استيراد كل الرسائل (بدون حد)
# ==============================
async def import_channel_history(client, channel):
    """استيراد جميع الرسائل القديمة من القناة بأقدمها أولاً."""
    logger.info(f"\n" + "="*50)
    logger.info(f"📂 بدء استيراد المحتوى القديم من القناة: {channel.title}")
    logger.info("="*50)
    
    imported_count = 0
    skipped_count = 0
    error_count = 0
    
    try:
        all_messages = []
        async for message in client.iter_messages(channel, limit=None):
            if message.text:
                all_messages.append(message)
        
        all_messages.reverse()
        
        logger.info(f"📊 تم جمع {len(all_messages)} رسالة للاستيراد...")
        
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
                    if DEBUG_MODE:
                        logger.debug(f"⚠️ لم يتم تحليل الرسالة: {message.text[:50]}...")
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

# ==============================
# 5. الدالة الرئيسية لمراقبة القنوات
# ==============================
async def monitor_channels():
    """الدالة الرئيسية لمراقبة عدة قنوات."""
    logger.info("="*50)
    logger.info(f"🔍 بدء مراقبة {len(CHANNEL_LIST)} قناة:")
    for i, chan in enumerate(CHANNEL_LIST, 1):
        logger.info(f"   {i}. {chan}")
    logger.info("="*50)
    
    client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
    
    try:
        await client.start()
        logger.info("✅ تم الاتصال بـ Telegram بنجاح.")
        
        # الحصول على كيانات جميع القنوات
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
        
        # خطوة المزامنة الأساسية (آخر 1000 رسالة)
        logger.info("\n🔄 بدء عملية المزامنة الشاملة مع القنوات...")
        for channel in channel_entities:
            await sync_channel_messages(client, channel)
        
        # استيراد المحتوى القديم إذا كان مفعلاً
        if IMPORT_HISTORY:
            for channel in channel_entities:
                await import_channel_history(client, channel)
        else:
            logger.info("⚠️ استيراد المحتوى القديم معطل. تمت المزامنة لآخر 1000 رسالة فقط.")
        
        # التحقق من الرسائل المحذوفة إذا كان مفعلاً
        if CHECK_DELETED_MESSAGES:
            for channel in channel_entities:
                await check_deleted_messages(client, channel)
        
        # مراقبة الرسائل الجديدة
        @client.on(events.NewMessage(chats=channel_entities))
        async def handler(event):
            message = event.message
            if message.text:
                channel_name = f"@{message.chat.username}" if hasattr(message.chat, 'username') and message.chat.username else message.chat.title
                logger.info(f"📥 رسالة جديدة من {channel_name}: {message.text[:50]}...")
                
                name, content_type, season_num, episode_num = parse_content_info(message.text)
                if name and content_type and episode_num is not None:
                    type_arabic = "مسلسل" if content_type == 'series' else "فيلم"
                    if content_type == 'movie':
                        logger.info(f"   تم التعرف على {type_arabic}: {name} - الجزء {season_num}")
                    else:
                        logger.info(f"   تم التعرف على {type_arabic}: {name} - الموسم {season_num} الحلقة {episode_num}")
                    
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
# 6. نقطة دخول البرنامج
# ==============================
if __name__ == "__main__":
    logger.info("🚀 بدء تشغيل Worker لمراقبة قنوات المسلسلات والأفلام...")
    logger.info(f"📡 عدد القنوات المحددة: {len(CHANNEL_LIST)}")
    asyncio.run(monitor_channels())
