import os
import logging
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes
)
from sqlalchemy import create_engine, text
from telegram.request import HTTPXRequest

# ==============================
# 1. الإعدادات والتكوين
# ==============================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

if not BOT_TOKEN:
    print("❌ خطأ: BOT_TOKEN غير موجود في متغيرات البيئة!")
    exit(1)

if not DATABASE_URL:
    print("⚠️ تحذير: DATABASE_URL غير موجود. قد لا تعرض المحتويات.")

# إصلاح رابط قاعدة البيانات
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# إعداد التسجيل
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# محرك قاعدة البيانات
engine = None
if DATABASE_URL:
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
        # اختبار الاتصال
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ تم الاتصال بقاعدة البيانات بنجاح.")
        
        # اختبار جلب البيانات مباشرة
        with engine.connect() as conn:
            series_count = conn.execute(text("SELECT COUNT(*) FROM series WHERE type = 'series'")).scalar()
            movies_count = conn.execute(text("SELECT COUNT(*) FROM series WHERE type = 'movie'")).scalar()
            print(f"📊 في الاختبار المبدئي:")
            print(f"   - عدد المسلسلات: {series_count}")
            print(f"   - عدد الأفلام: {movies_count}")
            
    except Exception as e:
        print(f"❌ فشل الاتصال بقاعدة البيانات: {e}")
        engine = None

# ==============================
# 2. دوال المساعدة للتعامل مع قاعدة البيانات
# ==============================
async def get_all_content(content_type=None):
    """جلب جميع المحتويات من قاعدة البيانات حسب النوع (مسلسلات/أفلام)"""
    if not engine:
        print("⚠️ محرك قاعدة البيانات غير متاح في get_all_content")
        return []
    
    try:
        with engine.connect() as conn:
            query = """
                SELECT s.id, s.name, s.type, 
                       COUNT(e.id) as episode_count,
                       COUNT(DISTINCT e.telegram_channel_id) as channel_count
                FROM series s
                LEFT JOIN episodes e ON s.id = e.series_id
            """
            
            if content_type:
                query += f" WHERE s.type = '{content_type}'"
            
            query += """
                GROUP BY s.id, s.name, s.type
                ORDER BY s.id ASC
            """
            
            print(f"🔍 تنفيذ الاستعلام: {query[:100]}...")
            result = conn.execute(text(query))
            rows = result.fetchall()
            
            print(f"📊 تم جلب {len(rows)} صفاً من قاعدة البيانات:")
            for row in rows:
                print(f"   - {row[1]} ({row[2]}) - {row[3]} حلقة/جزء - {row[4]} قناة")
            
            return rows
            
    except Exception as e:
        print(f"❌ خطأ في جلب المحتويات: {e}")
        import traceback
        traceback.print_exc()
        return []

async def get_content_episodes(series_id, page=1, per_page=50):
    """جلب حلقات/أجزاء محتوى محدد مع دعم التقسيم إلى صفحات"""
    if not engine:
        print("⚠️ محرك قاعدة البيانات غير متاح في get_content_episodes")
        return [], 0, 0
    
    try:
        with engine.connect() as conn:
            # حساب العدد الإجمالي للحلقات
            count_result = conn.execute(text("""
                SELECT COUNT(*) FROM episodes WHERE series_id = :series_id
            """), {"series_id": series_id})
            total_episodes = count_result.scalar()
            
            # حساب عدد الصفحات
            total_pages = (total_episodes + per_page - 1) // per_page
            
            # ضبط رقم الصفحة إذا كان خارج النطاق
            if page < 1:
                page = 1
            elif page > total_pages and total_pages > 0:
                page = total_pages
            
            # حساب offset للصفحة
            offset = (page - 1) * per_page
            
            # جلب الحلقات للصفحة الحالية
            result = conn.execute(text("""
                SELECT e.id, e.season, e.episode_number, 
                       e.telegram_message_id, e.telegram_channel_id
                FROM episodes e
                WHERE e.series_id = :series_id
                ORDER BY e.season, e.episode_number
                LIMIT :limit OFFSET :offset
            """), {
                "series_id": series_id,
                "limit": per_page,
                "offset": offset
            })
            
            rows = result.fetchall()
            return rows, total_episodes, total_pages
            
    except Exception as e:
        print(f"❌ خطأ في جلب حلقات المحتوى {series_id}: {e}")
        return [], 0, 0

async def get_content_info(series_id):
    """جلب معلومات محتوى محدد"""
    if not engine:
        print("⚠️ محرك قاعدة البيانات غير متاح في get_content_info")
        return None
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT id, name, type FROM series WHERE id = :series_id
            """), {"series_id": series_id})
            row = result.fetchone()
            if row:
                print(f"🔍 معلومات المحتوى {series_id}: {row[1]} ({row[2]})")
            return row
    except Exception as e:
        print(f"❌ خطأ في جلب معلومات المحتوى {series_id}: {e}")
        return None

# ==============================
# 3. دوال البوت الرئيسية مع معالجة الأخطاء
# ==============================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """أمر /start"""
    try:
        keyboard = [
            [InlineKeyboardButton("📺 المسلسلات", callback_data='series_list'),
             InlineKeyboardButton("🎬 الأفلام", callback_data='movies_list')],
            [InlineKeyboardButton("📁 جميع المحتويات", callback_data='all_content')],
            [InlineKeyboardButton("🔄 اختبار قاعدة البيانات", callback_data='test_db')],
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        welcome_text = """
🎬 *مرحباً في بوت مسلسلاتي وأفلامي* 🎬

*مميزات البوت:*
• تصفح جميع المسلسلات في القناة
• تصفح جميع الأفلام في القناة
• الوصول السريع للحلقات والأجزاء

📌 *الأوامر المتاحة:*
/start - عرض هذه الرسالة
/series - عرض المسلسلات
/movies - عرض الأفلام
/all - عرض كل المحتويات
/test - اختبار قاعدة البيانات
/debug - فحص حالة النظام
        """
        
        if update.callback_query:
            await update.callback_query.edit_message_text(
                welcome_text,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                welcome_text,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
    except Exception as e:
        logger.error(f"خطأ في أمر start: {e}")
        if update.callback_query:
            await update.callback_query.edit_message_text("⚠️ حدث خطأ أثناء معالجة الطلب. يرجى المحاولة مرة أخرى.")
        else:
            await update.message.reply_text("⚠️ حدث خطأ أثناء معالجة الطلب. يرجى المحاولة مرة أخرى.")

async def show_content(update: Update, context: ContextTypes.DEFAULT_TYPE, content_type=None):
    """عرض المحتويات حسب النوع"""
    try:
        if not engine:
            error_msg = "❌ قاعدة البيانات غير متاحة حالياً."
            if update.callback_query:
                await update.callback_query.edit_message_text(error_msg)
            else:
                await update.message.reply_text(error_msg)
            return
        
        content_list = await get_all_content(content_type)
        
        if content_type == 'series':
            title = "📺 *قائمة المسلسلات*"
            empty_msg = "📭 لا توجد مسلسلات حالياً."
        elif content_type == 'movie':
            title = "🎬 *قائمة الأفلام*"
            empty_msg = "📭 لا توجد أفلام حالياً."
        else:
            title = "📁 *جميع المحتويات*"
            empty_msg = "📭 لا توجد محتويات حالياً."
        
        if not content_list:
            no_data_msg = f"{empty_msg}\n\nℹ️ *ملاحظة:* يمكنك استخدام زر 'اختبار قاعدة البيانات' للتحقق."
            if update.callback_query:
                await update.callback_query.edit_message_text(no_data_msg)
            else:
                await update.message.reply_text(no_data_msg)
            return
        
        # بناء النص
        text = f"{title}\n\n"
        keyboard = []
        
        for content in content_list:
            content_id, name, content_type, episode_count, channel_count = content
            
            if content_type == 'series':
                count_text = f"{episode_count} حلقة في {channel_count} قناة" if episode_count > 0 else "بدون حلقات"
            else:
                count_text = f"{episode_count} جزء في {channel_count} قناة" if episode_count > 0 else "بدون أجزاء"
            
            text += f"• {name} ({count_text})\n"
            keyboard.append([
                InlineKeyboardButton(
                    f"{name[:20]} ({episode_count})",
                    callback_data=f"content_{content_id}"
                )
            ])
        
        # أزرار التنقل
        keyboard.append([
            InlineKeyboardButton("📺 المسلسلات", callback_data="series_list"),
            InlineKeyboardButton("🎬 الأفلام", callback_data="movies_list")
        ])
        keyboard.append([InlineKeyboardButton("🏠 الرئيسية", callback_data="home")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # الإرسال حسب مصدر الطلب
        if update.callback_query:
            await update.callback_query.edit_message_text(
                text,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                text,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
            
    except Exception as e:
        logger.error(f"خطأ في show_content: {e}")
        error_msg = "⚠️ حدث خطأ أثناء جلب البيانات. يرجى المحاولة مرة أخرى."
        if update.callback_query:
            await update.callback_query.edit_message_text(error_msg)
        else:
            await update.message.reply_text(error_msg)

async def series_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """أمر /series - عرض المسلسلات"""
    await show_content(update, context, 'series')

async def movies_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """أمر /movies - عرض الأفلام"""
    await show_content(update, context, 'movie')

async def all_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """أمر /all - عرض كل المحتويات"""
    await show_content(update, context)

async def test_db_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """أمر /test - اختبار قاعدة البيانات"""
    try:
        if not engine:
            await update.message.reply_text("❌ قاعدة البيانات غير متصلة.")
            return
        
        with engine.connect() as conn:
            # جلب جميع الجداول
            tables_result = conn.execute(text("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)).fetchall()
            
            tables_info = "📋 *الجداول الموجودة:*\n"
            for table in tables_result:
                table_name = table[0]
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()
                count = count_result[0] if count_result else 0
                tables_info += f"• `{table_name}`: {count} صف\n"
            
            # جلب عينات من البيانات
            series_sample = conn.execute(text("""
                SELECT id, name, type FROM series ORDER BY id LIMIT 5
            """)).fetchall()
            
            episodes_sample = conn.execute(text("""
                SELECT id, series_id, season, episode_number, telegram_channel_id FROM episodes ORDER BY id LIMIT 5
            """)).fetchall()
            
            series_text = "🎬 *عينة من المسلسلات والأفلام:*\n"
            for row in series_sample:
                series_text += f"• ID:{row[0]} - {row[1]} ({row[2]})\n"
            
            episodes_text = "📺 *عينة من الحلقات:*\n"
            for row in episodes_sample:
                episodes_text += f"• ID:{row[0]} - مسلسل:{row[1]} - م{row[2]} ح{row[3]} - قناة:{row[4]}\n"
            
            reply_text = f"{tables_info}\n{series_text}\n{episodes_text}"
            
        await update.message.reply_text(reply_text, parse_mode='Markdown')
        
    except Exception as e:
        await update.message.reply_text(f"❌ خطأ في اختبار قاعدة البيانات:\n`{str(e)[:300]}`")

# ==============================
# 4. معالجة تفاصيل المحتوى مع التقسيم إلى صفحات
# ==============================
async def show_content_details(update: Update, context: ContextTypes.DEFAULT_TYPE, content_id, page=1):
    """عرض تفاصيل محتوى محدد (مسلسل أو فيلم) مع دعم الصفحات"""
    query = update.callback_query
    
    try:
        # جلب معلومات المحتوى
        content_info = await get_content_info(content_id)
        if not content_info:
            await query.edit_message_text("❌ المحتوى غير موجود.")
            return
        
        content_id, name, content_type = content_info
        
        # جلب القنوات التي يوجد فيها هذا المحتوى
        channels = []
        if engine:
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT DISTINCT telegram_channel_id 
                    FROM episodes 
                    WHERE series_id = :series_id
                """), {"series_id": content_id}).fetchall()
                channels = [row[0] for row in result]
        
        episodes, total_episodes, total_pages = await get_content_episodes(content_id, page)
        
        if not episodes:
            item_type = "حلقات" if content_type == 'series' else "أجزاء"
            message_text = f"*{name}*\n\n📭 لا توجد {item_type} حالياً."
            
            if channels:
                message_text += f"\n\n*القنوات:* {', '.join(channels)}"
            
            keyboard = [[InlineKeyboardButton("⬅️ رجوع", callback_data=f"{content_type}_list")]]
            await query.edit_message_text(
                message_text, 
                parse_mode='Markdown', 
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        # تجميع الحلقات حسب الموسم (للمسلسلات) أو الجزء (للأفلام)
        seasons = {}
        for ep in episodes:
            ep_id, season, ep_num, msg_id, channel_id = ep
            if season not in seasons:
                seasons[season] = []
            seasons[season].append((ep_id, ep_num, msg_id, channel_id))
        
        # بناء الرسالة
        item_type = "حلقات" if content_type == 'series' else "أجزاء"
        message_text = f"*{name}*\n\n"
        
        if total_episodes > 0:
            message_text += f"عدد {item_type}: {total_episodes}\n"
            if total_pages > 1:
                message_text += f"الصفحة {page} من {total_pages}\n"
        
        # إظهار القنوات
        if channels:
            message_text += f"\n*القنوات:* {', '.join(channels)}\n\n"
        
        keyboard = []
        
        # ============================================
        # معالجة المسلسلات
        # ============================================
        if content_type == 'series':
            # إذا كان المسلسل له أكثر من موسم، نعرض قائمة المواسم
            if len(seasons) > 1:
                message_text += "اختر الموسم:"
                for season_num in sorted(seasons.keys()):
                    # حساب عدد الحلقات في هذا الموسم
                    ep_count = len(seasons[season_num])
                    keyboard.append([
                        InlineKeyboardButton(
                            f"الموسم {season_num} ({ep_count} حلقة)",
                            callback_data=f"season_{content_id}_{season_num}"
                        )
                    ])
            else:
                # إذا كان المسلسل له موسم واحد فقط، نعرض الحلقات مباشرة
                season_num = list(seasons.keys())[0] if seasons else 1
                season_episodes = seasons.get(season_num, [])
                
                message_text += f"الموسم {season_num}\nاختر الحلقة:"
                
                # تقسيم أزرار الحلقات (5 أزرار في كل صف)
                row_buttons = []
                for ep_id, ep_num, msg_id, channel_id in season_episodes:
                    row_buttons.append(
                        InlineKeyboardButton(
                            f"الحلقة {ep_num}",
                            callback_data=f"ep_{ep_id}"
                        )
                    )
                    
                    # كل 5 أزرار نبدأ صف جديد
                    if len(row_buttons) == 5:
                        keyboard.append(row_buttons)
                        row_buttons = []
                
                if row_buttons:
                    keyboard.append(row_buttons)
        
        # ============================================
        # معالجة الأفلام
        # ============================================
        else:  # content_type == 'movie'
            # إذا كان الفيلم له أكثر من جزء
            if len(seasons) > 1:
                message_text += "اختر الجزء:"
                for season_num in sorted(seasons.keys()):
                    # لكل جزء (موسم) نأخذ الحلقة الأولى (والوحيدة)
                    ep_id, ep_num, msg_id, channel_id = seasons[season_num][0]
                    keyboard.append([
                        InlineKeyboardButton(
                            f"الجزء {season_num}",
                            callback_data=f"ep_{ep_id}"
                        )
                    ])
            else:
                # إذا كان الفيلم له جزء واحد فقط
                season_num = list(seasons.keys())[0] if seasons else 1
                season_episodes = seasons.get(season_num, [])
                
                if season_episodes:
                    ep_id, ep_num, msg_id, channel_id = season_episodes[0]
                    message_text += "اضغط على الزر أدناه لمشاهدة الفيلم:"
                    keyboard = [[
                        InlineKeyboardButton(
                            "مشاهدة الفيلم",
                            callback_data=f"ep_{ep_id}"
                        )
                    ]]
        
        # أزرار التنقل بين الصفحات إذا كان هناك أكثر من صفحة
        if total_pages > 1:
            nav_buttons = []
            
            if page > 1:
                nav_buttons.append(
                    InlineKeyboardButton("⬅️ السابقة", callback_data=f"content_page_{content_id}_{page-1}")
                )
            
            nav_buttons.append(
                InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="page_info")
            )
            
            if page < total_pages:
                nav_buttons.append(
                    InlineKeyboardButton("التالية ➡️", callback_data=f"content_page_{content_id}_{page+1}")
                )
            
            keyboard.append(nav_buttons)
        
        # أزرار التنقل الرئيسية
        keyboard.append([
            InlineKeyboardButton("⬅️ رجوع", callback_data=f"{content_type}_list"),
            InlineKeyboardButton("🏠 الرئيسية", callback_data="home")
        ])
        
        await query.edit_message_text(
            message_text,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"خطأ في show_content_details: {e}")
        await query.edit_message_text("⚠️ حدث خطأ أثناء جلب البيانات. يرجى المحاولة مرة أخرى.")

# ==============================
# 5. معالج الأزرار التفاعلية مع إعادة المحاولة
# ==============================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة أزرار InlineKeyboard مع إعادة المحاولة"""
    query = update.callback_query
    
    # محاولة الإجابة على الاستعلام مع إعادة المحاولة
    max_retries = 3
    for attempt in range(max_retries):
        try:
            await query.answer()  # مهم لإعلام تليجرام
            break
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"محاولة {attempt + 1} فشلت، إعادة المحاولة... خطأ: {e}")
                await asyncio.sleep(1)  # انتظار ثانية قبل إعادة المحاولة
            else:
                logger.error(f"فشل جميع محاولات الإجابة على الاستعلام: {e}")
                return
    
    data = query.data
    
    try:
        if data == 'home':
            await start(update, context)
            return
        
        elif data == 'test_db':
            # دالة اختبار قاعدة البيانات
            await test_db_button(update, context)
            return
        
        elif data == 'all_content':
            await show_content(update, context)
            return
        
        elif data == 'series_list':
            await show_content(update, context, 'series')
            return
        
        elif data == 'movies_list':
            await show_content(update, context, 'movie')
            return
        
        elif data == 'page_info':
            # لا تفعل شيئاً لمعلومات الصفحة
            return
        
        elif data.startswith('content_page_'):
            # بيانات الزر: content_page_<content_id>_<page_number>
            parts = data.split('_')
            content_id = int(parts[2])
            page = int(parts[3])
            await show_content_details(update, context, content_id, page)
            return
        
        elif data.startswith('content_'):
            content_id = int(data.split('_')[1])
            await show_content_details(update, context, content_id, 1)
            return
        
        elif data.startswith('ep_'):
            episode_id = int(data.split('_')[1])
            await show_episode_details(update, context, episode_id)
            return
        
        elif data.startswith('season_'):
            # بيانات الزر: season_<content_id>_<season_number>
            parts = data.split('_')
            content_id = int(parts[1])
            season_num = int(parts[2])
            await show_season_episodes(update, context, content_id, season_num, 1)
            return
        
        elif data.startswith('season_page_'):
            # بيانات الزر: season_page_<content_id>_<season_number>_<page_number>
            parts = data.split('_')
            content_id = int(parts[2])
            season_num = int(parts[3])
            page = int(parts[4])
            await show_season_episodes(update, context, content_id, season_num, page)
            return
            
    except Exception as e:
        logger.error(f"خطأ في button_handler: {e}")
        await query.edit_message_text("⚠️ حدث خطأ أثناء معالجة طلبك. يرجى المحاولة مرة أخرى.")

# ==============================
# 6. دوال إضافية مطلوبة
# ==============================
async def show_season_episodes(update: Update, context: ContextTypes.DEFAULT_TYPE, content_id, season_num, page=1):
    """عرض حلقات موسم محدد لمسلسل مع دعم الصفحات"""
    query = update.callback_query
    
    try:
        # جلب معلومات المحتوى
        content_info = await get_content_info(content_id)
        if not content_info:
            await query.edit_message_text("❌ المحتوى غير موجود.")
            return
        
        content_id, name, content_type = content_info
        
        # هذه الدالة للمسلسلات فقط
        if content_type != 'series':
            await query.edit_message_text("❌ هذه الدالة للمسلسلات فقط.")
            return
        
        with engine.connect() as conn:
            # حساب العدد الإجمالي للحلقات للموسم
            count_result = conn.execute(text("""
                SELECT COUNT(*) FROM episodes 
                WHERE series_id = :series_id AND season = :season
            """), {"series_id": content_id, "season": season_num})
            total_episodes = count_result.scalar()
            
            # حساب عدد الصفحات
            per_page = 50
            total_pages = (total_episodes + per_page - 1) // per_page
            
            # ضبط رقم الصفحة
            if page < 1:
                page = 1
            elif page > total_pages and total_pages > 0:
                page = total_pages
            
            # حساب offset
            offset = (page - 1) * per_page
            
            # جلب الحلقات للصفحة الحالية
            result = conn.execute(text("""
                SELECT e.id, e.season, e.episode_number, 
                       e.telegram_message_id, e.telegram_channel_id
                FROM episodes e
                WHERE e.series_id = :series_id AND e.season = :season
                ORDER BY e.episode_number
                LIMIT :limit OFFSET :offset
            """), {
                "series_id": content_id,
                "season": season_num,
                "limit": per_page,
                "offset": offset
            })
            
            episodes = result.fetchall()
        
        if not episodes:
            await query.edit_message_text(f"❌ لا توجد حلقات للموسم {season_num}.")
            return
        
        message_text = f"*{name}*\nالموسم {season_num}\n\n"
        
        if total_episodes > 0:
            message_text += f"عدد الحلقات: {total_episodes}\n"
            if total_pages > 1:
                message_text += f"الصفحة {page} من {total_pages}\n\n"
        
        message_text += "اختر الحلقة:"
        
        keyboard = []
        
        # تقسيم أزرار الحلقات (5 أزرار في كل صف)
        row_buttons = []
        for ep in episodes:
            ep_id, season, ep_num, msg_id, channel_id = ep
            row_buttons.append(
                InlineKeyboardButton(
                    f"الحلقة {ep_num}",
                    callback_data=f"ep_{ep_id}"
                )
            )
            
            if len(row_buttons) == 5:
                keyboard.append(row_buttons)
                row_buttons = []
        
        if row_buttons:
            keyboard.append(row_buttons)
        
        # أزرار التنقل بين الصفحات إذا كان هناك أكثر من صفحة
        if total_pages > 1:
            nav_buttons = []
            
            if page > 1:
                nav_buttons.append(
                    InlineKeyboardButton("⬅️ السابقة", callback_data=f"season_page_{content_id}_{season_num}_{page-1}")
                )
            
            nav_buttons.append(
                InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="page_info")
            )
            
            if page < total_pages:
                nav_buttons.append(
                    InlineKeyboardButton("التالية ➡️", callback_data=f"season_page_{content_id}_{season_num}_{page+1}")
                )
            
            keyboard.append(nav_buttons)
        
        # أزرار التنقل
        keyboard.append([
            InlineKeyboardButton("⬅️ رجوع للمسلسل", callback_data=f"content_{content_id}"),
            InlineKeyboardButton("🏠 الرئيسية", callback_data="home")
        ])
        
        await query.edit_message_text(
            message_text,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"خطأ في show_season_episodes: {e}")
        await query.edit_message_text("⚠️ حدث خطأ أثناء جلب البيانات. يرجى المحاولة مرة أخرى.")

async def show_episode_details(update: Update, context: ContextTypes.DEFAULT_TYPE, episode_id):
    """عرض تفاصيل حلقة/جزء مع روابط"""
    query = update.callback_query
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT e.season, e.episode_number, e.telegram_message_id,
                       e.telegram_channel_id,
                       s.name as series_name, s.type as series_type, s.id as series_id
                FROM episodes e
                JOIN series s ON e.series_id = s.id
                WHERE e.id = :episode_id
            """), {"episode_id": episode_id}).fetchone()
        
        if not result:
            await query.edit_message_text("❌ الحلقة/الجزء غير موجود.")
            return
        
        season, episode_num, msg_id, channel_id, series_name, series_type, series_id = result
        
        # بناء الرابط الصحيح بناءً على معرف القناة المخزن
        if msg_id and channel_id:
            # إذا كان channel_id يبدأ بـ @ فهو معرف مستخدم
            if channel_id.startswith('@'):
                channel_username = channel_id[1:]  # إزالة @
                episode_link = f"https://t.me/{channel_username}/{msg_id}"
            else:
                # إذا كان معرفًا رقميًا
                episode_link = f"https://t.me/c/{channel_id}/{msg_id}"
            
            if series_type == 'series':
                link_text = f"🔗 [رابط الحلقة في القناة]({episode_link})"
                title_text = f"*{series_name}*\nالموسم {season} - الحلقة {episode_num}"
                button_text = "مشاهدة الحلقة"
            else:
                link_text = f"🔗 [رابط الجزء في القناة]({episode_link})"
                title_text = f"*{series_name}*\nالجزء {season}"
                button_text = "مشاهدة الجزء"
        else:
            episode_link = None
            link_text = "⚠️ تعذر إنشاء رابط للحلقة/الجزء."
            if series_type == 'series':
                title_text = f"*{series_name}*\nالموسم {season} - الحلقة {episode_num}"
                button_text = "مشاهدة الحلقة"
            else:
                title_text = f"*{series_name}*\nالجزء {season}"
                button_text = "مشاهدة الجزء"
        
        message_text = (
            f"{title_text}\n\n"
            f"{link_text}\n\n"
            f"*القناة:* {channel_id}\n"
            f"*ملاحظة:* تأكد من أنك منضم للقناة لمشاهدة المحتوى."
        )
        
        # بناء لوحة المفاتيح
        keyboard = []
        if episode_link:
            keyboard.append([InlineKeyboardButton(button_text, url=episode_link)])
        
        keyboard.append([
            InlineKeyboardButton("⬅️ رجوع للمحتوى", callback_data=f"content_{series_id}"),
            InlineKeyboardButton("🏠 الرئيسية", callback_data="home")
        ])
        
        await query.edit_message_text(
            message_text,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard),
            disable_web_page_preview=False
        )
        
    except Exception as e:
        logger.error(f"خطأ في show_episode_details: {e}")
        await query.edit_message_text("⚠️ حدث خطأ أثناء جلب معلومات الحلقة. يرجى المحاولة مرة أخرى.")

async def test_db_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """اختبار قاعدة البيانات من الزر"""
    query = update.callback_query
    
    try:
        if not engine:
            await query.edit_message_text("❌ قاعدة البيانات غير متصلة.")
            return
        
        with engine.connect() as conn:
            # جلب إحصائيات بسيطة
            series_count = conn.execute(text("SELECT COUNT(*) FROM series WHERE type = 'series'")).scalar()
            movies_count = conn.execute(text("SELECT COUNT(*) FROM series WHERE type = 'movie'")).scalar()
            
            # جلب بعض الأمثلة
            series_examples = conn.execute(text("""
                SELECT name FROM series WHERE type = 'series' ORDER BY id LIMIT 3
            """)).fetchall()
            
            movies_examples = conn.execute(text("""
                SELECT name FROM series WHERE type = 'movie' ORDER BY id LIMIT 3
            """)).fetchall()
            
            # جلب القنوات المختلفة
            distinct_channels = conn.execute(text("""
                SELECT DISTINCT telegram_channel_id FROM episodes LIMIT 5
            """)).fetchall()
        
        series_names = [row[0] for row in series_examples] if series_examples else ["لا يوجد"]
        movies_names = [row[0] for row in movies_examples] if movies_examples else ["لا يوجد"]
        channels = [row[0] for row in distinct_channels] if distinct_channels else ["لا يوجد"]
        
        reply_text = (
            f"✅ *اختبار قاعدة البيانات:*\n\n"
            f"📊 *الإحصائيات:*\n"
            f"• عدد المسلسلات: {series_count}\n"
            f"• عدد الأفلام: {movies_count}\n"
            f"• عدد القنوات المختلفة: {len(channels)}\n\n"
            f"📺 *أمثلة على المسلسلات:*\n"
            f"{chr(10).join(['• ' + name for name in series_names])}\n\n"
            f"🎬 *أمثلة على الأفلام:*\n"
            f"{chr(10).join(['• ' + name for name in movies_names])}\n\n"
            f"📡 *القنوات المتاحة:*\n"
            f"{chr(10).join(['• ' + channel for channel in channels])}\n\n"
            f"ℹ️ *ملاحظة:* تأكد من أن الروابط تعمل من القنوات الصحيحة."
        )
        
        keyboard = [
            [InlineKeyboardButton("📺 عرض المسلسلات", callback_data="series_list"),
             InlineKeyboardButton("🎬 عرض الأفلام", callback_data="movies_list")],
            [InlineKeyboardButton("🏠 الرئيسية", callback_data="home")]
        ]
        
        await query.edit_message_text(
            reply_text,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"خطأ في test_db_button: {e}")
        await query.edit_message_text(f"❌ خطأ في اختبار قاعدة البيانات:\n`{str(e)[:200]}`")

# ==============================
# 7. الدالة الرئيسية مع تحسينات الاتصال
# ==============================
def main():
    """الدالة الرئيسية لتشغيل البوت مع تحسينات الاتصال"""
    try:
        # إنشاء تطبيق البوت مع مهلة أطول
        application = Application.builder().token(BOT_TOKEN).build()
        
        # إضافة Handlers
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("series", series_command))
        application.add_handler(CommandHandler("movies", movies_command))
        application.add_handler(CommandHandler("all", all_command))
        application.add_handler(CommandHandler("test", test_db_command))
        application.add_handler(CallbackQueryHandler(button_handler))
        
        # تشغيل البوت
        print("🤖 البوت يعمل باستخدام Polling...")
        print(f"✅ تم الاتصال بقاعدة البيانات: {engine is not None}")
        print("🔄 إعدادات الاتصال:")
        print(f"   - BOT_TOKEN: {'✅ موجود' if BOT_TOKEN else '❌ غير موجود'}")
        print(f"   - DATABASE_URL: {'✅ موجود' if DATABASE_URL else '❌ غير موجود'}")
        
        application.run_polling(
            poll_interval=1.0,
            timeout=30,
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES
        )
        
    except Exception as e:
        print(f"❌ خطأ فادح في تشغيل البوت: {e}")
        print("🔄 إعادة تشغيل البوت بعد 5 ثوان...")
        import time
        time.sleep(5)
        # إعادة التشغيل
        main()

if __name__ == "__main__":
    main()
