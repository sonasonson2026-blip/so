import os
import logging
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes
)
from sqlalchemy import create_engine, text

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
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ تم الاتصال بقاعدة البيانات بنجاح.")

        with engine.connect() as conn:
            series_count = conn.execute(text("SELECT COUNT(*) FROM series WHERE type = 'series'")).scalar()
            movies_count = conn.execute(text("SELECT COUNT(*) FROM series WHERE type = 'movie'")).scalar()
            print(f"📊 في الاختبار المبدئي:")
            print(f"   - عدد المسلسلات: {series_count}")
            print(f"   - عدد الأفلام: {movies_count}")

        # إنشاء فهرس لتحسين الأداء
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_episodes_series_season ON episodes(series_id, season, episode_number)"))
                print("✅ تم إنشاء/التحقق من فهرس idx_episodes_series_season.")
        except Exception as e:
            print(f"⚠️ ملاحظة حول إنشاء الفهرس: {e}")

    except Exception as e:
        print(f"❌ فشل الاتصال بقاعدة البيانات: {e}")
        engine = None

# ==============================
# 2. دوال المساعدة للتعامل مع قاعدة البيانات
# ==============================
async def get_all_content(content_type=None):
    """جلب جميع المحتويات من قاعدة البيانات حسب النوع (مسلسلات/أفلام)"""
    if not engine:
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
            result = conn.execute(text(query))
            return result.fetchall()
    except Exception as e:
        logger.error(f"خطأ في جلب المحتويات: {e}")
        return []

async def get_content_episodes(series_id, page=1, per_page=50):
    """جلب حلقات/أجزاء محتوى محدد مع دعم التقسيم إلى صفحات"""
    if not engine:
        return [], 0, 0
    try:
        with engine.connect() as conn:
            count_result = conn.execute(text("""
                SELECT COUNT(*) FROM episodes WHERE series_id = :series_id
            """), {"series_id": series_id})
            total_episodes = count_result.scalar()
            total_pages = (total_episodes + per_page - 1) // per_page
            page = max(1, min(page, total_pages)) if total_pages > 0 else 1
            offset = (page - 1) * per_page

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
            return result.fetchall(), total_episodes, total_pages
    except Exception as e:
        logger.error(f"خطأ في جلب حلقات المحتوى {series_id}: {e}")
        return [], 0, 0

async def get_content_info(series_id):
    """جلب معلومات محتوى محدد"""
    if not engine:
        return None
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT id, name, type FROM series WHERE id = :series_id
            """), {"series_id": series_id})
            return result.fetchone()
    except Exception as e:
        logger.error(f"خطأ في جلب معلومات المحتوى {series_id}: {e}")
        return None

async def get_seasons_stats(series_id):
    """جلب إحصائيات المواسم لمسلسل معين: رقم الموسم وعدد حلقاته"""
    if not engine:
        return []
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT season, COUNT(*) as episode_count
                FROM episodes
                WHERE series_id = :series_id
                GROUP BY season
                ORDER BY season
            """), {"series_id": series_id})
            return result.fetchall()
    except Exception as e:
        logger.error(f"خطأ في جلب إحصائيات المواسم: {e}")
        return []

# ==============================
# 3. دوال البوت الرئيسية
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
        """

        if update.callback_query:
            await update.callback_query.edit_message_text(
                welcome_text, parse_mode='Markdown', reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                welcome_text, parse_mode='Markdown', reply_markup=reply_markup
            )
    except Exception as e:
        logger.error(f"خطأ في start: {e}")

async def show_content(update: Update, context: ContextTypes.DEFAULT_TYPE, content_type=None):
    """عرض المحتويات حسب النوع"""
    try:
        if not engine:
            msg = "❌ قاعدة البيانات غير متاحة حالياً."
            if update.callback_query:
                await update.callback_query.edit_message_text(msg)
            else:
                await update.message.reply_text(msg)
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
            msg = f"{empty_msg}\n\nℹ️ *ملاحظة:* يمكنك استخدام زر 'اختبار قاعدة البيانات' للتحقق."
            if update.callback_query:
                await update.callback_query.edit_message_text(msg)
            else:
                await update.message.reply_text(msg)
            return

        text = f"{title}\n\n"
        keyboard = []
        for content in content_list:
            content_id, name, ctype, ep_count, channel_count = content
            if ctype == 'series':
                count_text = f"{ep_count} حلقة في {channel_count} قناة" if ep_count > 0 else "بدون حلقات"
            else:
                count_text = f"{ep_count} جزء في {channel_count} قناة" if ep_count > 0 else "بدون أجزاء"
            text += f"• {name} ({count_text})\n"
            keyboard.append([InlineKeyboardButton(f"{name[:20]} ({ep_count})", callback_data=f"content_{content_id}")])

        keyboard.append([
            InlineKeyboardButton("📺 المسلسلات", callback_data="series_list"),
            InlineKeyboardButton("🎬 الأفلام", callback_data="movies_list")
        ])
        keyboard.append([InlineKeyboardButton("🏠 الرئيسية", callback_data="home")])

        if update.callback_query:
            await update.callback_query.edit_message_text(
                text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await update.message.reply_text(
                text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard)
            )
    except Exception as e:
        logger.error(f"خطأ في show_content: {e}")

async def series_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_content(update, context, 'series')

async def movies_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_content(update, context, 'movie')

async def all_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_content(update, context)

async def test_db_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """أمر /test - اختبار قاعدة البيانات"""
    try:
        if not engine:
            await update.message.reply_text("❌ قاعدة البيانات غير متصلة.")
            return
        with engine.connect() as conn:
            tables = conn.execute(text("""
                SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'
            """)).fetchall()
            tables_info = "📋 *الجداول الموجودة:*\n"
            for table in tables:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {table[0]}")).scalar()
                tables_info += f"• `{table[0]}`: {count} صف\n"

            series_sample = conn.execute(text("SELECT id, name, type FROM series ORDER BY id LIMIT 5")).fetchall()
            episodes_sample = conn.execute(text("SELECT id, series_id, season, episode_number, telegram_channel_id FROM episodes ORDER BY id LIMIT 5")).fetchall()

            series_text = "🎬 *عينة من المسلسلات والأفلام:*\n"
            for row in series_sample:
                series_text += f"• ID:{row[0]} - {row[1]} ({row[2]})\n"

            episodes_text = "📺 *عينة من الحلقات:*\n"
            for row in episodes_sample:
                episodes_text += f"• ID:{row[0]} - مسلسل:{row[1]} - م{row[2]} ح{row[3]} - قناة:{row[4]}\n"

        await update.message.reply_text(f"{tables_info}\n{series_text}\n{episodes_text}", parse_mode='Markdown')
    except Exception as e:
        await update.message.reply_text(f"❌ خطأ في اختبار قاعدة البيانات:\n`{str(e)[:300]}`")

# ==============================
# 4. عرض تفاصيل المحتوى والمواسم والحلقات
# ==============================
async def show_content_details(update: Update, context: ContextTypes.DEFAULT_TYPE, content_id, page=1):
    """عرض تفاصيل محتوى محدد (مسلسل أو فيلم)"""
    query = update.callback_query
    try:
        content_info = await get_content_info(content_id)
        if not content_info:
            await query.edit_message_text("❌ المحتوى غير موجود.")
            return

        content_id, name, content_type = content_info

        # جلب القنوات
        channels = []
        if engine:
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT DISTINCT telegram_channel_id FROM episodes WHERE series_id = :series_id
                """), {"series_id": content_id}).fetchall()
                channels = [row[0] for row in result]

        message_text = f"*{name}*\n\n"
        if channels:
            message_text += f"*القنوات:* {', '.join(channels)}\n\n"

        keyboard = []

        if content_type == 'series':
            seasons_stats = await get_seasons_stats(content_id)
            if not seasons_stats:
                message_text += "لا توجد حلقات بعد."
            else:
                total_seasons = len(seasons_stats)
                total_episodes = sum(row[1] for row in seasons_stats)
                message_text += f"عدد المواسم: {total_seasons}\n"
                message_text += f"إجمالي الحلقات: {total_episodes}\n\n"
                message_text += "اختر الموسم:"
                for season_num, ep_count in seasons_stats:
                    keyboard.append([
                        InlineKeyboardButton(
                            f"الموسم {season_num} ({ep_count} حلقة)",
                            callback_data=f"season_{content_id}_{season_num}"
                        )
                    ])

        else:  # movie
            episodes, total_episodes, total_pages = await get_content_episodes(content_id, page)
            if not episodes:
                message_text += "لا توجد أجزاء بعد."
            else:
                if total_episodes > 0:
                    message_text += f"عدد الأجزاء: {total_episodes}\n"
                    if total_pages > 1:
                        message_text += f"الصفحة {page} من {total_pages}\n\n"

                seasons = {}
                for ep in episodes:
                    ep_id, season, ep_num, msg_id, channel_id = ep
                    seasons.setdefault(season, []).append((ep_id, ep_num, msg_id, channel_id))

                if len(seasons) > 1:
                    message_text += "اختر الجزء:"
                    for season_num in sorted(seasons.keys()):
                        ep_count = len(seasons[season_num])
                        keyboard.append([
                            InlineKeyboardButton(
                                f"الجزء {season_num} ({ep_count})",
                                callback_data=f"season_{content_id}_{season_num}"
                            )
                        ])
                else:
                    season_num = next(iter(seasons)) if seasons else 1
                    season_episodes = seasons.get(season_num, [])
                    if season_episodes:
                        ep_id = season_episodes[0][0]
                        message_text += "اضغط على الزر أدناه لمشاهدة الفيلم:"
                        keyboard = [[InlineKeyboardButton("مشاهدة الفيلم", callback_data=f"ep_{ep_id}")]]

                # أزرار التنقل بين الصفحات للأفلام (إذا كان هناك أكثر من صفحة)
                if total_pages > 1:
                    nav_buttons = []
                    if page > 1:
                        nav_buttons.append(InlineKeyboardButton("⬅️ السابقة", callback_data=f"content_page_{content_id}_{page-1}"))
                    nav_buttons.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="page_info"))
                    if page < total_pages:
                        nav_buttons.append(InlineKeyboardButton("التالية ➡️", callback_data=f"content_page_{content_id}_{page+1}"))
                    keyboard.append(nav_buttons)

        # أزرار الرجوع والرئيسية
        keyboard.append([
            InlineKeyboardButton("⬅️ رجوع", callback_data=f"{content_type}_list"),
            InlineKeyboardButton("🏠 الرئيسية", callback_data="home")
        ])

        await query.edit_message_text(
            message_text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"خطأ في show_content_details: {e}")
        await query.edit_message_text("⚠️ حدث خطأ أثناء جلب البيانات.")

async def show_season_episodes(update: Update, context: ContextTypes.DEFAULT_TYPE, content_id, season_num, page=1):
    """عرض حلقات موسم محدد لمسلسل مع دعم الصفحات"""
    query = update.callback_query
    try:
        content_info = await get_content_info(content_id)
        if not content_info:
            await query.edit_message_text("❌ المحتوى غير موجود.")
            return

        content_id, name, content_type = content_info
        if content_type != 'series':
            await query.edit_message_text("❌ هذه الدالة للمسلسلات فقط.")
            return

        with engine.connect() as conn:
            count_result = conn.execute(text("""
                SELECT COUNT(*) FROM episodes WHERE series_id = :series_id AND season = :season
            """), {"series_id": content_id, "season": season_num})
            total_episodes = count_result.scalar()
            per_page = 50
            total_pages = (total_episodes + per_page - 1) // per_page
            page = max(1, min(page, total_pages)) if total_pages > 0 else 1
            offset = (page - 1) * per_page

            result = conn.execute(text("""
                SELECT e.id, e.episode_number, e.telegram_message_id, e.telegram_channel_id
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
        message_text += f"عدد الحلقات: {total_episodes}\n"
        if total_pages > 1:
            message_text += f"الصفحة {page} من {total_pages}\n\n"
        message_text += "اختر الحلقة:"

        keyboard = []
        row_buttons = []
        for ep in episodes:
            ep_id, ep_num, msg_id, channel_id = ep
            row_buttons.append(InlineKeyboardButton(f"الحلقة {ep_num}", callback_data=f"ep_{ep_id}"))
            if len(row_buttons) == 5:
                keyboard.append(row_buttons)
                row_buttons = []
        if row_buttons:
            keyboard.append(row_buttons)

        if total_pages > 1:
            nav_buttons = []
            if page > 1:
                nav_buttons.append(InlineKeyboardButton("⬅️ السابقة", callback_data=f"season_page_{content_id}_{season_num}_{page-1}"))
            nav_buttons.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="page_info"))
            if page < total_pages:
                nav_buttons.append(InlineKeyboardButton("التالية ➡️", callback_data=f"season_page_{content_id}_{season_num}_{page+1}"))
            keyboard.append(nav_buttons)

        keyboard.append([
            InlineKeyboardButton("⬅️ رجوع للمسلسل", callback_data=f"content_{content_id}"),
            InlineKeyboardButton("🏠 الرئيسية", callback_data="home")
        ])

        await query.edit_message_text(
            message_text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"خطأ في show_season_episodes: {e}")
        await query.edit_message_text("⚠️ حدث خطأ أثناء جلب البيانات.")

async def show_episode_details(update: Update, context: ContextTypes.DEFAULT_TYPE, episode_id):
    """عرض تفاصيل حلقة/جزء مع رابط"""
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

        if msg_id and channel_id:
            if channel_id.startswith('@'):
                episode_link = f"https://t.me/{channel_id[1:]}/{msg_id}"
            else:
                episode_link = f"https://t.me/c/{channel_id}/{msg_id}"

            if series_type == 'series':
                title = f"*{series_name}*\nالموسم {season} - الحلقة {episode_num}"
                button_text = "مشاهدة الحلقة"
            else:
                title = f"*{series_name}*\nالجزء {season}"
                button_text = "مشاهدة الجزء"
            link_text = f"🔗 [رابط في القناة]({episode_link})"
        else:
            title = f"*{series_name}*"
            link_text = "⚠️ تعذر إنشاء الرابط"
            button_text = "مشاهدة"

        message_text = f"{title}\n\n{link_text}\n\n*القناة:* {channel_id}\n*ملاحظة:* تأكد من انضمامك للقناة."

        keyboard = []
        if msg_id and channel_id:
            keyboard.append([InlineKeyboardButton(button_text, url=episode_link)])
        keyboard.append([
            InlineKeyboardButton("⬅️ رجوع للمحتوى", callback_data=f"content_{series_id}"),
            InlineKeyboardButton("🏠 الرئيسية", callback_data="home")
        ])

        await query.edit_message_text(
            message_text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"خطأ في show_episode_details: {e}")
        await query.edit_message_text("⚠️ حدث خطأ.")

# ==============================
# 5. معالج الأزرار مع تصحيح خطأ "page"
# ==============================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    # محاولة الإجابة مع إعادة المحاولة
    for attempt in range(3):
        try:
            await query.answer()
            break
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(1)
            else:
                logger.error(f"فشل answer: {e}")
                return

    data = query.data
    try:
        # الأزرار الثابتة
        if data == 'home':
            await start(update, context)
        elif data == 'test_db':
            await test_db_button(update, context)
        elif data == 'all_content':
            await show_content(update, context)
        elif data == 'series_list':
            await show_content(update, context, 'series')
        elif data == 'movies_list':
            await show_content(update, context, 'movie')
        elif data == 'page_info' or data == 'page':   # تصحيح: التعامل مع "page" أيضًا
            # لا تفعل شيئًا لزر معلومات الصفحة
            return

        # أزرار المحتوى مع الصفحات
        elif data.startswith('content_page_'):
            parts = data.split('_')
            if len(parts) >= 4:
                content_id = int(parts[2])
                page = int(parts[3])
                await show_content_details(update, context, content_id, page)
            else:
                logger.warning(f"تنسيق غير متوقع لـ content_page_: {data}")

        elif data.startswith('content_'):
            content_id = int(data.split('_')[1])
            await show_content_details(update, context, content_id, 1)

        elif data.startswith('ep_'):
            episode_id = int(data.split('_')[1])
            await show_episode_details(update, context, episode_id)

        elif data.startswith('season_page_'):
            parts = data.split('_')
            if len(parts) >= 5:
                content_id = int(parts[2])
                season_num = int(parts[3])
                page = int(parts[4])
                await show_season_episodes(update, context, content_id, season_num, page)
            else:
                logger.warning(f"تنسيق غير متوقع لـ season_page_: {data}")

        elif data.startswith('season_'):
            parts = data.split('_')
            content_id = int(parts[1])
            season_num = int(parts[2])
            await show_season_episodes(update, context, content_id, season_num, 1)

        else:
            logger.warning(f"زر غير معروف: {data}")
    except Exception as e:
        logger.error(f"خطأ في button_handler: {e}")
        await query.edit_message_text("⚠️ حدث خطأ أثناء معالجة طلبك.")

async def test_db_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """اختبار قاعدة البيانات من الزر"""
    query = update.callback_query
    try:
        if not engine:
            await query.edit_message_text("❌ قاعدة البيانات غير متصلة.")
            return

        with engine.connect() as conn:
            series_count = conn.execute(text("SELECT COUNT(*) FROM series WHERE type = 'series'")).scalar()
            movies_count = conn.execute(text("SELECT COUNT(*) FROM series WHERE type = 'movie'")).scalar()
            series_ex = conn.execute(text("SELECT name FROM series WHERE type = 'series' ORDER BY id LIMIT 3")).fetchall()
            movies_ex = conn.execute(text("SELECT name FROM series WHERE type = 'movie' ORDER BY id LIMIT 3")).fetchall()
            channels = conn.execute(text("SELECT DISTINCT telegram_channel_id FROM episodes LIMIT 5")).fetchall()

        series_names = [row[0] for row in series_ex] or ["لا يوجد"]
        movies_names = [row[0] for row in movies_ex] or ["لا يوجد"]
        channels_list = [row[0] for row in channels] or ["لا يوجد"]

        reply = (
            f"✅ *اختبار قاعدة البيانات:*\n\n"
            f"• عدد المسلسلات: {series_count}\n"
            f"• عدد الأفلام: {movies_count}\n"
            f"• عدد القنوات المختلفة: {len(channels_list)}\n\n"
            f"📺 *أمثلة مسلسلات:*\n" + "\n".join(f"• {n}" for n in series_names) + "\n\n"
            f"🎬 *أمثلة أفلام:*\n" + "\n".join(f"• {n}" for n in movies_names) + "\n\n"
            f"📡 *قنوات:*\n" + "\n".join(f"• {c}" for c in channels_list)
        )

        keyboard = [
            [InlineKeyboardButton("📺 المسلسلات", callback_data="series_list"),
             InlineKeyboardButton("🎬 الأفلام", callback_data="movies_list")],
            [InlineKeyboardButton("🏠 الرئيسية", callback_data="home")]
        ]
        await query.edit_message_text(reply, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"خطأ في test_db_button: {e}")
        await query.edit_message_text(f"❌ خطأ: {str(e)[:200]}")

# ==============================
# 6. الدالة الرئيسية
# ==============================
def main():
    try:
        app = Application.builder().token(BOT_TOKEN).build()
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("series", series_command))
        app.add_handler(CommandHandler("movies", movies_command))
        app.add_handler(CommandHandler("all", all_command))
        app.add_handler(CommandHandler("test", test_db_command))
        app.add_handler(CallbackQueryHandler(button_handler))

        print("🤖 البوت يعمل...")
        print(f"✅ قاعدة البيانات: {engine is not None}")
        app.run_polling(poll_interval=1.0, timeout=30, drop_pending_updates=True)
    except Exception as e:
        print(f"❌ خطأ فادح: {e}")
        import time
        time.sleep(5)
        main()

if __name__ == "__main__":
    main()
