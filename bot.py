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

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

engine = None
if DATABASE_URL:
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ تم الاتصال بقاعدة البيانات بنجاح.")
    except Exception as e:
        print(f"❌ فشل الاتصال بقاعدة البيانات: {e}")
        engine = None

# ==============================
# 2. دوال المساعدة (الترتيب حسب آخر معرف رسالة)
# ==============================
async def get_all_content(content_type=None):
    """جلب جميع المحتويات مع ترتيبها حسب آخر معرف رسالة (الأحدث في الأسفل)."""
    if not engine:
        return []
    try:
        with engine.connect() as conn:
            query = """
                SELECT s.id, s.name, s.type, 
                       COUNT(e.id) as episode_count,
                       COUNT(DISTINCT e.telegram_channel_id) as channel_count,
                       MAX(e.telegram_message_id) as last_msg_id
                FROM series s
                LEFT JOIN episodes e ON s.id = e.series_id
            """
            if content_type:
                query += f" WHERE s.type = '{content_type}'"
            query += """
                GROUP BY s.id, s.name, s.type
                ORDER BY last_msg_id ASC NULLS LAST
            """
            result = conn.execute(text(query))
            return result.fetchall()
    except Exception as e:
        logger.error(f"خطأ في جلب المحتويات: {e}")
        return []

async def get_content_info(series_id):
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

async def get_season_episodes(series_id, season, page=1, per_page=50):
    if not engine:
        return [], 0, 0, page
    try:
        with engine.connect() as conn:
            count_result = conn.execute(text("""
                SELECT COUNT(*) FROM episodes 
                WHERE series_id = :series_id AND season = :season
            """), {"series_id": series_id, "season": season})
            total_episodes = count_result.scalar()
            total_pages = (total_episodes + per_page - 1) // per_page if total_episodes > 0 else 0
            if page < 1:
                page = 1
            elif page > total_pages and total_pages > 0:
                page = total_pages
            offset = (page - 1) * per_page
            result = conn.execute(text("""
                SELECT e.id, e.season, e.episode_number, 
                       e.telegram_message_id, e.telegram_channel_id
                FROM episodes e
                WHERE e.series_id = :series_id AND e.season = :season
                ORDER BY e.episode_number ASC
                LIMIT :limit OFFSET :offset
            """), {
                "series_id": series_id,
                "season": season,
                "limit": per_page,
                "offset": offset
            })
            episodes = result.fetchall()
            return episodes, total_episodes, total_pages, page
    except Exception as e:
        logger.error(f"خطأ في get_season_episodes: {e}")
        return [], 0, 0, page

async def get_movie_parts(series_id):
    if not engine:
        return []
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT season, COUNT(*) as cnt
                FROM episodes
                WHERE series_id = :series_id
                GROUP BY season
                ORDER BY season ASC
            """), {"series_id": series_id})
            return result.fetchall()
    except Exception as e:
        logger.error(f"خطأ في get_movie_parts: {e}")
        return []

# ==============================
# 3. أوامر البوت
# ==============================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📺 المسلسلات", callback_data='series_list'),
         InlineKeyboardButton("🎬 الأفلام", callback_data='movies_list')],
        [InlineKeyboardButton("📁 جميع المحتويات", callback_data='all_content')],
        [InlineKeyboardButton("🔄 اختبار قاعدة البيانات", callback_data='test_db')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    welcome_text = """
🎬 <b>مرحباً في بوت مسلسلاتي وأفلامي</b> 🎬

<b>مميزات البوت:</b>
• تصفح جميع المسلسلات في القناة
• تصفح جميع الأفلام في القناة
• الوصول السريع للحلقات والأجزاء

📌 <b>الأوامر المتاحة:</b>
/start - عرض هذه الرسالة
/series - عرض المسلسلات
/movies - عرض الأفلام
/all - عرض كل المحتويات
    """
    if update.callback_query:
        await update.callback_query.edit_message_text(
            welcome_text, parse_mode='HTML', reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            welcome_text, parse_mode='HTML', reply_markup=reply_markup
        )

async def show_content(update: Update, context: ContextTypes.DEFAULT_TYPE, content_type=None):
    if not engine:
        msg = "❌ قاعدة البيانات غير متاحة حالياً."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return

    content_list = await get_all_content(content_type)

    if content_type == 'series':
        title = "📺 <b>قائمة المسلسلات</b>"
        empty_msg = "📭 لا توجد مسلسلات حالياً."
    elif content_type == 'movie':
        title = "🎬 <b>قائمة الأفلام</b>"
        empty_msg = "📭 لا توجد أفلام حالياً."
    else:
        title = "📁 <b>جميع المحتويات</b>"
        empty_msg = "📭 لا توجد محتويات حالياً."

    if not content_list:
        no_data_msg = f"{empty_msg}\n\nℹ️ <b>ملاحظة:</b> يمكنك استخدام زر 'اختبار قاعدة البيانات' للتحقق."
        if update.callback_query:
            await update.callback_query.edit_message_text(no_data_msg)
        else:
            await update.message.reply_text(no_data_msg)
        return

    text = f"{title}\n\n"
    keyboard = []
    for content in content_list:
        content_id, name, ctype, ep_count, ch_count, last_msg_id = content
        if ctype == 'series':
            count_text = f"{ep_count} حلقة في {ch_count} قناة" if ep_count > 0 else "بدون حلقات"
        else:
            count_text = f"{ep_count} جزء في {ch_count} قناة" if ep_count > 0 else "بدون أجزاء"
        text += f"• {name} ({count_text})\n"
        keyboard.append([
            InlineKeyboardButton(
                f"{name[:20]} ({ep_count})",
                callback_data=f"content_{content_id}"
            )
        ])

    keyboard.append([
        InlineKeyboardButton("📺 المسلسلات", callback_data="series_list"),
        InlineKeyboardButton("🎬 الأفلام", callback_data="movies_list")
    ])
    keyboard.append([InlineKeyboardButton("🏠 الرئيسية", callback_data="home")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.edit_message_text(
            text, parse_mode='HTML', reply_markup=reply_markup
        )
    else:
        await update.message.reply_text(
            text, parse_mode='HTML', reply_markup=reply_markup
        )

async def series_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_content(update, context, 'series')

async def movies_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_content(update, context, 'movie')

async def all_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_content(update, context)

# ==============================
# 4. عرض التفاصيل
# ==============================
async def show_content_details(update: Update, context: ContextTypes.DEFAULT_TYPE, content_id, page=1):
    query = update.callback_query
    try:
        content_info = await get_content_info(content_id)
        if not content_info:
            await query.edit_message_text("❌ المحتوى غير موجود.")
            return
        content_id, name, content_type = content_info

        channels = []
        if engine:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT DISTINCT telegram_channel_id FROM episodes WHERE series_id = :series_id"), {"series_id": content_id}).fetchall()
                channels = [r[0] for r in res]

        message_text = f"<b>{name}</b>\n\n"
        if channels:
            message_text += f"<b>القنوات:</b> {', '.join(channels)}\n\n"
        keyboard = []

        if content_type == 'series':
            with engine.connect() as conn:
                seasons = conn.execute(text("""
                    SELECT season, COUNT(*) as cnt
                    FROM episodes
                    WHERE series_id = :series_id
                    GROUP BY season
                    ORDER BY season
                """), {"series_id": content_id}).fetchall()
            if not seasons:
                message_text += "📭 لا توجد حلقات لهذا المسلسل حالياً."
                keyboard.append([InlineKeyboardButton("⬅️ رجوع", callback_data="series_list")])
                await query.edit_message_text(message_text, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            if len(seasons) > 1:
                message_text += "اختر الموسم:"
                for s, cnt in seasons:
                    keyboard.append([InlineKeyboardButton(f"الموسم {s} ({cnt} حلقة)", callback_data=f"season_{content_id}_{s}")])
            else:
                season = seasons[0][0]
                await show_season_episodes(update, context, content_id, season, page)
                return
        else:
            parts = await get_movie_parts(content_id)
            if not parts:
                message_text += "📭 لا توجد أجزاء لهذا الفيلم حالياً."
                keyboard.append([InlineKeyboardButton("⬅️ رجوع", callback_data="movies_list")])
                await query.edit_message_text(message_text, reply_markup=InlineKeyboardMarkup(keyboard))
                return
            if len(parts) > 1:
                message_text += "اختر الجزء:"
                for p, _ in parts:
                    with engine.connect() as conn:
                        ep_id = conn.execute(text("""
                            SELECT id FROM episodes
                            WHERE series_id = :series_id AND season = :season
                            ORDER BY episode_number LIMIT 1
                        """), {"series_id": content_id, "season": p}).scalar()
                    keyboard.append([InlineKeyboardButton(f"الجزء {p}", callback_data=f"ep_{ep_id}")])
            else:
                p = parts[0][0]
                with engine.connect() as conn:
                    ep_id = conn.execute(text("""
                        SELECT id FROM episodes
                        WHERE series_id = :series_id AND season = :season
                        ORDER BY episode_number LIMIT 1
                    """), {"series_id": content_id, "season": p}).scalar()
                message_text += "اضغط على الزر أدناه لمشاهدة الفيلم:"
                keyboard = [[InlineKeyboardButton("مشاهدة الفيلم", callback_data=f"ep_{ep_id}")]]

        keyboard.append([InlineKeyboardButton("⬅️ رجوع", callback_data=f"{content_type}_list"), InlineKeyboardButton("🏠 الرئيسية", callback_data="home")])
        await query.edit_message_text(message_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"خطأ في show_content_details: {e}")
        await query.edit_message_text("⚠️ حدث خطأ أثناء جلب البيانات. يرجى المحاولة مرة أخرى.")

async def show_season_episodes(update: Update, context: ContextTypes.DEFAULT_TYPE, content_id, season_num, page=1):
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

        episodes, total_episodes, total_pages, actual_page = await get_season_episodes(content_id, season_num, page)
        if not episodes:
            await query.edit_message_text(f"❌ لا توجد حلقات للموسم {season_num}.")
            return

        page = actual_page
        message_text = f"<b>{name}</b>\nالموسم {season_num}\n\n"
        if total_episodes > 0:
            message_text += f"عدد الحلقات: {total_episodes}\n"
            if total_pages > 1:
                message_text += f"الصفحة {page} من {total_pages}\n\n"
        message_text += "اختر الحلقة:"

        keyboard = []
        row_buttons = []
        for ep in episodes:
            ep_id, _, ep_num, _, _ = ep
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

        keyboard.append([InlineKeyboardButton("⬅️ رجوع للمسلسل", callback_data=f"content_{content_id}"), InlineKeyboardButton("🏠 الرئيسية", callback_data="home")])
        await query.edit_message_text(message_text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"خطأ في show_season_episodes: {e}")
        await query.edit_message_text("⚠️ حدث خطأ أثناء جلب البيانات. يرجى المحاولة مرة أخرى.")

async def show_episode_details(update: Update, context: ContextTypes.DEFAULT_TYPE, episode_id):
    query = update.callback_query
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT e.season, e.episode_number, e.telegram_message_id, e.telegram_channel_id,
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
                link = f"https://t.me/{channel_id[1:]}/{msg_id}"
            else:
                link = f"https://t.me/c/{channel_id}/{msg_id}"
            if series_type == 'series':
                title = f"<b>{series_name}</b>\nالموسم {season} - الحلقة {episode_num}"
                btn_text = "مشاهدة الحلقة"
            else:
                title = f"<b>{series_name}</b>\nالجزء {season}"
                btn_text = "مشاهدة الجزء"
            link_text = f"🔗 <a href='{link}'>رابط المحتوى في القناة</a>"
        else:
            link = None
            link_text = "⚠️ تعذر إنشاء رابط."
            if series_type == 'series':
                title = f"<b>{series_name}</b>\nالموسم {season} - الحلقة {episode_num}"
                btn_text = "مشاهدة الحلقة"
            else:
                title = f"<b>{series_name}</b>\nالجزء {season}"
                btn_text = "مشاهدة الجزء"

        msg = f"{title}\n\n{link_text}\n\n<b>القناة:</b> {channel_id}\n<b>ملاحظة:</b> تأكد من أنك منضم للقناة."
        keyboard = []
        if link:
            keyboard.append([InlineKeyboardButton(btn_text, url=link)])
        keyboard.append([InlineKeyboardButton("⬅️ رجوع للمحتوى", callback_data=f"content_{series_id}"), InlineKeyboardButton("🏠 الرئيسية", callback_data="home")])
        await query.edit_message_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard), disable_web_page_preview=False)
    except Exception as e:
        logger.error(f"خطأ في show_episode_details: {e}")
        await query.edit_message_text("⚠️ حدث خطأ أثناء جلب المعلومات.")

# ==============================
# 5. اختبار قاعدة البيانات (اختياري)
# ==============================
async def test_db_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        if not engine:
            await query.edit_message_text("❌ قاعدة البيانات غير متصلة.")
            return
        with engine.connect() as conn:
            series_cnt = conn.execute(text("SELECT COUNT(*) FROM series WHERE type='series'")).scalar()
            movies_cnt = conn.execute(text("SELECT COUNT(*) FROM series WHERE type='movie'")).scalar()
            series_ex = conn.execute(text("SELECT name FROM series WHERE type='series' ORDER BY id LIMIT 3")).fetchall()
            movies_ex = conn.execute(text("SELECT name FROM series WHERE type='movie' ORDER BY id LIMIT 3")).fetchall()
            channels = conn.execute(text("SELECT DISTINCT telegram_channel_id FROM episodes LIMIT 5")).fetchall()
        series_names = [r[0] for r in series_ex] or ["لا يوجد"]
        movies_names = [r[0] for r in movies_ex] or ["لا يوجد"]
        ch_list = [r[0] for r in channels] or ["لا يوجد"]
        reply = (
            f"✅ <b>اختبار قاعدة البيانات:</b>\n\n"
            f"📊 <b>الإحصائيات:</b>\n"
            f"• عدد المسلسلات: {series_cnt}\n"
            f"• عدد الأفلام: {movies_cnt}\n"
            f"• عدد القنوات المختلفة: {len(channels)}\n\n"
            f"📺 <b>أمثلة على المسلسلات:</b>\n" + "\n".join("• " + n for n in series_names) + "\n\n"
            f"🎬 <b>أمثلة على الأفلام:</b>\n" + "\n".join("• " + n for n in movies_names) + "\n\n"
            f"📡 <b>القنوات المتاحة:</b>\n" + "\n".join("• " + c for c in ch_list)
        )
        keyboard = [[InlineKeyboardButton("📺 المسلسلات", callback_data="series_list"), InlineKeyboardButton("🎬 الأفلام", callback_data="movies_list")],
                    [InlineKeyboardButton("🏠 الرئيسية", callback_data="home")]]
        await query.edit_message_text(reply, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"خطأ في test_db_button: {e}")
        await query.edit_message_text(f"❌ خطأ في الاختبار: {str(e)[:200]}")

# ==============================
# 6. معالج الأزرار
# ==============================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    for attempt in range(3):
        try:
            await query.answer()
            break
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(1)
            else:
                logger.error(f"فشل answerCallbackQuery: {e}")
                return
    data = query.data
    logger.info(f"استقبال callback data: {data}")
    try:
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
        elif data == 'page_info':
            return
        elif data.startswith('content_page_'):
            parts = data.split('_')
            content_id = int(parts[2])
            page = int(parts[3])
            await show_content_details(update, context, content_id, page)
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
            if len(parts) == 3:
                content_id = int(parts[1])
                season_num = int(parts[2])
                await show_season_episodes(update, context, content_id, season_num, 1)
            else:
                logger.warning(f"تنسيق غير متوقع لـ season_: {data}")
        else:
            logger.warning(f"Callback data غير معروف: {data}")
    except Exception as e:
        logger.error(f"خطأ في button_handler: {e}", exc_info=True)
        await query.edit_message_text("⚠️ حدث خطأ أثناء معالجة طلبك. يرجى المحاولة مرة أخرى.")

# ==============================
# 7. الدالة الرئيسية
# ==============================
def main():
    try:
        app = Application.builder().token(BOT_TOKEN).build()
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("series", series_command))
        app.add_handler(CommandHandler("movies", movies_command))
        app.add_handler(CommandHandler("all", all_command))
        app.add_handler(CallbackQueryHandler(button_handler))
        print("🤖 البوت يعمل...")
        print(f"✅ قاعدة البيانات: {'موجودة' if engine else 'غير متصلة'}")
        app.run_polling(poll_interval=1.0, timeout=30, drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        print(f"❌ خطأ فادح: {e}")

if __name__ == "__main__":
    main()
