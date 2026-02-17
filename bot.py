# ==============================
# bot.py (الكود الكامل)
# ==============================
import os
import logging
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes
)
from sqlalchemy import create_engine, text

# ------------------------------
# الإعدادات
# ------------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

if not BOT_TOKEN:
    print("❌ BOT_TOKEN غير موجود")
    exit(1)
if not DATABASE_URL:
    print("⚠️ DATABASE_URL غير موجود")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

engine = None
if DATABASE_URL:
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ اتصال بقاعدة البيانات ناجح")
    except Exception as e:
        print(f"❌ فشل الاتصال: {e}")
        engine = None

# ------------------------------
# دوال مساعدة
# ------------------------------
async def get_all_content(content_type=None):
    """جلب المحتويات مرتبة حسب آخر رسالة (الأحدث في الأسفل)"""
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
                ORDER BY last_msg_id DESC NULLS LAST
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
            result = conn.execute(text("SELECT id, name, type FROM series WHERE id = :sid"), {"sid": series_id})
            return result.fetchone()
    except Exception as e:
        logger.error(f"خطأ في جلب معلومات المحتوى {series_id}: {e}")
        return None

async def get_season_episodes(series_id, season, page=1, per_page=50):
    if not engine:
        return [], 0, 0, page
    try:
        with engine.connect() as conn:
            total = conn.execute(
                text("SELECT COUNT(*) FROM episodes WHERE series_id = :sid AND season = :season"),
                {"sid": series_id, "season": season}
            ).scalar()
            total_pages = (total + per_page - 1) // per_page if total else 0
            if page < 1:
                page = 1
            elif page > total_pages:
                page = total_pages
            offset = (page - 1) * per_page
            episodes = conn.execute(
                text("""
                    SELECT id, season, episode_number, telegram_message_id, telegram_channel_id
                    FROM episodes
                    WHERE series_id = :sid AND season = :season
                    ORDER BY episode_number DESC
                    LIMIT :limit OFFSET :offset
                """),
                {"sid": series_id, "season": season, "limit": per_page, "offset": offset}
            ).fetchall()
            return episodes, total, total_pages, page
    except Exception as e:
        logger.error(f"خطأ في get_season_episodes: {e}")
        return [], 0, 0, page

async def get_movie_parts(series_id):
    if not engine:
        return []
    try:
        with engine.connect() as conn:
            return conn.execute(
                text("SELECT season, COUNT(*) FROM episodes WHERE series_id = :sid GROUP BY season ORDER BY season"),
                {"sid": series_id}
            ).fetchall()
    except Exception as e:
        logger.error(f"خطأ في get_movie_parts: {e}")
        return []

# ------------------------------
# أوامر البوت
# ------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📺 المسلسلات", callback_data='series_list'),
         InlineKeyboardButton("🎬 الأفلام", callback_data='movies_list')],
        [InlineKeyboardButton("📁 جميع المحتويات", callback_data='all_content')],
        [InlineKeyboardButton("🔄 فحص قاعدة البيانات", callback_data='test_db')],
    ]
    text = """
🎬 <b>مرحباً في بوت مسلسلاتي وأفلامي</b>

<b>الأوامر المتاحة:</b>
/start - عرض هذه الرسالة
/series - عرض المسلسلات
/movies - عرض الأفلام
/all - عرض كل المحتويات
    """
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def show_content(update: Update, context: ContextTypes.DEFAULT_TYPE, content_type=None):
    if not engine:
        msg = "❌ قاعدة البيانات غير متاحة"
        if update.callback_query:
            await update.callback_query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return

    items = await get_all_content(content_type)
    if content_type == 'series':
        title = "📺 المسلسلات"
        empty = "📭 لا توجد مسلسلات"
    elif content_type == 'movie':
        title = "🎬 الأفلام"
        empty = "📭 لا توجد أفلام"
    else:
        title = "📁 جميع المحتويات"
        empty = "📭 لا توجد محتويات"

    if not items:
        await (update.callback_query or update.message).reply_text(f"{empty}\n\nℹ️ استخدم زر الفحص للتحقق")
        return

    text = f"<b>{title}</b>\n\n"
    keyboard = []
    for row in items:
        sid, name, typ, ep_count, ch_count, _ = row
        info = f"{ep_count} حلقة" if typ == 'series' else f"{ep_count} جزء"
        text += f"• {name} ({info})\n"
        keyboard.append([InlineKeyboardButton(f"{name[:20]} ({ep_count})", callback_data=f"content_{sid}")])

    keyboard.append([
        InlineKeyboardButton("📺 المسلسلات", callback_data='series_list'),
        InlineKeyboardButton("🎬 الأفلام", callback_data='movies_list')
    ])
    keyboard.append([InlineKeyboardButton("🏠 الرئيسية", callback_data='home')])

    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def series_command(update, context): await show_content(update, context, 'series')
async def movies_command(update, context): await show_content(update, context, 'movie')
async def all_command(update, context): await show_content(update, context)

async def show_content_details(update: Update, context: ContextTypes.DEFAULT_TYPE, content_id, page=1):
    query = update.callback_query
    info = await get_content_info(content_id)
    if not info:
        await query.edit_message_text("❌ المحتوى غير موجود")
        return
    sid, name, typ = info

    # جلب القنوات
    with engine.connect() as conn:
        channels = conn.execute(
            text("SELECT DISTINCT telegram_channel_id FROM episodes WHERE series_id = :sid"),
            {"sid": sid}
        ).fetchall()
    chan_text = ", ".join([c[0] for c in channels]) if channels else "غير معروف"

    msg = f"<b>{name}</b>\n<b>القنوات:</b> {chan_text}\n\n"

    if typ == 'series':
        with engine.connect() as conn:
            seasons = conn.execute(
                text("SELECT season, COUNT(*) FROM episodes WHERE series_id = :sid GROUP BY season ORDER BY season"),
                {"sid": sid}
            ).fetchall()
        if not seasons:
            msg += "📭 لا توجد حلقات"
            keyboard = [[InlineKeyboardButton("⬅️ رجوع", callback_data="series_list")]]
            await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        if len(seasons) > 1:
            msg += "اختر الموسم:"
            keyboard = [[InlineKeyboardButton(f"الموسم {s} ({c} حلقة)", callback_data=f"season_{sid}_{s}")] for s, c in seasons]
        else:
            season = seasons[0][0]
            await show_season_episodes(update, context, sid, season, page)
            return
    else:  # فيلم
        parts = await get_movie_parts(sid)
        if not parts:
            msg += "📭 لا توجد أجزاء"
            keyboard = [[InlineKeyboardButton("⬅️ رجوع", callback_data="movies_list")]]
            await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
            return
        if len(parts) > 1:
            msg += "اختر الجزء:"
            keyboard = []
            for p, _ in parts:
                with engine.connect() as conn:
                    ep_id = conn.execute(
                        text("SELECT id FROM episodes WHERE series_id = :sid AND season = :p LIMIT 1"),
                        {"sid": sid, "p": p}
                    ).scalar()
                keyboard.append([InlineKeyboardButton(f"الجزء {p}", callback_data=f"ep_{ep_id}")])
        else:
            p = parts[0][0]
            with engine.connect() as conn:
                ep_id = conn.execute(
                    text("SELECT id FROM episodes WHERE series_id = :sid AND season = :p LIMIT 1"),
                    {"sid": sid, "p": p}
                ).scalar()
            msg += "اضغط لمشاهدة الفيلم:"
            keyboard = [[InlineKeyboardButton("مشاهدة", callback_data=f"ep_{ep_id}")]]

    keyboard.append([InlineKeyboardButton("⬅️ رجوع", callback_data=f"{typ}_list"), InlineKeyboardButton("🏠 الرئيسية", callback_data="home")])
    await query.edit_message_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def show_season_episodes(update: Update, context: ContextTypes.DEFAULT_TYPE, sid, season, page=1):
    query = update.callback_query
    info = await get_content_info(sid)
    if not info:
        await query.edit_message_text("❌ المحتوى غير موجود")
        return
    name = info[1]

    episodes, total, total_pages, current_page = await get_season_episodes(sid, season, page)
    if not episodes:
        await query.edit_message_text(f"❌ لا توجد حلقات للموسم {season}")
        return

    msg = f"<b>{name}</b>\nالموسم {season}\nعدد الحلقات: {total}\n"
    if total_pages > 1:
        msg += f"الصفحة {current_page} من {total_pages}\n\n"
    msg += "اختر الحلقة:"

    keyboard = []
    row = []
    for ep in episodes:
        eid, _, num, _, _ = ep
        row.append(InlineKeyboardButton(f"ح{num}", callback_data=f"ep_{eid}"))
        if len(row) == 5:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    if total_pages > 1:
        nav = []
        if current_page > 1:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"season_page_{sid}_{season}_{current_page-1}"))
        nav.append(InlineKeyboardButton(f"📄 {current_page}/{total_pages}", callback_data="page_info"))
        if current_page < total_pages:
            nav.append(InlineKeyboardButton("➡️", callback_data=f"season_page_{sid}_{season}_{current_page+1}"))
        keyboard.append(nav)

    keyboard.append([InlineKeyboardButton("⬅️ رجوع للمسلسل", callback_data=f"content_{sid}"), InlineKeyboardButton("🏠 الرئيسية", callback_data="home")])
    await query.edit_message_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def show_episode_details(update: Update, context: ContextTypes.DEFAULT_TYPE, episode_id):
    query = update.callback_query
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT e.season, e.episode_number, e.telegram_message_id, e.telegram_channel_id,
                           s.name, s.type, s.id
                    FROM episodes e
                    JOIN series s ON e.series_id = s.id
                    WHERE e.id = :eid
                """),
                {"eid": episode_id}
            ).fetchone()
        if not row:
            await query.edit_message_text("❌ غير موجود")
            return
        season, ep_num, msg_id, channel, name, typ, sid = row

        if msg_id and channel:
            if channel.startswith('@'):
                link = f"https://t.me/{channel[1:]}/{msg_id}"
            else:
                link = f"https://t.me/c/{channel}/{msg_id}"
            title = f"<b>{name}</b>\n{'الموسم ' + str(season) if typ=='series' else 'الجزء ' + str(season)} - الحلقة {ep_num}" if typ=='series' else f"<b>{name}</b>\nالجزء {season}"
            btn_text = "مشاهدة الحلقة" if typ=='series' else "مشاهدة الفيلم"
        else:
            link = None
            title = f"<b>{name}</b>"
            btn_text = "رابط غير متوفر"

        msg = f"{title}\n\nالقناة: {channel}\nتأكد من الانضمام للقناة."
        keyboard = []
        if link:
            keyboard.append([InlineKeyboardButton(btn_text, url=link)])
        keyboard.append([InlineKeyboardButton("⬅️ رجوع", callback_data=f"content_{sid}"), InlineKeyboardButton("🏠 الرئيسية", callback_data="home")])
        await query.edit_message_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"خطأ في show_episode_details: {e}")
        await query.edit_message_text("⚠️ حدث خطأ")

async def test_db_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not engine:
        await query.edit_message_text("❌ قاعدة البيانات غير متصلة")
        return
    try:
        with engine.connect() as conn:
            series_cnt = conn.execute(text("SELECT COUNT(*) FROM series WHERE type='series'")).scalar()
            movies_cnt = conn.execute(text("SELECT COUNT(*) FROM series WHERE type='movie'")).scalar()
            series_ex = conn.execute(text("SELECT name FROM series WHERE type='series' LIMIT 3")).fetchall()
            movies_ex = conn.execute(text("SELECT name FROM series WHERE type='movie' LIMIT 3")).fetchall()
            channels = conn.execute(text("SELECT DISTINCT telegram_channel_id FROM episodes LIMIT 5")).fetchall()
        reply = (
            f"✅ <b>إحصائيات قاعدة البيانات</b>\n\n"
            f"مسلسلات: {series_cnt}\nأفلام: {movies_cnt}\n"
            f"قنوات: {len(channels)}\n\n"
            f"نماذج مسلسلات: {', '.join([r[0] for r in series_ex])}\n"
            f"نماذج أفلام: {', '.join([r[0] for r in movies_ex])}"
        )
        keyboard = [[InlineKeyboardButton("📺 المسلسلات", callback_data="series_list"), InlineKeyboardButton("🎬 الأفلام", callback_data="movies_list")],
                    [InlineKeyboardButton("🏠 الرئيسية", callback_data="home")]]
        await query.edit_message_text(reply, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        await query.edit_message_text(f"❌ خطأ: {e}")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    logger.info(f"callback: {data}")

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
    elif data.startswith('content_'):
        sid = int(data.split('_')[1])
        await show_content_details(update, context, sid)
    elif data.startswith('ep_'):
        eid = int(data.split('_')[1])
        await show_episode_details(update, context, eid)
    elif data.startswith('season_page_'):
        parts = data.split('_')
        sid, season, page = int(parts[2]), int(parts[3]), int(parts[4])
        await show_season_episodes(update, context, sid, season, page)
    elif data.startswith('season_'):
        parts = data.split('_')
        sid, season = int(parts[1]), int(parts[2])
        await show_season_episodes(update, context, sid, season, 1)

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("series", series_command))
    app.add_handler(CommandHandler("movies", movies_command))
    app.add_handler(CommandHandler("all", all_command))
    app.add_handler(CallbackQueryHandler(button_handler))
    print("🤖 البوت يعمل...")
    app.run_polling()

if __name__ == "__main__":
    main()
