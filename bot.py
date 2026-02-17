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
    except Exception as e:
        print(f"❌ فشل الاتصال بقاعدة البيانات: {e}")
        engine = None

# ==============================
# 2. دوال المساعدة لجلب البيانات
# ==============================
async def get_all_content(content_type=None):
    """جلب جميع المحتويات مع ترتيبها بحيث الأحدث في الأسفل."""
    if not engine:
        return []
    try:
        with engine.connect() as conn:
            query = """
                SELECT s.id, s.name, s.type, 
                       COUNT(e.id) as episode_count,
                       COUNT(DISTINCT e.telegram_channel_id) as channel_count,
                       MAX(e.added_at) as last_added
                FROM series s
                LEFT JOIN episodes e ON s.id = e.series_id
            """
            if content_type:
                query += f" WHERE s.type = '{content_type}'"
            query += """
                GROUP BY s.id, s.name, s.type
                ORDER BY last_added ASC NULLS LAST
            """
            result = conn.execute(text(query))
            rows = result.fetchall()
            return rows
    except Exception as e:
        logger.error(f"خطأ في جلب المحتويات: {e}")
        return []

async def get_content_info(series_id):
    """جلب معلومات محتوى محدد."""
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
    """جلب حلقات موسم محدد مع دعم الصفحات."""
    if not engine:
        return [], 0, 0, page
    try:
        with engine.connect() as conn:
            # عدد الحلقات الكلي في الموسم
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

            # جلب الحلقات مع ترتيب تصاعدي حسب رقم الحلقة
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
    """جلب أجزاء الفيلم مرتبة حسب season (الجزء)."""
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
# 3. دوال البوت الرئيسية
# ==============================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """أمر /start."""
    try:
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
/test - اختبار قاعدة البيانات
/debug - فحص تفاصيل مسلسل/فيلم
/debug_movies - عرض قائمة الأفلام مع المعرفات
/find &lt;كلمة&gt; - البحث عن مسلسل/فيلم بالاسم
/debug_season &lt;id&gt; &lt;موسم&gt; - تشخيص حلقات موسم
        """

        if update.callback_query:
            await update.callback_query.edit_message_text(
                welcome_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                welcome_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
    except Exception as e:
        logger.error(f"خطأ في أمر start: {e}")

async def show_content(update: Update, context: ContextTypes.DEFAULT_TYPE, content_type=None):
    """عرض المحتويات حسب النوع مع ترتيب تصاعدي (الأحدث في الأسفل)."""
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
            content_id, name, ctype, ep_count, ch_count, last_added = content
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
                text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                text,
                parse_mode='HTML',
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
    await show_content(update, context, 'series')

async def movies_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_content(update, context, 'movie')

async def all_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_content(update, context)

async def test_db_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """أمر /test - اختبار قاعدة البيانات."""
    try:
        if not engine:
            await update.message.reply_text("❌ قاعدة البيانات غير متصلة.")
            return

        with engine.connect() as conn:
            tables = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")).fetchall()
            tables_info = "📋 <b>الجداول الموجودة:</b>\n"
            for table in tables:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {table[0]}")).scalar()
                tables_info += f"• <code>{table[0]}</code>: {count} صف\n"
            series_sample = conn.execute(text("SELECT id, name, type FROM series ORDER BY id LIMIT 5")).fetchall()
            episodes_sample = conn.execute(text("SELECT id, series_id, season, episode_number, telegram_channel_id FROM episodes ORDER BY id LIMIT 5")).fetchall()
            series_text = "🎬 <b>عينة من المسلسلات والأفلام:</b>\n"
            for row in series_sample:
                series_text += f"• ID:{row[0]} - {row[1]} ({row[2]})\n"
            episodes_text = "📺 <b>عينة من الحلقات:</b>\n"
            for row in episodes_sample:
                episodes_text += f"• ID:{row[0]} - مسلسل:{row[1]} - م{row[2]} ح{row[3]} - قناة:{row[4]}\n"
            reply_text = f"{tables_info}\n{series_text}\n{episodes_text}"

        await update.message.reply_text(reply_text, parse_mode='HTML')

    except Exception as e:
        await update.message.reply_text(f"❌ خطأ في اختبار قاعدة البيانات:\n<code>{str(e)[:300]}</code>")

# ==============================
# 4. عرض تفاصيل المحتوى (مواسم/أجزاء)
# ==============================
async def show_content_details(update: Update, context: ContextTypes.DEFAULT_TYPE, content_id, page=1):
    """عرض قائمة المواسم (للمسلسل) أو الأجزاء (للفيلم)."""
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
        else:  # movie
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

# ==============================
# 5. عرض حلقات موسم محدد مع دعم الصفحات
# ==============================
async def show_season_episodes(update: Update, context: ContextTypes.DEFAULT_TYPE, content_id, season_num, page=1):
    """عرض حلقات موسم محدد لمسلسل مع دعم الصفحات."""
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
        logger.info(f"تم عرض الموسم {season_num} للمسلسل {content_id}، الصفحة {page} من {total_pages}")
    except Exception as e:
        logger.error(f"خطأ في show_season_episodes: {e}")
        await query.edit_message_text("⚠️ حدث خطأ أثناء جلب البيانات. يرجى المحاولة مرة أخرى.")

# ==============================
# 6. عرض تفاصيل حلقة/جزء
# ==============================
async def show_episode_details(update: Update, context: ContextTypes.DEFAULT_TYPE, episode_id):
    """عرض تفاصيل حلقة/جزء مع رابط المشاهدة."""
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
# 7. أوامر التصحيح (debug)
# ==============================
async def debug_series(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """عرض تفاصيل مسلسل/فيلم محدد (للتشخيص)."""
    try:
        if not context.args:
            await update.message.reply_text("استخدم: /debug <معرف المسلسل>")
            return
        series_id = int(context.args[0])

        with engine.connect() as conn:
            series = conn.execute(text("SELECT id, name, type FROM series WHERE id = :id"), {"id": series_id}).fetchone()
            if not series:
                await update.message.reply_text("المسلسل غير موجود")
                return

            episodes = conn.execute(text("""
                SELECT season, COUNT(*) as count, MIN(episode_number) as min_ep, MAX(episode_number) as max_ep
                FROM episodes WHERE series_id = :sid GROUP BY season ORDER BY season
            """), {"sid": series_id}).fetchall()

            text = f"<b>{series[1]}</b> (ID: {series[0]}, نوع: {series[2]})\n"
            for ep in episodes:
                text += f"• الموسم {ep[0]}: {ep[1]} حلقة (من {ep[2]} إلى {ep[3]})\n"

            total = conn.execute(text("SELECT COUNT(*) FROM episodes WHERE series_id = :sid"), {"sid": series_id}).scalar()
            text += f"\nإجمالي الحلقات: {total}"

            await update.message.reply_text(text, parse_mode='HTML')
    except Exception as e:
        await update.message.reply_text(f"خطأ: {e}")

async def debug_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """عرض قائمة الأفلام مع معرفاتها."""
    try:
        with engine.connect() as conn:
            movies = conn.execute(text("SELECT id, name FROM series WHERE type = 'movie' ORDER BY name")).fetchall()
            if not movies:
                await update.message.reply_text("لا توجد أفلام")
                return

            text = "🎬 <b>قائمة الأفلام (مع المعرفات):</b>\n"
            for m in movies:
                text += f"• {m[1]} – معرف <code>{m[0]}</code>\n"
            await update.message.reply_text(text, parse_mode='HTML')
    except Exception as e:
        await update.message.reply_text(f"خطأ: {e}")

async def find_series(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """البحث عن مسلسلات أو أفلام بالاسم."""
    if not context.args:
        await update.message.reply_text("استخدم: /find <كلمة>")
        return
    search_term = ' '.join(context.args)
    try:
        with engine.connect() as conn:
            results = conn.execute(
                text("""
                    SELECT s.id, s.name, s.type, s.normalized_name, 
                           COUNT(e.id) as episode_count
                    FROM series s
                    LEFT JOIN episodes e ON s.id = e.series_id
                    WHERE s.name ILIKE :pattern OR s.normalized_name ILIKE :pattern
                    GROUP BY s.id, s.name, s.type, s.normalized_name
                """),
                {"pattern": f"%{search_term}%"}
            ).fetchall()
            if not results:
                await update.message.reply_text(f"لا توجد نتائج لـ '{search_term}'")
                return
            response = f"🔍 نتائج البحث عن '{search_term}':\n\n"
            for r in results:
                response += f"• {r[1]} (ID: {r[0]}, نوع: {r[2]}, مقيس: {r[3]}, عدد الحلقات: {r[4]})\n"
            await update.message.reply_text(response)
    except Exception as e:
        await update.message.reply_text(f"خطأ: {e}")

async def debug_season(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تشخيص عدد حلقات موسم معين."""
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("استخدم: /debug_season <series_id> <season>")
        return
    try:
        series_id = int(context.args[0])
        season = int(context.args[1])
        with engine.connect() as conn:
            # عدد الحلقات في الموسم
            count = conn.execute(
                text("SELECT COUNT(*) FROM episodes WHERE series_id = :sid AND season = :season"),
                {"sid": series_id, "season": season}
            ).scalar()
            # عينة من الحلقات
            episodes = conn.execute(
                text("SELECT episode_number, telegram_message_id, telegram_channel_id, added_at FROM episodes WHERE series_id = :sid AND season = :season ORDER BY episode_number"),
                {"sid": series_id, "season": season}
            ).fetchall()
            if count == 0:
                await update.message.reply_text(f"لا توجد حلقات للمسلسل {series_id} في الموسم {season}")
                return
            # أرقام الحلقات
            ep_numbers = [ep[0] for ep in episodes]
            min_ep = min(ep_numbers)
            max_ep = max(ep_numbers)
            msg = f"🔍 <b>المسلسل ID {series_id} - الموسم {season}</b>\n"
            msg += f"إجمالي الحلقات: {count}\n"
            msg += f"أصغر رقم حلقة: {min_ep}\n"
            msg += f"أكبر رقم حلقة: {max_ep}\n"
            msg += f"أول 20 رقم: {', '.join(map(str, ep_numbers[:20]))}"
            if len(ep_numbers) > 20:
                msg += f"... (و{len(ep_numbers)-20} أخرى)"
            await update.message.reply_text(msg, parse_mode='HTML')
    except Exception as e:
        await update.message.reply_text(f"خطأ: {e}")
# ==============================
# 8. اختبار قاعدة البيانات من الزر
# ==============================
async def test_db_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """اختبار قاعدة البيانات من الزر."""
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
# 9. معالج الأزرار التفاعلية
# ==============================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة جميع أزرار InlineKeyboard."""
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
#
# ==============================
async def debug_all_episodes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """عرض جميع حلقات مسلسل معين (للتشخيص)."""
    if not context.args:
        await update.message.reply_text("استخدم: /debug_all_episodes <series_id>")
        return
    try:
        series_id = int(context.args[0])
        with engine.connect() as conn:
            # جلب جميع الحلقات مرتبة حسب الموسم ورقم الحلقة
            episodes = conn.execute(
                text("""
                    SELECT season, episode_number
                    FROM episodes
                    WHERE series_id = :sid
                    ORDER BY season, episode_number
                """),
                {"sid": series_id}
            ).fetchall()
            if not episodes:
                await update.message.reply_text("لا توجد حلقات لهذا المسلسل.")
                return
            # تجميع النتائج
            result = {}
            for season, ep in episodes:
                if season not in result:
                    result[season] = []
                result[season].append(ep)
            text = f"📊 جميع حلقات المسلسل {series_id}:\n\n"
            for season in sorted(result.keys()):
                eps = result[season]
                text += f"الموسم {season}: {len(eps)} حلقة (من {min(eps)} إلى {max(eps)})\n"
                # عرض أول 20 رقم للموسم
                text += f"  الأرقام: {', '.join(map(str, eps[:20]))}"
                if len(eps) > 20:
                    text += f"... (و{len(eps)-20} أخرى)"
                text += "\n\n"
            await update.message.reply_text(text, parse_mode='HTML')
    except Exception as e:
        await update.message.reply_text(f"خطأ: {e}")
# ==============================
# 10. الدالة الرئيسية
# ==============================
def main():
    try:
        app = Application.builder().token(BOT_TOKEN).build()
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("series", series_command))
        app.add_handler(CommandHandler("movies", movies_command))
        app.add_handler(CommandHandler("all", all_command))
        app.add_handler(CommandHandler("test", test_db_command))
        app.add_handler(CommandHandler("debug", debug_series))
        app.add_handler(CommandHandler("debug_movies", debug_movies))
        app.add_handler(CommandHandler("find", find_series))
        app.add_handler(CommandHandler("debug_season", debug_season))
        app.add_handler(CommandHandler("debug_all_episodes", debug_all_episodes))
        app.add_handler(CallbackQueryHandler(button_handler))

        print("🤖 البوت يعمل...")
        print(f"✅ قاعدة البيانات: {'موجودة' if engine else 'غير متصلة'}")
        app.run_polling(poll_interval=1.0, timeout=30, drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        print(f"❌ خطأ فادح: {e}")
        # لا نعيد الاستدعاء لتجنب حلقة لا نهائية

if __name__ == "__main__":
    main()
