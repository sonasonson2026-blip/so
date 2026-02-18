# ==============================
# bot.py (نسخة نهائية مع دمج ذكي)
# ==============================
import os
import logging
import re
import unicodedata
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
# دوال تطبيع النص (مطابقة لما في worker)
# ------------------------------
def normalize_arabic(text):
    if not text:
        return ''
    text = unicodedata.normalize('NFKD', text)
    text = re.sub(r'[\u064B-\u065F]', '', text)  # إزالة التشكيل
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

# ------------------------------
# دوال مساعدة للبحث والدمج
# ------------------------------
async def get_all_series_by_keywords(name, content_type=None):
    """البحث عن جميع المسلسلات التي تبدأ بنفس الكلمات المفتاحية"""
    if not engine:
        return []
    try:
        # استخراج أول 3 كلمات من الاسم (بدون أرقام)
        words = re.sub(r'\d+', '', name).split()[:3]
        if not words:
            return []
        # بناء pattern للبحث: %كلمة1%كلمة2%كلمة3%
        pattern = '%' + '%'.join(words) + '%'
        
        with engine.connect() as conn:
            query = "SELECT id, name, type FROM series WHERE name ILIKE :pat"
            params = {"pat": pattern}
            if content_type:
                query += " AND type = :typ"
                params["typ"] = content_type
            result = conn.execute(text(query), params).fetchall()
            return result
    except Exception as e:
        logger.error(f"خطأ في البحث عن مسلسلات مشابهة: {e}")
        return []

async def get_all_episodes_for_series(series_ids):
    """جلب جميع الحلقات من عدة مسلسلات، مرتبة حسب الموسم ورقم الحلقة"""
    if not engine or not series_ids:
        return []
    try:
        with engine.connect() as conn:
            # تحويل list إلى tuple للاستعلام
            ids_tuple = tuple(series_ids)
            result = conn.execute(
                text("""
                    SELECT id, series_id, season, episode_number, telegram_message_id, telegram_channel_id
                    FROM episodes
                    WHERE series_id IN :ids
                    ORDER BY season ASC, episode_number ASC
                """),
                {"ids": ids_tuple}
            ).fetchall()
            return result
    except Exception as e:
        logger.error(f"خطأ في جلب الحلقات: {e}")
        return []

# ------------------------------
# دوال العرض (معدلة)
# ------------------------------
async def get_all_content_paginated(content_type=None, page=1, per_page=10):
    """جلب المحتويات مع دعم الصفحات"""
    if not engine:
        return [], 0, 0, page
    try:
        with engine.connect() as conn:
            # حساب العدد الإجمالي
            count_query = "SELECT COUNT(DISTINCT id) FROM series"
            if content_type:
                count_query += f" WHERE type = '{content_type}'"
            total = conn.execute(text(count_query)).scalar() or 0
            total_pages = (total + per_page - 1) // per_page if total else 0
            if page < 1:
                page = 1
            elif page > total_pages:
                page = total_pages
            offset = (page - 1) * per_page

            query = f"""
                SELECT id, name, type
                FROM series
            """
            if content_type:
                query += f" WHERE type = '{content_type}'"
            query += """
                ORDER BY id DESC
                LIMIT :limit OFFSET :offset
            """
            result = conn.execute(text(query), {"limit": per_page, "offset": offset})
            items = result.fetchall()
            
            # إحضار عدد الحلقات لكل مسلسل (للعرض فقط)
            items_with_count = []
            for sid, name, typ in items:
                cnt = conn.execute(
                    text("SELECT COUNT(*) FROM episodes WHERE series_id = :sid"),
                    {"sid": sid}
                ).scalar() or 0
                items_with_count.append((sid, name, typ, cnt))
            return items_with_count, total, total_pages, page
    except Exception as e:
        logger.error(f"خطأ في جلب المحتويات: {e}")
        return [], 0, 0, page

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📺 المسلسلات", callback_data='series_list_1'),
         InlineKeyboardButton("🎬 الأفلام", callback_data='movies_list_1')],
        [InlineKeyboardButton("📁 جميع المحتويات", callback_data='all_content_1')],
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

async def show_content(update: Update, context: ContextTypes.DEFAULT_TYPE, content_type=None, page=1):
    if not engine:
        msg = "❌ قاعدة البيانات غير متاحة"
        if update.callback_query:
            await update.callback_query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return

    items, total, total_pages, current_page = await get_all_content_paginated(content_type, page)
    if content_type == 'series':
        title = "📺 المسلسلات"
        empty = "📭 لا توجد مسلسلات"
        callback_prefix = 'series_list'
    elif content_type == 'movie':
        title = "🎬 الأفلام"
        empty = "📭 لا توجد أفلام"
        callback_prefix = 'movies_list'
    else:
        title = "📁 جميع المحتويات"
        empty = "📭 لا توجد محتويات"
        callback_prefix = 'all_content'

    if not items:
        await (update.callback_query or update.message).reply_text(f"{empty}\n\nℹ️ استخدم زر الفحص للتحقق")
        return

    text = f"<b>{title}</b> (الصفحة {current_page}/{total_pages})\n\n"
    keyboard = []
    for sid, name, typ, ep_count in items:
        info = f"{ep_count} حلقة" if typ == 'series' else f"{ep_count} جزء"
        text += f"• {name} ({info})\n"
        keyboard.append([InlineKeyboardButton(f"{name[:20]} ({ep_count})", callback_data=f"content_{sid}")])

    # أزرار التنقل
    nav = []
    if current_page > 1:
        nav.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"{callback_prefix}_{current_page-1}"))
    nav.append(InlineKeyboardButton(f"📄 {current_page}/{total_pages}", callback_data="page_info"))
    if current_page < total_pages:
        nav.append(InlineKeyboardButton("التالي ➡️", callback_data=f"{callback_prefix}_{current_page+1}"))
    if nav:
        keyboard.append(nav)

    keyboard.append([
        InlineKeyboardButton("📺 المسلسلات", callback_data='series_list_1'),
        InlineKeyboardButton("🎬 الأفلام", callback_data='movies_list_1')
    ])
    keyboard.append([InlineKeyboardButton("🏠 الرئيسية", callback_data='home')])

    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def series_command(update, context):
    await show_content(update, context, 'series', 1)

async def movies_command(update, context):
    await show_content(update, context, 'movie', 1)

async def all_command(update, context):
    await show_content(update, context, None, 1)

async def show_content_details(update: Update, context: ContextTypes.DEFAULT_TYPE, content_id):
    query = update.callback_query
    # الحصول على معلومات المسلسل المختار
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id, name, type FROM series WHERE id = :sid"),
            {"sid": content_id}
        ).fetchone()
    if not row:
        await query.edit_message_text("❌ المحتوى غير موجود")
        return
    sid, name, typ = row

    # البحث عن جميع المسلسلات المشابهة (باستخدام الكلمات المفتاحية)
    similar_series = await get_all_series_by_keywords(name, typ)
    all_ids = [s[0] for s in similar_series]
    if not all_ids:
        all_ids = [sid]

    # حفظ القائمة في context
    context.user_data['current_series_ids'] = all_ids
    context.user_data['current_name'] = name
    context.user_data['current_type'] = typ

    # جلب جميع الحلقات من هذه المسلسلات
    all_episodes = await get_all_episodes_for_series(all_ids)
    if not all_episodes:
        await query.edit_message_text(f"📭 لا توجد حلقات لهذا المحتوى")
        return

    # تجميع المواسم وعدد الحلقات
    seasons = {}
    for ep in all_episodes:
        s = ep[2]  # season
        seasons[s] = seasons.get(s, 0) + 1

    # ترتيب المواسم
    seasons = sorted(seasons.items())
    context.user_data['all_episodes'] = all_episodes  # نخزن كل الحلقات للاستخدام لاحقاً

    # عرض المواسم
    msg = f"<b>{name}</b>\n\n"
    if typ == 'series':
        if len(seasons) > 1:
            msg += "اختر الموسم:"
            keyboard = []
            for s, count in seasons:
                keyboard.append([InlineKeyboardButton(f"الموسم {s} ({count} حلقة)", callback_data=f"season_{s}_1")])
        else:
            # موسم واحد فقط، نعرض الحلقات مباشرة
            season = seasons[0][0]
            await show_season_episodes(update, context, season, 1)
            return
    else:  # فيلم
        if len(seasons) > 1:
            msg += "اختر الجزء:"
            keyboard = []
            for s, count in seasons:
                # نأخذ أول حلقة في هذا الجزء
                ep = next((e for e in all_episodes if e[2] == s), None)
                if ep:
                    keyboard.append([InlineKeyboardButton(f"الجزء {s}", callback_data=f"ep_{ep[0]}")])
        else:
            s = seasons[0][0]
            ep = next((e for e in all_episodes if e[2] == s), None)
            if ep:
                msg += "اضغط لمشاهدة الفيلم:"
                keyboard = [[InlineKeyboardButton("مشاهدة", callback_data=f"ep_{ep[0]}")]]
            else:
                msg += "لا يوجد رابط"
                keyboard = []

    keyboard.append([InlineKeyboardButton("⬅️ رجوع", callback_data=f"{'series' if typ=='series' else 'movies'}_list_1"), 
                     InlineKeyboardButton("🏠 الرئيسية", callback_data="home")])
    await query.edit_message_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

async def show_season_episodes(update: Update, context: ContextTypes.DEFAULT_TYPE, season, page=1):
    query = update.callback_query
    all_episodes = context.user_data.get('all_episodes', [])
    name = context.user_data.get('current_name', '')
    if not all_episodes:
        await query.edit_message_text("❌ جلسة منتهية، الرجاء العودة للقائمة الرئيسية")
        return

    # فلترة الحلقات حسب الموسم
    season_episodes = [ep for ep in all_episodes if ep[2] == season]
    total = len(season_episodes)
    per_page = 50
    total_pages = (total + per_page - 1) // per_page if total else 0
    if page < 1:
        page = 1
    elif page > total_pages:
        page = total_pages
    offset = (page - 1) * per_page
    episodes_page = season_episodes[offset:offset+per_page]

    if not episodes_page:
        await query.edit_message_text(f"❌ لا توجد حلقات للموسم {season}")
        return

    msg = f"<b>{name}</b>\nالموسم {season}\nعدد الحلقات: {total}\n"
    if total_pages > 1:
        msg += f"الصفحة {page} من {total_pages}\n\n"
    msg += "اختر الحلقة:"

    keyboard = []
    row = []
    for ep in episodes_page:
        eid, _, _, num, _, _ = ep
        row.append(InlineKeyboardButton(f"ح{num}", callback_data=f"ep_{eid}"))
        if len(row) == 5:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    if total_pages > 1:
        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"season_page_{season}_{page-1}"))
        nav.append(InlineKeyboardButton(f"📄 {page}/{total_pages}", callback_data="page_info"))
        if page < total_pages:
            nav.append(InlineKeyboardButton("➡️", callback_data=f"season_page_{season}_{page+1}"))
        keyboard.append(nav)

    keyboard.append([InlineKeyboardButton("⬅️ رجوع للمسلسل", callback_data=f"content_{context.user_data.get('current_series_ids', [0])[0]}"), 
                     InlineKeyboardButton("🏠 الرئيسية", callback_data="home")])
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
        keyboard.append([InlineKeyboardButton("⬅️ رجوع", callback_data=f"content_{sid}"), 
                         InlineKeyboardButton("🏠 الرئيسية", callback_data="home")])
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
        keyboard = [[InlineKeyboardButton("📺 المسلسلات", callback_data="series_list_1"), 
                     InlineKeyboardButton("🎬 الأفلام", callback_data="movies_list_1")],
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
    elif data.startswith('series_list_'):
        page = int(data.split('_')[2])
        await show_content(update, context, 'series', page)
    elif data.startswith('movies_list_'):
        page = int(data.split('_')[2])
        await show_content(update, context, 'movie', page)
    elif data.startswith('all_content_'):
        page = int(data.split('_')[2])
        await show_content(update, context, None, page)
    elif data.startswith('content_'):
        sid = int(data.split('_')[1])
        await show_content_details(update, context, sid)
    elif data.startswith('ep_'):
        eid = int(data.split('_')[1])
        await show_episode_details(update, context, eid)
    elif data.startswith('season_page_'):
        parts = data.split('_')
        season = int(parts[2])
        page = int(parts[3])
        await show_season_episodes(update, context, season, page)
    elif data.startswith('season_'):
        parts = data.split('_')
        season = int(parts[1])
        page = int(parts[2]) if len(parts) > 2 else 1
        await show_season_episodes(update, context, season, page)
    elif data == 'page_info':
        await query.answer("استخدم أزرار التنقل", show_alert=False)

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
