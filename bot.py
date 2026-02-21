import asyncio
import logging
import datetime
import os
import shutil
import random
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile

from config import BOT_TOKEN, ADMIN_IDS, TYUMEN_DISTRICTS, DEBUG
from database import Database
import keyboards as kb
from states import States

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Инициализация
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
db = Database()

# Глобальные переменные
waiting_users = []
active_chats = {}
chat_messages = {}
user_last_message = {}
search_mode = {}
active_chat_ids = {}
broadcast_data = {}

bot_stats = {
    "total_messages": 0,
    "total_chats": 0,
    "active_chats": 0,
    "online_users": 0,
    "start_time": datetime.datetime.now(),
}

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def generate_nickname():
    adj = ["Сибирский", "Тюменский", "Набережный", "Солнечный", "Гилевский",
           "Тарманский", "Калининский", "Центральный", "Нефтяной", "Вечерний"]
    nouns = ["Волк", "Лис", "Медведь", "Соболь", "Кедр", "Тура", "Мост",
             "Фонтан", "Парк", "Студент", "Нефтяник", "Сибиряк"]
    return f"{random.choice(adj)} {random.choice(nouns)}"

def get_rating_level(rating):
    if rating >= 90: return "🌟 Легенда"
    if rating >= 70: return "⭐ Почётный"
    if rating >= 50: return "👍 Активный"
    if rating >= 30: return "👌 Местный"
    if rating >= 10: return "🤔 Гость"
    return "👎 Нарушитель"

async def force_cleanup_user(user_id, db):
    if user_id in waiting_users:
        waiting_users.remove(user_id)
        db.update_online_status(user_id, False)
    
    if user_id in active_chats:
        pid = active_chats[user_id]
        if pid in active_chats:
            if pid in active_chat_ids:
                db.end_chat(active_chat_ids[pid])
                del active_chat_ids[pid]
            db.update_online_status(pid, False)
            del active_chats[pid]
        if user_id in active_chat_ids:
            db.end_chat(active_chat_ids[user_id])
            del active_chat_ids[user_id]
        del active_chats[user_id]
    
    if user_id in search_mode:
        del search_mode[user_id]

async def update_online_stats(db):
    online_users = set(active_chats.keys()) | set(waiting_users)
    online_by_district = {}
    
    for uid in online_users:
        user = db.get_user(uid)
        if user and not db.check_banned(uid):
            district = user['district']
            online_by_district[district] = online_by_district.get(district, 0) + 1
    
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE district_stats SET online_now = 0')
    for district, count in online_by_district.items():
        cursor.execute('UPDATE district_stats SET online_now = ? WHERE district = ?', (count, district))
    conn.commit()
    conn.close()
    
    bot_stats["online_users"] = len(online_users)
    return online_users, online_by_district

async def show_main_menu(message, user_id):
    user = db.get_user(user_id)
    if not user:
        return
    
    anon = "🕵️ Вкл" if user['anon_mode'] else "👁️ Выкл"
    rating = user['rating'] or 50.0
    rating_level = get_rating_level(rating)
    
    stats = db.get_district_stats()
    online = 0
    for s in stats:
        if s['district'] == user['district']:
            online = s['online_now']
            break
    
    text = (
        f"👋 <b>ТюменьChat</b>\n\n"
        f"👤 {user['nickname']}\n"
        f"🏘️ {user['district']}\n"
        f"{anon} | Рейтинг: {rating:.1f}% ({rating_level})\n"
        f"📍 В районе онлайн: {online}"
    )
    
    try:
        await message.edit_text(text, reply_markup=kb.main_menu())
    except:
        await message.answer(text, reply_markup=kb.main_menu())

async def create_chat(user1_id, user2_id, db, bot):
    user1 = db.get_user(user1_id)
    user2 = db.get_user(user2_id)
    
    if not user1 or not user2:
        return False
    
    chat_uuid = f"{min(user1_id, user2_id)}_{max(user1_id, user2_id)}_{datetime.datetime.now().timestamp()}"
    chat_district = user1['district'] if user1['district'] == user2['district'] else 'разные районы'
    
    db.create_chat(chat_uuid, user1_id, user2_id, user1['nickname'], user2['nickname'], chat_district)
    
    active_chats[user1_id] = user2_id
    active_chats[user2_id] = user1_id
    active_chat_ids[user1_id] = chat_uuid
    active_chat_ids[user2_id] = chat_uuid
    
    bot_stats["total_chats"] += 1
    bot_stats["active_chats"] = len(active_chats) // 2
    
    try:
        if user1['district'] == user2['district']:
            info1 = f"\n📍 Вы оба из {user1['district']}!"
            info2 = f"\n📍 Вы оба из {user2['district']}!"
        else:
            info1 = f"\n📍 Ты из {user1['district']}, собеседник из {user2['district']}"
            info2 = f"\n📍 Ты из {user2['district']}, собеседник из {user1['district']}"
        
        await bot.send_message(
            user1_id,
            f"🔔 <b>Собеседник найден!</b>\n\n"
            f"Ты общаешься с: {user2['nickname']}{info1}",
            reply_markup=kb.chat_actions()
        )
        
        await bot.send_message(
            user2_id,
            f"🔔 <b>Собеседник найден!</b>\n\n"
            f"Ты общаешься с: {user1['nickname']}{info2}",
            reply_markup=kb.chat_actions()
        )
    except Exception as e:
        logger.error(f"Error notifying users: {e}")
        return False
    
    await update_online_stats(db)
    return True

async def stop_chat(user_id, db, bot):
    partner_id = active_chats.get(user_id)
    if not partner_id:
        return
    
    user = db.get_user(user_id)
    partner = db.get_user(partner_id)
    
    if user_id in active_chat_ids:
        db.end_chat(active_chat_ids[user_id])
    
    if user_id in active_chats:
        del active_chats[user_id]
        db.update_online_status(user_id, False)
    if partner_id in active_chats:
        del active_chats[partner_id]
        db.update_online_status(partner_id, False)
    
    if user_id in active_chat_ids:
        del active_chat_ids[user_id]
    if partner_id in active_chat_ids:
        del active_chat_ids[partner_id]
    
    await update_online_stats(db)
    
    # Уведомляем обоих о завершении чата
    try:
        await bot.send_message(user_id, "✅ Чат завершен", reply_markup=kb.main_menu())
        await bot.send_message(partner_id, "❌ Собеседник покинул чат", reply_markup=kb.main_menu())
    except:
        pass
    
    # Отправляем клавиатуру для оценки ОБОИМ пользователям
    if user and not db.check_banned(user_id):
        rating_keyboard1 = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="👍", callback_data=f"like_{partner_id}"),
                InlineKeyboardButton(text="👎", callback_data=f"dislike_{partner_id}")
            ],
            [
                InlineKeyboardButton(text="🚫 В ЧС", callback_data=f"blacklist_add_{partner_id}"),
                InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_menu")
            ]
        ])
        try:
            await bot.send_message(
                user_id,
                f"👤 Как тебе общение с {partner['nickname']}?\nОцени собеседника:",
                reply_markup=rating_keyboard1
            )
        except:
            pass
    
    if partner and not db.check_banned(partner_id):
        rating_keyboard2 = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="👍", callback_data=f"like_{user_id}"),
                InlineKeyboardButton(text="👎", callback_data=f"dislike_{user_id}")
            ],
            [
                InlineKeyboardButton(text="🚫 В ЧС", callback_data=f"blacklist_add_{user_id}"),
                InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_menu")
            ]
        ])
        try:
            await bot.send_message(
                partner_id,
                f"👤 Как тебе общение с {user['nickname']}?\nОцени собеседника:",
                reply_markup=rating_keyboard2
            )
        except:
            pass

# ========== КОМАНДЫ ==========
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    await force_cleanup_user(user_id, db)
    
    if db.check_banned(user_id):
        await message.answer("❌ Вы заблокированы.")
        return
    
    user = db.get_user(user_id)
    if not user:
        nickname = generate_nickname()
        await message.answer(
            "👋 Добро пожаловать в ТюменьChat!\n\nВыбери свой район:",
            reply_markup=kb.districts_keyboard()
        )
        await state.set_state(States.changing_district)
        await state.update_data(new_user=True, nickname=nickname)
        return
    
    db.update_user_activity(user_id)
    db.update_daily_stats()
    await show_main_menu(message, user_id)

@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    if message.from_user.id in ADMIN_IDS:
        await message.answer("👑 Панель администратора", reply_markup=kb.admin_menu())
    else:
        await message.answer("❌ Нет доступа")

@dp.message(Command("myid"))
async def cmd_myid(message: types.Message):
    user_id = message.from_user.id
    user = db.get_user(user_id)
    text = f"🆔 Твой ID: <code>{user_id}</code>"
    if user:
        text += f"\n✅ Ник: {user['nickname']}"
    else:
        text += "\n❌ Не зарегистрирован. Нажми /start"
    await message.answer(text)

@dp.message(Command("online"))
async def cmd_online(message: types.Message):
    online_users, online_by_district = await update_online_stats(db)
    
    text = "🟢 <b>Сейчас онлайн</b>\n\n"
    text += f"👥 Всего: {len(online_users)} человек\n"
    text += f"⏳ В очереди: {len(waiting_users)}\n"
    text += f"💬 В чатах: {len(active_chats) // 2}\n\n"
    
    if online_by_district:
        text += "📊 <b>По районам:</b>\n"
        for district, count in sorted(online_by_district.items(), key=lambda x: x[1], reverse=True)[:5]:
            text += f"  {district}: {count} чел.\n"
    
    await message.answer(text)

@dp.message(Command("fix_online"))
async def cmd_fix_online(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    online_users, online_by_district = await update_online_stats(db)
    
    report = "✅ Онлайн статистика исправлена!\n\n"
    report += f"👥 Всего онлайн: {len(online_users)}\n"
    report += f"⏳ В очереди: {len(waiting_users)}\n"
    report += f"💬 В чатах: {len(active_chats) // 2}\n\n"
    report += "📊 По районам:\n"
    
    for district, count in sorted(online_by_district.items(), key=lambda x: x[1], reverse=True):
        report += f"  {district}: {count} чел.\n"
    
    await message.answer(report)

@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    if message.from_user.id in broadcast_data:
        del broadcast_data[message.from_user.id]
    await state.clear()
    await message.answer("❌ Отменено", reply_markup=kb.main_menu())

# ========== УНИВЕРСАЛЬНЫЙ ОБРАБОТЧИК ВСЕХ КНОПОК ==========
@dp.callback_query()
async def handle_all_callbacks(callback: types.CallbackQuery, state: FSMContext):
    data = callback.data
    user_id = callback.from_user.id
    
    print(f"\n🔴 НАЖАТА КНОПКА: {data}")
    
    async def safe_edit(text, reply_markup=None):
        try:
            await callback.message.edit_text(text, reply_markup=reply_markup)
        except:
            await callback.message.answer(text, reply_markup=reply_markup)
    
    # АДМИН-ПАНЕЛЬ
    if data.startswith('admin_'):
        if user_id not in ADMIN_IDS:
            await callback.answer("❌ Нет доступа", show_alert=True)
            return
        
        if data == "admin_stats":
            stats = db.get_all_stats()
            online = len(set(active_chats.keys()) | set(waiting_users))
            text = f"👑 <b>Статистика</b>\n\n👥 Всего: {stats['total_users']}\n🚫 Бан: {stats['banned_users']}\n🟢 Онлайн: {online}\n⏳ В очереди: {len(waiting_users)}\n💬 В чатах: {len(active_chats)//2}"
            await safe_edit(text, kb.admin_menu())
        
        elif data == "admin_online":
            online = set(active_chats.keys()) | set(waiting_users)
            if not online:
                text = "👥 Сейчас нет онлайн пользователей"
            else:
                text = "👥 <b>Онлайн пользователи</b>\n\n"
                for uid in list(online)[:20]:
                    user = db.get_user(uid)
                    if user:
                        status = "💬 в чате" if uid in active_chats else "⏳ в очереди"
                        text += f"• {user['nickname']} - {status}\n"
            await safe_edit(text, kb.admin_menu())
        
        elif data == "admin_districts":
            stats = db.get_district_stats()
            text = "🗺️ <b>Статистика по районам</b>\n\n"
            for s in stats:
                text += f"{s['district']}\n   👥 {s['user_count']} | 🟢 {s['online_now']}\n\n"
            await safe_edit(text, kb.admin_menu())
        
        elif data == "admin_bans":
            banned = db.get_banned_users()
            if not banned:
                text = "✅ Нет забаненных пользователей"
            else:
                text = "🔨 <b>Забаненные пользователи</b>\n\n"
                for u in banned[:20]:
                    text += f"• {u['nickname']} (ID: {u['user_id']})\n"
                    if u['ban_reason']:
                        text += f"  Причина: {u['ban_reason']}\n"
            await safe_edit(text, kb.admin_menu())
        
        elif data == "admin_daily":
            stats = db.get_all_stats()
            text = "📈 <b>Статистика по дням</b>\n\n"
            for d in stats['daily_stats'][:7]:
                text += f"<b>{d['date']}:</b> 💬{d['total_messages']} 👥+{d['new_users']}\n"
            await safe_edit(text, kb.admin_menu())
        
        elif data == "admin_logs":
            logs = db.get_admin_logs(20)
            if not logs:
                text = "📋 Логов нет"
            else:
                text = "📋 <b>Последние действия</b>\n\n"
                for log in logs:
                    admin = db.get_user(log['admin_id'])
                    name = admin['nickname'] if admin else str(log['admin_id'])
                    text += f"• {log['timestamp'][:16]} {name}: {log['action']}\n"
            await safe_edit(text, kb.admin_menu())
        
        elif data == "admin_getdb":
            await callback.answer("⏳ Загружаю...")
            try:
                ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                backup = f"tyumenchat_backup_{ts}.db"
                shutil.copy2(db.db_name, backup)
                await callback.message.answer_document(FSInputFile(backup), caption=f"📊 База данных на {ts}")
                os.remove(backup)
            except Exception as e:
                await callback.message.answer(f"❌ Ошибка: {e}")
        
        elif data == "admin_menu":
            await safe_edit("👑 Панель администратора", kb.admin_menu())
        
        elif data == "admin_search_district":
            districts = "\n".join([f"• {d}" for d in TYUMEN_DISTRICTS])
            await safe_edit(f"🔍 Введи название района:\n\n{districts}", kb.cancel_keyboard())
            await state.set_state(States.admin_search_district)
        
        elif data == "admin_search_messages":
            await safe_edit("🔍 Введи текст для поиска:", kb.cancel_keyboard())
            await state.set_state(States.admin_search_messages)
        
        elif data == "admin_user_details":
            await safe_edit("👤 Введи ID или ник:", kb.cancel_keyboard())
            await state.set_state(States.admin_get_user)
        
        elif data == "admin_broadcast":
            broadcast_data[user_id] = "waiting"
            await safe_edit("📤 Отправь текст для рассылки:", kb.cancel_keyboard())
            await state.set_state(States.admin_broadcast)
    
    # ПОЛЬЗОВАТЕЛЬСКИЕ КНОПКИ
    elif data == "menu":
        await show_main_menu(callback.message, user_id)
    
    elif data == "search_menu":
        await safe_edit("🔍 <b>Поиск собеседника</b>\n\nВыбери режим:", kb.search_menu_keyboard())
    
    elif data == "search_all":
        user = db.get_user(user_id)
        if not user:
            await safe_edit("❌ Сначала нажми /start", kb.main_menu())
        else:
            await force_cleanup_user(user_id, db)
            
            partner_id = None
            for uid in waiting_users:
                if uid != user_id and not db.check_banned(uid) and not db.is_blocked(user_id, uid) and not db.is_blocked(uid, user_id):
                    partner_id = uid
                    break
            
            if partner_id:
                waiting_users.remove(partner_id)
                await create_chat(user_id, partner_id, db, bot)
                await safe_edit("✅ Собеседник найден! Чат создан.")
            else:
                db.update_online_status(user_id, True)
                if user_id not in waiting_users:
                    waiting_users.append(user_id)
                
                await update_online_stats(db)
                await safe_edit(
                    f"⏳ <b>Поиск собеседника...</b>\n\nПозиция в очереди: {len(waiting_users)}",
                    InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="❌ Отменить поиск", callback_data="cancel_search")]
                    ])
                )
    
    elif data == "search_district":
        user = db.get_user(user_id)
        if not user:
            await safe_edit("❌ Сначала нажми /start", kb.main_menu())
        else:
            await force_cleanup_user(user_id, db)
            
            partner_id = None
            for uid in waiting_users:
                partner = db.get_user(uid)
                if partner and uid != user_id and partner['district'] == user['district'] and not db.check_banned(uid) and not db.is_blocked(user_id, uid) and not db.is_blocked(uid, user_id):
                    partner_id = uid
                    break
            
            if partner_id:
                waiting_users.remove(partner_id)
                await create_chat(user_id, partner_id, db, bot)
                await safe_edit("✅ Собеседник найден! Чат создан.")
            else:
                db.update_online_status(user_id, True)
                if user_id not in waiting_users:
                    waiting_users.append(user_id)
                
                await update_online_stats(db)
                await safe_edit(
                    f"⏳ <b>Поиск собеседника в районе {user['district']}...</b>\n\nПозиция в очереди: {len(waiting_users)}",
                    InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="❌ Отменить поиск", callback_data="cancel_search")]
                    ])
                )
    
    elif data == "cancel_search":
        if user_id in waiting_users:
            waiting_users.remove(user_id)
            db.update_online_status(user_id, False)
            await update_online_stats(db)
        await safe_edit("❌ Поиск отменен", kb.main_menu())
        await state.clear()
    
    elif data == "districts_menu":
        stats = db.get_district_stats()
        text = "🗺️ <b>Районы Тюмени</b>\n\n"
        for s in stats:
            text += f"{s['district']}\n   👥 {s['user_count']} | 🟢 {s['online_now']}\n\n"
        await safe_edit(text, kb.districts_keyboard())
    
    elif data.startswith("district_"):
        idx = int(data.split("_")[1]) - 1
        district = TYUMEN_DISTRICTS[idx]
        st = await state.get_data()
        
        if st.get('new_user'):
            db.add_user(user_id, st['nickname'], district)
            await state.clear()
            await show_main_menu(callback.message, user_id)
        else:
            user = db.get_user(user_id)
            if user:
                db.update_user_district(user_id, district)
                await callback.answer("✅ Район изменен")
                await show_main_menu(callback.message, user_id)
    
    elif data == "top_rating":
        top = db.get_top_users(10)
        if not top:
            await safe_edit("🏆 Пока нет данных для рейтинга", kb.main_menu())
        else:
            text = "🏆 <b>Топ 10 пользователей</b>\n\n"
            for i, u in enumerate(top, 1):
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                text += f"{medal} {u['nickname']} ({u['district']})\n"
                text += f"   👍 {u['likes']} | 👎 {u['dislikes']} | Рейтинг: {u['rating']:.1f}%\n\n"
            await safe_edit(text, kb.main_menu())
    
    elif data == "settings":
        user = db.get_user(user_id)
        if user:
            anon = "🕵️ Вкл" if user['anon_mode'] else "👁️ Выкл"
            text = f"⚙️ <b>Настройки</b>\n\n👤 {user['nickname']}\n🏘️ {user['district']}\n{anon}"
            await safe_edit(text, kb.settings_menu())
    
    elif data == "change_nick":
        await safe_edit("✏️ Введи новый ник (до 20 символов):", kb.cancel_keyboard())
        await state.set_state(States.changing_nick)
    
    elif data == "change_district":
        await safe_edit("🏘️ Выбери новый район:", kb.change_district_keyboard())
    
    elif data.startswith("change_district_"):
        idx = int(data.split("_")[2]) - 1
        district = TYUMEN_DISTRICTS[idx]
        db.update_user_district(user_id, district)
        await callback.answer("✅ Район изменен")
        user = db.get_user(user_id)
        anon = "🕵️ Вкл" if user['anon_mode'] else "👁️ Выкл"
        text = f"⚙️ <b>Настройки</b>\n\n👤 {user['nickname']}\n🏘️ {user['district']}\n{anon}"
        await safe_edit(text, kb.settings_menu())
    
    elif data == "toggle_anon":
        db.toggle_anon_mode(user_id)
        user = db.get_user(user_id)
        anon = "🕵️ Вкл" if user['anon_mode'] else "👁️ Выкл"
        text = f"⚙️ <b>Настройки</b>\n\n👤 {user['nickname']}\n🏘️ {user['district']}\n{anon}"
        await safe_edit(text, kb.settings_menu())
    
    elif data == "blacklist":
        bl = db.get_blacklist(user_id)
        text = f"🚫 <b>Черный список</b>\n\nВсего заблокировано: {len(bl)}"
        await safe_edit(text, kb.blacklist_menu())
    
    elif data == "show_blacklist":
        bl = db.get_blacklist(user_id)
        if not bl:
            await safe_edit("📋 Твой черный список пуст", kb.blacklist_menu())
        else:
            keyboard = []
            for b in bl:
                keyboard.append([InlineKeyboardButton(
                    text=f"❌ {b['nickname']}",
                    callback_data=f"blacklist_remove_{b['blocked_id']}"
                )])
            keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data="blacklist")])
            await safe_edit(
                "🚫 <b>Черный список:</b>\n\nНажми на пользователя, чтобы удалить:",
                InlineKeyboardMarkup(inline_keyboard=keyboard)
            )
    
    elif data.startswith("blacklist_remove_"):
        tid = int(data.replace("blacklist_remove_", ""))
        db.remove_from_blacklist(user_id, tid)
        await callback.answer("✅ Пользователь удален из ЧС")
        bl = db.get_blacklist(user_id)
        if not bl:
            await safe_edit("📋 Черный список пуст", kb.blacklist_menu())
        else:
            keyboard = []
            for b in bl:
                keyboard.append([InlineKeyboardButton(
                    text=f"❌ {b['nickname']}",
                    callback_data=f"blacklist_remove_{b['blocked_id']}"
                )])
            keyboard.append([InlineKeyboardButton(text="◀️ Назад", callback_data="blacklist")])
            await safe_edit(
                "🚫 <b>Черный список:</b>\n\nНажми на пользователя, чтобы удалить:",
                InlineKeyboardMarkup(inline_keyboard=keyboard)
            )
    
    elif data.startswith("blacklist_add_"):
        tid = int(data.replace("blacklist_add_", ""))
        if user_id == tid:
            await callback.answer("❌ Нельзя добавить себя в ЧС", show_alert=True)
        else:
            db.add_to_blacklist(user_id, tid)
            await callback.answer("✅ Пользователь добавлен в ЧС")
            await safe_edit("✅ Пользователь добавлен в черный список", kb.main_menu())
    
    elif data == "stop":
        if user_id in active_chats:
            partner_id = active_chats[user_id]
            await stop_chat(user_id, db, bot)
            await safe_edit("✅ Чат завершен", kb.main_menu())
        elif user_id in waiting_users:
            waiting_users.remove(user_id)
            db.update_online_status(user_id, False)
            await update_online_stats(db)
            await safe_edit("✅ Ты удален из очереди поиска", kb.main_menu())
        else:
            await callback.answer("❌ Ты не в чате", show_alert=True)
    
    # ЛАЙКИ И ДИЗЛАЙКИ
    elif data.startswith('like_') or data.startswith('dislike_'):
        parts = data.split('_')
        action = parts[0]
        partner_id = int(parts[1])
        
        print(f"👍👎 Получена оценка: {action} для пользователя {partner_id}")
        
        if db.check_banned(user_id):
            await callback.answer("❌ Вы заблокированы", show_alert=True)
            return
        
        partner = db.get_user(partner_id)
        if not partner:
            await callback.answer("❌ Собеседник не найден", show_alert=True)
            return
        
        user = db.get_user(user_id)
        if not user:
            await callback.answer("❌ Ошибка", show_alert=True)
            return
        
        is_like = (action == "like")
        db.update_rating(partner_id, is_like)
        
        updated_partner = db.get_user(partner_id)
        new_rating = updated_partner['rating'] if updated_partner else 50.0
        
        if is_like:
            text = f"👍 Ты поставил лайк пользователю {partner['nickname']}!\n\n"
            text += f"Теперь его рейтинг: {new_rating:.1f}%"
            
            try:
                await bot.send_message(
                    partner_id,
                    f"👍 {user['nickname']} оценил(а) тебя положительно!\n"
                    f"Твой текущий рейтинг: {new_rating:.1f}%"
                )
            except:
                pass
        else:
            text = f"👎 Ты поставил дизлайк пользователю {partner['nickname']}.\n\n"
            text += f"Теперь его рейтинг: {new_rating:.1f}%"
        
        await safe_edit(text, kb.main_menu())
        
        if db.check_banned(partner_id):
            try:
                await bot.send_message(
                    partner_id,
                    "🚫 Вы были заблокированы из-за большого количества дизлайков.\n"
                    "Обратитесь к администратору для разблокировки."
                )
            except:
                pass
        
        await callback.answer()
    
    elif data == "cancel":
        await state.clear()
        await show_main_menu(callback.message, user_id)
    
    await callback.answer()

# ========== ОБРАБОТЧИКИ СОСТОЯНИЙ АДМИН-ПАНЕЛИ ==========
@dp.message(States.admin_search_district)
async def process_admin_search_district(message: types.Message, state: FSMContext):
    """Обработка поиска пользователей по району"""
    admin_id = message.from_user.id
    
    if admin_id not in ADMIN_IDS:
        await state.clear()
        return
    
    search_text = message.text.strip()
    
    # Ищем похожие районы
    matching_districts = []
    for district in TYUMEN_DISTRICTS:
        if search_text.lower() in district.lower():
            matching_districts.append(district)
    
    if not matching_districts:
        await message.answer(
            f"❌ Район '{search_text}' не найден.\n"
            f"Попробуй еще раз:",
            reply_markup=kb.cancel_keyboard()
        )
        return
    
    if len(matching_districts) > 1:
        districts_list = "\n".join([f"• {d}" for d in matching_districts])
        await message.answer(
            f"🔍 Найдено несколько районов:\n\n{districts_list}\n\n"
            f"Уточни запрос (введи полное название):",
            reply_markup=kb.cancel_keyboard()
        )
        return
    
    # Один район - ищем пользователей
    district = matching_districts[0]
    users = db.get_users_by_district(district)
    
    if not users:
        await message.answer(
            f"👥 В районе {district} пока нет пользователей",
            reply_markup=kb.admin_menu()
        )
        await state.clear()
        return
    
    # Считаем статистику по району
    online_users = set(active_chats.keys()) | set(waiting_users)
    
    text = f"🏘️ <b>Район: {district}</b>\n\n"
    text += f"👥 Всего пользователей: {len(users)}\n"
    text += f"🟢 Сейчас онлайн: {len([u for u in users if u[0] in online_users])}\n\n"
    text += f"<b>Список пользователей:</b>\n\n"
    
    for user in users[:30]:  # Показываем первых 30
        last_active = user[3][:16] if user[3] else "никогда"
        status = "🚫 БАН" if user[9] else "✅"
        online = "🟢" if user[0] in online_users else "⚫"
        
        text += f"{online} <b>{user[1]}</b> {status}\n"
        text += f"   🆔 <code>{user[0]}</code>\n"
        text += f"   🕐 {last_active} | 💬 {user[4]} чатов\n"
        text += f"   👍 {user[6] or 0} | 👎 {user[7] or 0} | Рейтинг: {user[8] or 50:.1f}%\n\n"
    
    if len(users) > 30:
        text += f"... и ещё {len(users) - 30} пользователей"
    
    await message.answer(text, reply_markup=kb.admin_menu())
    await state.clear()

@dp.message(States.admin_search_messages)
async def process_admin_search_messages(message: types.Message, state: FSMContext):
    """Обработка поиска сообщений по тексту"""
    admin_id = message.from_user.id
    
    if admin_id not in ADMIN_IDS:
        await state.clear()
        return
    
    search_text = message.text.strip()
    
    if len(search_text) < 3:
        await message.answer(
            "❌ Слишком короткий запрос. Минимум 3 символа.",
            reply_markup=kb.cancel_keyboard()
        )
        return
    
    # Отправляем сообщение о начале поиска
    status_msg = await message.answer("🔍 Ищу сообщения...")
    
    # Выполняем поиск
    messages = db.search_messages(search_text, limit=30)
    
    await status_msg.delete()
    
    if not messages:
        await message.answer(
            f"❌ Сообщения с текстом '{search_text}' не найдены",
            reply_markup=kb.admin_menu()
        )
        await state.clear()
        return
    
    # Формируем результат
    text = f"🔍 <b>Найдено {len(messages)} сообщений с текстом '{search_text}':</b>\n\n"
    
    for msg in messages[:20]:
        try:
            # Получаем данные из сообщения
            time = msg['timestamp'][:16] if msg['timestamp'] else "неизвестно"
            from_nick = msg['from_nick']
            to_nick = msg['to_nick']
            msg_text = msg['message_text']
            if msg_text and len(msg_text) > 50:
                msg_text = msg_text[:50] + "..."
            
            text += f"📅 {time}\n"
            text += f"👤 {from_nick} → {to_nick}\n"
            text += f"💬 {msg_text}\n\n"
        except Exception as e:
            logger.error(f"Error formatting message: {e}")
            continue
    
    if len(messages) > 20:
        text += f"... и ещё {len(messages) - 20} сообщений"
    
    # Разбиваем длинное сообщение на части если нужно
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
            await message.answer(part, reply_markup=kb.admin_menu())
    else:
        await message.answer(text, reply_markup=kb.admin_menu())
    
    await state.clear()

@dp.message(States.admin_get_user)
async def process_admin_get_user(message: types.Message, state: FSMContext):
    """Обработка поиска пользователя по ID или нику"""
    admin_id = message.from_user.id
    
    if admin_id not in ADMIN_IDS:
        await state.clear()
        return
    
    search_text = message.text.strip()
    
    # Пробуем найти по ID
    try:
        target_id = int(search_text)
        user = db.get_user_details(target_id)
        users = [user] if user else []
    except ValueError:
        # Ищем по нику
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT u.*, r.likes, r.dislikes, r.rating, r.banned, r.ban_reason
            FROM users u
            LEFT JOIN ratings r ON u.user_id = r.user_id
            WHERE u.nickname LIKE ?
            ORDER BY u.last_activity DESC
        ''', (f'%{search_text}%',))
        users = cursor.fetchall()
        conn.close()
    
    if not users:
        await message.answer(f"❌ Пользователь '{search_text}' не найден")
        await state.clear()
        return
    
    # Если несколько результатов
    if len(users) > 1:
        text = f"🔍 <b>Найдено {len(users)} пользователей:</b>\n\n"
        
        for i, user in enumerate(users[:10], 1):
            last_active = user['last_activity'][:16] if user['last_activity'] else "никогда"
            text += f"{i}. <b>{user['nickname']}</b> ({user['district']})\n"
            text += f"   🆔 <code>{user['user_id']}</code>\n"
            text += f"   🕐 {last_active}\n"
            text += f"   👍 {user['likes']} | 👎 {user['dislikes']} | 🚫 {'Да' if user['banned'] else 'Нет'}\n\n"
        
        await message.answer(text, reply_markup=kb.admin_menu())
        await state.clear()
        return
    
    # Если один результат - показываем детали
    user = users[0]
    
    # Получаем черный список
    blacklist = db.get_blacklist(user['user_id'])
    blacklist_text = ""
    if blacklist:
        blacklist_text = "\n🚫 <b>В ЧС у пользователя:</b>\n"
        for blocked in blacklist[:5]:
            blacklist_text += f"  • {blocked['nickname']}\n"
    
    # Получаем последние чаты
    recent_chats = db.get_user_chats(user['user_id'], 5)
    chats_text = ""
    if recent_chats:
        chats_text = "\n📋 <b>Последние чаты:</b>\n"
        for chat in recent_chats[:3]:
            partner_nick = chat['user2_nick'] if chat['user1_id'] == user['user_id'] else chat['user1_nick']
            chat_time = chat['start_time'][:16]
            msg_count = chat['message_count']
            chats_text += f"  • С {partner_nick} | {chat_time} | {msg_count} сообщ.\n"
    
    online_status = "🟢 Онлайн" if user['user_id'] in set(active_chats.keys()) | set(waiting_users) else "⚫ Офлайн"
    
    text = (
        f"👤 <b>Детали пользователя</b>\n\n"
        f"{online_status}\n"
        f"🆔 <b>ID:</b> <code>{user['user_id']}</code>\n"
        f"📝 <b>Ник:</b> {user['nickname']}\n"
        f"🏘️ <b>Район:</b> {user['district']}\n"
        f"🕵️ <b>Анонимный режим:</b> {'Включен' if user['anon_mode'] else 'Выключен'}\n"
        f"📅 <b>Присоединился:</b> {user['join_date'][:16]}\n"
        f"🕐 <b>Последняя активность:</b> {user['last_activity'][:16]}\n"
        f"📊 <b>Всего чатов:</b> {user['total_chats']}\n"
        f"💬 <b>Всего сообщений:</b> {user['total_messages']}\n\n"
        f"🏆 <b>Рейтинг:</b> {user['rating']:.1f}%\n"
        f"👍 <b>Лайки:</b> {user['likes']}\n"
        f"👎 <b>Дизлайки:</b> {user['dislikes']}\n"
        f"🚫 <b>Забанен:</b> {'Да' if user['banned'] else 'Нет'}"
    )
    
    if user['banned'] and user['ban_reason']:
        text += f"\n   Причина: {user['ban_reason']}"
    
    text += f"\n{blacklist_text}{chats_text}"
    
    # Кнопки действий
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔨 Забанить", callback_data=f"admin_ban_{user['user_id']}"),
            InlineKeyboardButton(text="✅ Разбанить", callback_data=f"admin_unban_{user['user_id']}")
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_user_details")]
    ])
    
    await message.answer(text, reply_markup=keyboard)
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith("admin_ban_"))
async def admin_ban_user(callback: types.CallbackQuery, state: FSMContext):
    """Начало процесса бана пользователя"""
    admin_id = callback.from_user.id
    
    if admin_id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    target_id = int(callback.data.replace("admin_ban_", ""))
    
    await callback.message.edit_text(
        f"🔨 <b>Бан пользователя {target_id}</b>\n\n"
        f"Введи причину бана:",
        reply_markup=kb.cancel_keyboard()
    )
    await state.update_data(ban_target=target_id)
    await state.set_state(States.admin_broadcast)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("admin_unban_"))
async def admin_unban_user(callback: types.CallbackQuery):
    """Разбан пользователя"""
    admin_id = callback.from_user.id
    
    if admin_id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    target_id = int(callback.data.replace("admin_unban_", ""))
    
    db.unban_user(target_id)
    db.log_admin_action(admin_id, "unban", target_id, "Разбанен администратором")
    
    await callback.answer(f"✅ Пользователь {target_id} разбанен", show_alert=True)
    
    # Возвращаемся в админ-меню
    await callback.message.edit_text("✅ Готово", reply_markup=kb.admin_menu())

# ========== ОБРАБОТЧИК РАССЫЛКИ ==========
@dp.callback_query(lambda c: c.data == "broadcast_send")
async def broadcast_send_handler(callback: types.CallbackQuery):
    admin_id = callback.from_user.id
    text = broadcast_data.get(admin_id)
    
    if not text:
        await callback.answer("❌ Ошибка")
        return
    
    await callback.message.edit_text("⏳ Отправка...")
    
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT user_id FROM users')
    users = cursor.fetchall()
    conn.close()
    
    sent = 0
    failed = 0
    
    for (uid,) in users:
        if db.check_banned(uid):
            failed += 1
            continue
        try:
            await bot.send_message(uid, f"📢 <b>Рассылка</b>\n\n{text}")
            sent += 1
            await asyncio.sleep(0.05)
        except:
            failed += 1
    
    await callback.message.edit_text(
        f"✅ Отправлено: {sent}\n❌ Ошибок: {failed}",
        reply_markup=kb.admin_menu()
    )
    
    if admin_id in broadcast_data:
        del broadcast_data[admin_id]
    
    db.log_admin_action(admin_id, "broadcast", details=f"Sent: {sent}, Failed: {failed}")
    await callback.answer()

@dp.callback_query(lambda c: c.data == "broadcast_cancel")
async def broadcast_cancel_handler(callback: types.CallbackQuery):
    admin_id = callback.from_user.id
    if admin_id in broadcast_data:
        del broadcast_data[admin_id]
    await callback.message.edit_text("❌ Отменено", reply_markup=kb.admin_menu())
    await callback.answer()

# ========== ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ ==========
@dp.message()
async def handle_messages(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    # Смена ника
    if await state.get_state() == States.changing_nick:
        if not message.text:
            await message.answer("❌ Отправь текстовое сообщение")
            return
        
        new_nick = message.text.strip()
        if len(new_nick) > 20 or len(new_nick) < 2:
            await message.answer("❌ Ник должен быть 2-20 символов")
            return
        
        db.update_nickname(user_id, new_nick)
        await state.clear()
        await show_main_menu(message, user_id)
        return
    
    # Рассылка
    if user_id in broadcast_data and broadcast_data[user_id] == "waiting":
        if not message.text:
            await message.answer("❌ Отправь текст для рассылки")
            return
        
        broadcast_data[user_id] = message.text
        confirm = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Отправить", callback_data="broadcast_send"),
             InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast_cancel")]
        ])
        await message.answer(f"📤 Подтверждение:\n\n{message.text}", reply_markup=confirm)
        return
    
    # Проверка регистрации
    user = db.get_user(user_id)
    if not user:
        return
    
    if db.check_banned(user_id):
        return
    
    # Проверка чата
    if user_id not in active_chats:
        return
    
    partner_id = active_chats[user_id]
    if partner_id not in active_chats:
        del active_chats[user_id]
        return
    
    partner = db.get_user(partner_id)
    if not partner:
        return
    
    # Определяем имя
    if user['anon_mode']:
        sender = user['nickname']
    else:
        sender = message.from_user.full_name or "Пользователь"
        if message.from_user.username:
            sender += f" (@{message.from_user.username})"
    
    chat_uuid = active_chat_ids.get(user_id)
    
    # Отправка сообщения
    try:
        if message.text:
            await bot.send_message(partner_id, f"<b>{sender}:</b> {message.text}")
            if chat_uuid:
                db.save_message(chat_uuid, user_id, partner_id, sender, partner['nickname'], message.text, "text")
        
        elif message.sticker:
            await bot.send_sticker(partner_id, message.sticker.file_id)
            if chat_uuid:
                db.save_message(chat_uuid, user_id, partner_id, sender, partner['nickname'], None, "sticker", message.sticker.file_id)
        
        elif message.photo:
            photo = message.photo[-1]
            caption = f"<b>{sender}:</b> {message.caption or '📸 Фото'}"
            await bot.send_photo(partner_id, photo.file_id, caption=caption)
            if chat_uuid:
                db.save_message(chat_uuid, user_id, partner_id, sender, partner['nickname'], message.caption, "photo", photo.file_id)
        
        elif message.video:
            caption = f"<b>{sender}:</b> {message.caption or '🎥 Видео'}"
            await bot.send_video(partner_id, message.video.file_id, caption=caption)
            if chat_uuid:
                db.save_message(chat_uuid, user_id, partner_id, sender, partner['nickname'], message.caption, "video", message.video.file_id)
        
        elif message.voice:
            await bot.send_voice(partner_id, message.voice.file_id)
            if chat_uuid:
                db.save_message(chat_uuid, user_id, partner_id, sender, partner['nickname'], None, "voice", message.voice.file_id)
        
        elif message.animation:
            caption = f"<b>{sender}:</b> {message.caption or '🎬 GIF'}"
            await bot.send_animation(partner_id, message.animation.file_id, caption=caption)
            if chat_uuid:
                db.save_message(chat_uuid, user_id, partner_id, sender, partner['nickname'], message.caption, "animation", message.animation.file_id)
        
        elif message.video_note:
            await bot.send_video_note(partner_id, message.video_note.file_id)
            if chat_uuid:
                db.save_message(chat_uuid, user_id, partner_id, sender, partner['nickname'], None, "video_note", message.video_note.file_id)
        
        elif message.audio:
            caption = f"<b>{sender}:</b> {message.caption or '🎵 Аудио'}"
            await bot.send_audio(partner_id, message.audio.file_id, caption=caption)
            if chat_uuid:
                db.save_message(chat_uuid, user_id, partner_id, sender, partner['nickname'], message.caption, "audio", message.audio.file_id)
        
        elif message.document:
            caption = f"<b>{sender}:</b> {message.caption or '📎 Документ'}"
            await bot.send_document(partner_id, message.document.file_id, caption=caption)
            if chat_uuid:
                db.save_message(chat_uuid, user_id, partner_id, sender, partner['nickname'], message.caption, "document", message.document.file_id)
    
    except Exception as e:
        logger.error(f"Error sending message: {e}")

# ========== ЗАПУСК ==========
async def main():
    print("=" * 50)
    print("✅ ТюменьChat бот запущен!")
    print("=" * 50)
    print(f"📊 База данных: {db.db_name}")
    print(f"👑 Администраторы: {ADMIN_IDS}")
    print(f"🤖 ID бота: {bot.id}")
    print("=" * 50)
    
    async def periodic_cleanup():
        while True:
            await asyncio.sleep(60)
            # Функция очистки
    
    asyncio.create_task(periodic_cleanup())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
