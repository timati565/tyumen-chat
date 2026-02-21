import asyncio
import logging
import datetime
import os
import shutil
import sqlite3
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.exceptions import TelegramBadRequest

from config import BOT_TOKEN, ADMIN_IDS, TYUMEN_DISTRICTS, DEBUG
from database import Database
import keyboards as kb
import utils
from utils import (
    generate_tyumen_nickname, get_user_rating_level,
    save_message_id, delete_bot_messages, send_temp_message,
    cleanup_invalid_chats, force_cleanup_user, set_bot
)

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
@dp.callback_query()
async def debug_all_callbacks(callback: types.CallbackQuery):
    """Отлавливает все callback для отладки"""
    print(f"\n🔴 ПОЛУЧЕН CALLBACK: {callback.data}")
    print(f"   От пользователя: {callback.from_user.id}")
    print(f"   Имя: {callback.from_user.first_name}")
    
    # Отвечаем, чтобы убрать "часики"
    await callback.answer()
    
    # Если это нужные нам кнопки, обрабатываем их здесь
    if callback.data == "search_all":
        await callback.message.edit_text("🔍 Тест: поиск по всей Тюмени")
    elif callback.data == "search_district":
        await callback.message.edit_text("🔍 Тест: поиск по району")
    elif callback.data == "search_menu":
        await callback.message.edit_text("🔍 Тест: меню поиска")
        
# Устанавливаем бота в утилиты
set_bot(bot)

# Инициализация базы данных
db = Database()

# ========== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ==========
waiting_users = utils.waiting_users
active_chats = utils.active_chats
chat_messages = utils.chat_messages
user_last_message = {}
search_mode = utils.search_mode
active_chat_ids = utils.active_chat_ids
bot_stats = utils.bot_stats

# Временное хранилище для рассылки
broadcast_data = {}

# ========== СОСТОЯНИЯ ==========
class States(StatesGroup):
    waiting = State()
    chatting = State()
    changing_nick = State()
    changing_district = State()
    admin_broadcast = State()
    admin_get_user = State()
    admin_search_district = State()
    admin_search_messages = State()
    admin_view_chat = State()

# ========== ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ ПРОВЕРКИ БОТА ==========
def is_bot(user_id):
    """Проверяет, является ли ID ID бота"""
    return user_id == bot.id

# ========== КОМАНДЫ ==========
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    # Игнорируем сообщения от бота
    if is_bot(message.from_user.id):
        return
    
    user_id = message.from_user.id
    
    # Принудительно очищаем пользователя от старых сессий
    await force_cleanup_user(user_id, db)
    await delete_bot_messages(user_id)
    
    # Проверяем забанен ли пользователь
    if db.check_banned(user_id):
        await message.answer("❌ Вы заблокированы. Обратитесь к администратору.")
        return
    
    # Получаем или создаем пользователя
    user = db.get_user(user_id)
    if user is None:
        nickname = generate_tyumen_nickname()
        # Предлагаем выбрать район
        await message.answer(
            "👋 Добро пожаловать в <b>ТюменьChat</b>!\n\n"
            "Для начала выбери свой район в Тюмени:",
            reply_markup=kb.districts_keyboard()
        )
        await state.set_state(States.changing_district)
        await state.update_data(new_user=True, nickname=nickname)
        return
    
    # Обновляем активность
    db.update_user_activity(user_id)
    db.update_daily_stats()
    
    await show_main_menu(message, user_id)

@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if is_bot(message.from_user.id):
        return
    
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к админ-панели")
        return
    
    await force_cleanup_user(user_id, db)
    
    await message.answer(
        "👑 <b>Панель администратора</b>\n\n"
        "Выберите действие:",
        reply_markup=kb.admin_menu()
    )

@dp.message(Command("debug"))
async def debug_search(message: types.Message):
    if is_bot(message.from_user.id):
        return
    
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS:
        return
    
    text = "🔍 <b>Диагностика поиска:</b>\n\n"
    text += f"📊 <b>Очередь:</b> {len(waiting_users)} пользователей\n"
    
    for uid in waiting_users[:5]:
        user = db.get_user(uid)
        if user:
            text += f"  • {user['nickname']} (ID: {uid}) - {user['district']}\n"
    
    text += f"\n💬 <b>Активные чаты:</b> {len(active_chats) // 2}\n"
    for uid, pid in list(active_chats.items())[:5]:
        if uid < pid:
            user1 = db.get_user(uid)
            user2 = db.get_user(pid)
            if user1 and user2:
                text += f"  • {user1['nickname']} - {user2['nickname']}\n"
    
    text += f"\n👥 <b>Забаненные:</b> {len(db.get_banned_users())}\n"
    text += f"\n📊 <b>Статистика районов:</b>\n"
    
    for stat in db.get_district_stats():
        text += f"  • {stat['district']}: {stat['online_now']} онлайн\n"
    
    await message.answer(text)

@dp.message(Command("reset_queue"))
async def reset_queue(message: types.Message):
    if is_bot(message.from_user.id):
        return
    
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS:
        return
    
    waiting_users.clear()
    active_chats.clear()
    active_chat_ids.clear()
    search_mode.clear()
    
    # Сбрасываем онлайн статус в БД
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE district_stats SET online_now = 0')
    conn.commit()
    conn.close()
    
    await message.answer("✅ Очередь и активные чаты сброшены")

@dp.message(Command("online"))
async def show_online(message: types.Message):
    if is_bot(message.from_user.id):
        return
    
    """Показывает текущий онлайн"""
    online_users = set(active_chats.keys()) | set(waiting_users)
    
    text = "🟢 <b>Сейчас онлайн:</b>\n\n"
    text += f"👥 Всего: {len(online_users)} человек\n"
    text += f"⏳ В очереди: {len(waiting_users)}\n"
    text += f"💬 В чатах: {len(active_chats) // 2}\n\n"
    
    # Статистика по районам
    districts_online = {}
    for uid in online_users:
        user = db.get_user(uid)
        if user:
            district = user['district']
            districts_online[district] = districts_online.get(district, 0) + 1
    
    if districts_online:
        text += "📊 По районам:\n"
        for district, count in sorted(districts_online.items(), key=lambda x: x[1], reverse=True)[:5]:
            text += f"  {district}: {count} чел.\n"
    
    await message.answer(text)

@dp.message(Command("users"))
async def list_users(message: types.Message):
    if is_bot(message.from_user.id):
        return
    
    """Показывает список пользователей (только для админа)"""
    if message.from_user.id not in ADMIN_IDS:
        return
    
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT user_id, nickname FROM users LIMIT 20')
    users = cursor.fetchall()
    conn.close()
    
    text = "📋 <b>Пользователи:</b>\n\n"
    for user in users:
        text += f"• {user['nickname']}: <code>{user['user_id']}</code>\n"
    
    await message.answer(text)

@dp.message(Command("myid"))
async def show_my_id(message: types.Message):
    if is_bot(message.from_user.id):
        return
    
    """Показывает ID пользователя"""
    user_id = message.from_user.id
    await message.answer(f"🆔 Твой ID: <code>{user_id}</code>")
    
    # Проверяем, есть ли пользователь в БД
    user = db.get_user(user_id)
    if user:
        await message.answer(f"✅ Ты зарегистрирован как: {user['nickname']}")
    else:
        await message.answer("❌ Ты не зарегистрирован! Нажми /start")

@dp.message(Command("fix_online"))
async def fix_online_stats(message: types.Message):
    if is_bot(message.from_user.id):
        return
    
    user_id = message.from_user.id
    
    if user_id not in ADMIN_IDS:
        return
    
    # Сначала сбрасываем всю онлайн статистику в БД
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE district_stats SET online_now = 0')
    conn.commit()
    conn.close()
    
    # Теперь правильно пересчитываем онлайн для каждого района
    online_users = set(active_chats.keys()) | set(waiting_users)
    
    # Словарь для подсчета онлайн по районам
    online_by_district = {}
    
    for uid in online_users:
        user = db.get_user(uid)
        if user and not db.check_banned(uid):
            district = user['district']
            online_by_district[district] = online_by_district.get(district, 0) + 1
    
    # Обновляем статистику в БД
    conn = db.get_connection()
    cursor = conn.cursor()
    
    for district, count in online_by_district.items():
        cursor.execute('''
            UPDATE district_stats SET online_now = ? WHERE district = ?
        ''', (count, district))
    
    conn.commit()
    conn.close()
    
    # Обновляем глобальную статистику
    bot_stats["online_users"] = len(online_users)
    
    # Формируем отчет
    report = "✅ Онлайн статистика исправлена!\n\n"
    report += f"👥 Всего онлайн: {len(online_users)}\n"
    report += f"⏳ В очереди: {len(waiting_users)}\n"
    report += f"💬 В чатах: {len(active_chats) // 2}\n\n"
    report += "📊 По районам:\n"
    
    for district, count in sorted(online_by_district.items(), key=lambda x: x[1], reverse=True):
        report += f"  {district}: {count} чел.\n"
    
    await message.answer(report)

async def show_main_menu(message: types.Message, user_id: int):
    """Показывает главное меню"""
    if is_bot(user_id):
        return
    
    if db.check_banned(user_id):
        await message.answer("❌ Вы заблокированы. Обратитесь к администратору.")
        return
    
    user = db.get_user(user_id)
    if user is None:
        return
    
    anon_status = "🕵️ Включен" if user['anon_mode'] else "👁️ Выключен"
    rating = user['rating'] or 50.0
    rating_level = get_user_rating_level(rating)
    
    blacklist = db.get_blacklist(user_id)
    blacklist_count = len(blacklist)
    
    district_stats = db.get_district_stats()
    online_in_district = 0
    for stat in district_stats:
        if stat['district'] == user['district']:
            online_in_district = stat['online_now']
            break
    
    text = (
        f"👋 Добро пожаловать в <b>ТюменьChat</b>!\n\n"
        f"👤 Твой ник: <b>{user['nickname']}</b>\n"
        f"🏘️ Район: {user['district']}\n"
        f"🕵️ Анонимный режим: {anon_status}\n"
        f"🏆 Твой рейтинг: {rating:.1f}% ({rating_level})\n"
        f"👍 Лайки: {user['likes']} | 👎 Дизлайки: {user['dislikes']}\n"
        f"📍 В твоем районе онлайн: {online_in_district} чел.\n\n"
        f"🔹 Общайся с тюменцами анонимно!"
    )
    
    msg = await message.answer(text, reply_markup=kb.main_menu())
    await save_message_id(user_id, msg.message_id)

# ========== РАЙОНЫ ==========
@dp.callback_query(F.data == "districts_menu")
async def districts_menu_callback(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    
    if db.check_banned(user_id):
        await callback.answer("❌ Вы заблокированы", show_alert=True)
        return
    
    stats = db.get_district_stats()
    
    text = "🗺️ <b>Районы Тюмени</b>\n\n"
    text += "Статистика по районам:\n\n"
    
    for stat in stats:
        text += f"{stat['district']}\n"
        text += f"   👥 Всего: {stat['user_count']} | 🟢 Онлайн: {stat['online_now']}\n\n"
    
    await callback.message.edit_text(text, reply_markup=kb.districts_keyboard())
    await callback.answer()

@dp.callback_query(F.data.startswith("district_"))
async def select_district(callback: types.CallbackQuery, state: FSMContext):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    district_index = int(callback.data.split("_")[1]) - 1
    selected_district = TYUMEN_DISTRICTS[district_index]
    
    data = await state.get_data()
    new_user = data.get('new_user', False)
    nickname = data.get('nickname')
    
    if new_user:
        # Новый пользователь
        db.add_user(user_id, nickname, selected_district)
        user = db.get_user(user_id)
        if user:
            logger.info(f"New user registered: {user_id} ({nickname}) in {selected_district}")
        await state.clear()
        await show_main_menu(callback.message, user_id)
    else:
        # Меняем район
        user = db.get_user(user_id)
        if user:
            old_district = user['district']
            db.update_user_district(user_id, selected_district)
            await callback.answer(f"✅ Район изменен на {selected_district}", show_alert=True)
            
            await callback.message.edit_text(
                f"🏘️ Район успешно изменен!\n"
                f"Был: {old_district}\n"
                f"Стал: {selected_district}",
                reply_markup=kb.settings_menu()
            )
        await state.clear()

# ========== ПОИСК СОБЕСЕДНИКА ==========
@dp.callback_query(F.data == "search_menu")
async def search_menu_callback(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    
    await force_cleanup_user(user_id, db)
    
    if db.check_banned(user_id):
        await callback.answer("❌ Вы заблокированы", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🔍 <b>Поиск собеседника</b>\n\n"
        "Выбери режим поиска:",
        reply_markup=kb.search_menu_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "search_all")
async def search_all_callback(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    
    await force_cleanup_user(user_id, db)
    
    if db.check_banned(user_id):
        await callback.answer("❌ Вы заблокированы", show_alert=True)
        return
    
    search_mode[user_id] = 'any'
    await start_searching(callback.message, mode='any')
    await callback.answer()

@dp.callback_query(F.data == "search_district")
async def search_district_callback(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    
    await force_cleanup_user(user_id, db)
    
    if db.check_banned(user_id):
        await callback.answer("❌ Вы заблокированы", show_alert=True)
        return
    
    search_mode[user_id] = 'district'
    await start_searching(callback.message, mode='district')
    await callback.answer()

async def start_searching(message: types.Message, mode='any'):
    # Получаем user_id из message
    if hasattr(message, 'from_user') and message.from_user:
        user_id = message.from_user.id
    elif hasattr(message, 'chat'):
        user_id = message.chat.id
    else:
        logger.error(f"Cannot get user_id from message: {message}")
        return
    
    # Проверка на бота
    if is_bot(user_id):
        return
    
    # Добавляем отладочное сообщение
    logger.info(f"start_searching called for user {user_id} with mode {mode}")
    
    user = db.get_user(user_id)
    
    if user is None:
        logger.error(f"User {user_id} not found in DB")
        await message.answer(
            "❌ Ошибка: пользователь не найден. Нажмите /start для регистрации.",
            reply_markup=kb.main_menu()
        )
        return
    
    if db.check_banned(user_id):
        await message.edit_text(
            "❌ Вы заблокированы и не можете искать собеседника.",
            reply_markup=kb.main_menu()
        )
        return
    
    logger.info(f"User {user_id} ({user['nickname']}) starts searching in mode: {mode}")
    
    
    # Обновляем онлайн статус
    db.update_online_status(user_id, True)
    
    # Очищаем очередь от забаненных
    valid_waiting = []
    for uid in waiting_users:
        if not db.check_banned(uid) and not is_bot(uid):
            u = db.get_user(uid)
            if u:
                valid_waiting.append(uid)
    
    waiting_users[:] = valid_waiting
    
    # Убираем себя из очереди, если уже там
    if user_id in waiting_users:
        waiting_users.remove(user_id)
    
    logger.info(f"Current waiting users: {len(waiting_users)}")
    
    # Ищем собеседника
    partner_id = None
    partner_index = -1
    
    if waiting_users:
        if mode == 'district':
            # Ищем в своем районе
            for i, uid in enumerate(waiting_users):
                if uid == user_id:
                    continue
                    
                partner_check = db.get_user(uid)
                if not partner_check:
                    continue
                    
                if (partner_check['district'] == user['district'] and
                    not db.check_banned(uid) and
                    not db.is_blocked(user_id, uid) and
                    not db.is_blocked(uid, user_id)):
                    
                    partner_id = uid
                    partner_index = i
                    logger.info(f"Found district match: {uid}")
                    break
        else:
            # Ищем по всей Тюмени
            for i, uid in enumerate(waiting_users):
                if uid == user_id:
                    continue
                    
                partner_check = db.get_user(uid)
                if not partner_check:
                    continue
                    
                if (not db.check_banned(uid) and
                    not db.is_blocked(user_id, uid) and
                    not db.is_blocked(uid, user_id)):
                    
                    partner_id = uid
                    partner_index = i
                    logger.info(f"Found any match: {uid}")
                    break
    
    if partner_id is not None and partner_index >= 0:
        # Нашли собеседника
        waiting_users.pop(partner_index)
        logger.info(f"Removed partner {partner_id} from queue")
        
        partner = db.get_user(partner_id)
        if not partner:
            logger.error(f"Partner {partner_id} not found in DB")
            await message.edit_text(
                "❌ Ошибка при поиске собеседника. Попробуйте снова.",
                reply_markup=kb.main_menu()
            )
            return
        
        # Создаем уникальный ID чата
        chat_uuid = f"{min(user_id, partner_id)}_{max(user_id, partner_id)}_{datetime.datetime.now().timestamp()}"
        
        # Определяем район для статистики
        chat_district = user['district'] if user['district'] == partner['district'] else 'разные районы'
        
        try:
            db.create_chat(chat_uuid, user_id, partner_id, user['nickname'], partner['nickname'], chat_district)
            logger.info(f"Chat created: {chat_uuid}")
        except Exception as e:
            logger.error(f"Error creating chat: {e}")
        
        active_chats[user_id] = partner_id
        active_chats[partner_id] = user_id
        active_chat_ids[user_id] = chat_uuid
        active_chat_ids[partner_id] = chat_uuid
        
        bot_stats["total_chats"] += 1
        bot_stats["active_chats"] = len(active_chats) // 2
        bot_stats["online_users"] = len(set(active_chats.keys()) | set(waiting_users))
        
        # Отправляем уведомления
        try:
            if user['district'] == partner['district']:
                district_info_user = f"\n📍 Вы оба из {user['district']}!"
                district_info_partner = f"\n📍 Вы оба из {partner['district']}!"
            else:
                district_info_user = f"\n📍 Ты из {user['district']}, собеседник из {partner['district']}"
                district_info_partner = f"\n📍 Ты из {partner['district']}, собеседник из {user['district']}"
            
            await bot.send_message(
                user_id,
                f"🔔 <b>Собеседник найден!</b>\n\n"
                f"Ты общаешься с: {partner['nickname']}{district_info_user}\n\n"
                f"Можете начинать общение!",
                reply_markup=kb.chat_actions()
            )
            
            await bot.send_message(
                partner_id,
                f"🔔 <b>Собеседник найден!</b>\n\n"
                f"Ты общаешься с: {user['nickname']}{district_info_partner}\n\n"
                f"Можете начинать общение!",
                reply_markup=kb.chat_actions()
            )
            
            logger.info(f"Both users notified: {user_id} and {partner_id}")
        except Exception as e:
            logger.error(f"Error notifying users: {e}")
        
        try:
            await message.delete()
        except:
            pass
    else:
        # Никого не нашли - встаем в очередь
        if user_id not in waiting_users:
            waiting_users.append(user_id)
            logger.info(f"Added {user_id} to queue")
        
        queue_position = len(waiting_users)
        bot_stats["online_users"] = len(set(active_chats.keys()) | set(waiting_users))
        
        mode_text = "по всей Тюмени" if mode == 'any' else f"в районе {user['district']}"
        
        # Создаем клавиатуру для отмены
        cancel_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить поиск", callback_data="cancel_search")]
        ])
        
        await message.edit_text(
            f"⏳ <b>Поиск собеседника {mode_text}...</b>\n\n"
            f"Позиция в очереди: {queue_position}\n"
            f"Всего в очереди: {len(waiting_users)}\n\n"
            f"Ожидайте, как только появится свободный собеседник - вы сразу соединитесь",
            reply_markup=cancel_keyboard
        )

@dp.callback_query(F.data == "cancel_search")
async def cancel_search(callback: types.CallbackQuery, state: FSMContext):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    
    if user_id in waiting_users:
        waiting_users.remove(user_id)
        db.update_online_status(user_id, False)
    
    if user_id in search_mode:
        del search_mode[user_id]
    
    await callback.message.edit_text(
        "❌ Поиск отменен.",
        reply_markup=kb.main_menu()
    )
    await state.clear()
    await callback.answer()

# ========== ЗАВЕРШЕНИЕ ЧАТА ==========
@dp.callback_query(F.data == "stop")
async def stop_chat_callback(callback: types.CallbackQuery, state: FSMContext):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    
    if user_id not in active_chats:
        if user_id in waiting_users:
            waiting_users.remove(user_id)
            db.update_online_status(user_id, False)
            await callback.message.edit_text(
                "✅ Ты удален из очереди поиска.",
                reply_markup=kb.main_menu()
            )
        else:
            await callback.answer("❌ Ты не в чате", show_alert=True)
        return
    
    await stop_chat(user_id, callback.message.chat.id, state, initiated_by=user_id)
    await callback.answer()

async def stop_chat(user_id: int, chat_id: int, state: FSMContext, initiated_by: int):
    partner_id = active_chats.get(user_id)
    
    if not partner_id:
        await bot.send_message(chat_id, "❌ Чат не найден.", reply_markup=kb.main_menu())
        return
    
    if partner_id not in active_chats or active_chats.get(partner_id) != user_id:
        if user_id in active_chats:
            del active_chats[user_id]
            db.update_online_status(user_id, False)
            
            if user_id in active_chat_ids:
                db.end_chat(active_chat_ids[user_id])
                del active_chat_ids[user_id]
        
        await bot.send_message(chat_id, "✅ Чат завершен.", reply_markup=kb.main_menu())
        return
    
    user = db.get_user(user_id)
    partner = db.get_user(partner_id)
    user_nick = user['nickname'] if user else 'Собеседник'
    partner_nick = partner['nickname'] if partner else 'Собеседник'
    
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
    
    bot_stats["active_chats"] = len(active_chats) // 2
    bot_stats["online_users"] = len(set(active_chats.keys()) | set(waiting_users))
    
    try:
        await send_temp_message(
            user_id,
            "✅ Ты завершил чат.",
            reply_markup=kb.main_menu(),
            delete_after=5
        )
        
        await bot.send_message(
            partner_id,
            f"❌ {user_nick} покинул чат.\n\n"
            f"Хочешь найти нового собеседника?",
            reply_markup=kb.main_menu()
        )
    except Exception as e:
        logger.error(f"Error notifying users: {e}")
    
    # Предлагаем оценить друг друга
    if user and not db.check_banned(user_id):
        await bot.send_message(
            user_id,
            f"👤 Как тебе общение с {partner_nick}?\n"
            f"Оцени собеседника:",
            reply_markup=kb.rating_keyboard(partner_id)
        )
    
    if partner and not db.check_banned(partner_id):
        await bot.send_message(
            partner_id,
            f"👤 Как тебе общение с {user_nick}?\n"
            f"Оцени собеседника:",
            reply_markup=kb.rating_keyboard(user_id)
        )
    
    await state.clear()

# ========== ОБРАБОТКА ЛАЙКОВ/ДИЗЛАЙКОВ ==========
@dp.callback_query(F.data.startswith(('like_', 'dislike_')))
async def process_rating(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    
    data_parts = callback.data.split('_')
    rating_type = data_parts[0]
    partner_id = int(data_parts[1])
    
    if db.check_banned(user_id):
        await callback.answer("❌ Вы заблокированы", show_alert=True)
        return
    
    partner = db.get_user(partner_id)
    if not partner:
        await callback.answer("❌ Собеседник не найден", show_alert=True)
        return
    
    is_like = (rating_type == "like")
    db.update_rating(partner_id, is_like)
    
    if is_like:
        await callback.message.edit_text(
            f"👍 Ты поставил лайк пользователю {partner['nickname']}!\n\n"
            f"Спасибо за оценку!",
            reply_markup=kb.main_menu()
        )
    else:
        await callback.message.edit_text(
            f"👎 Ты поставил дизлайк пользователю {partner['nickname']}.\n\n"
            f"Спасибо за обратную связь!",
            reply_markup=kb.main_menu()
        )
    
    await callback.answer()

# ========== ТОП РЕЙТИНГ ==========
@dp.callback_query(F.data == "top_rating")
async def show_top_rating(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    
    if db.check_banned(user_id):
        await callback.answer("❌ Вы заблокированы", show_alert=True)
        return
    
    top_users = db.get_top_users(10)
    
    if not top_users:
        await callback.message.edit_text(
            "🏆 Пока нет данных для рейтинга.\n\n"
            "Будь первым, кто получит оценки!",
            reply_markup=kb.main_menu()
        )
        await callback.answer()
        return
    
    text = "🏆 <b>Топ 10 пользователей по количеству лайков:</b>\n\n"
    
    for i, user in enumerate(top_users, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        level = get_user_rating_level(user['rating'])
        text += f"{medal} {user['nickname']} ({user['district']})\n"
        text += f"   👍 {user['likes']} лайков | 👎 {user['dislikes']} дизлайков\n"
        text += f"   Рейтинг: {user['rating']:.1f}% ({level})\n\n"
    
    await callback.message.edit_text(text, reply_markup=kb.main_menu())
    await callback.answer()

# ========== НАСТРОЙКИ ==========
@dp.callback_query(F.data == "settings")
async def show_settings(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    
    if db.check_banned(user_id):
        await callback.answer("❌ Вы заблокированы", show_alert=True)
        return
    
    user = db.get_user(user_id)
    if user is None:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return
    
    anon_status = "🕵️ Включен" if user['anon_mode'] else "👁️ Выключен"
    
    text = (
        f"⚙️ <b>Настройки</b>\n\n"
        f"👤 Твой ник: <b>{user['nickname']}</b>\n"
        f"🏘️ Район: {user['district']}\n"
        f"🕵️ Анонимный режим: {anon_status}\n\n"
        f"<i>В анонимном режиме собеседник видит только твой ник</i>\n"
        f"<i>Если выключить - будет видно имя из Telegram</i>"
    )
    
    await callback.message.edit_text(text, reply_markup=kb.settings_menu())
    await callback.answer()

@dp.callback_query(F.data == "change_nick")
async def change_nick(callback: types.CallbackQuery, state: FSMContext):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    
    if db.check_banned(user_id):
        await callback.answer("❌ Вы заблокированы", show_alert=True)
        return
    
    await callback.message.edit_text(
        "✏️ <b>Смена ника</b>\n\n"
        "Введи новый ник (до 20 символов):",
        reply_markup=kb.cancel_keyboard()
    )
    await state.set_state(States.changing_nick)
    await callback.answer()

@dp.message(States.changing_nick)
async def process_nick_change(message: types.Message, state: FSMContext):
    if is_bot(message.from_user.id):
        return
    
    user_id = message.from_user.id
    
    if db.check_banned(user_id):
        await message.answer("❌ Вы заблокированы")
        await state.clear()
        return
    
    new_nick = message.text.strip()
    
    if len(new_nick) > 20:
        await message.answer(
            "❌ Ник слишком длинный! Максимум 20 символов.\n"
            "Попробуй еще раз:",
            reply_markup=kb.cancel_keyboard()
        )
        return
    
    if len(new_nick) < 2:
        await message.answer(
            "❌ Ник слишком короткий! Минимум 2 символа.\n"
            "Попробуй еще раз:",
            reply_markup=kb.cancel_keyboard()
        )
        return
    
    user = db.get_user(user_id)
    if user:
        old_nick = user['nickname']
        db.update_nickname(user_id, new_nick)
        
        user = db.get_user(user_id)
        anon_status = "🕵️ Включен" if user['anon_mode'] else "👁️ Выключен"
        
        text = (
            f"✅ Ник успешно изменен!\n\n"
            f"Старый ник: {old_nick}\n"
            f"Новый ник: <b>{new_nick}</b>\n\n"
            f"⚙️ <b>Настройки</b>\n\n"
            f"👤 Твой ник: <b>{user['nickname']}</b>\n"
            f"🕵️ Анонимный режим: {anon_status}"
        )
        
        await message.answer(text, reply_markup=kb.settings_menu())
    
    await state.clear()

@dp.callback_query(F.data == "change_district")
async def change_district_callback(callback: types.CallbackQuery, state: FSMContext):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    
    if db.check_banned(user_id):
        await callback.answer("❌ Вы заблокированы", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🏘️ <b>Выбери новый район</b>",
        reply_markup=kb.change_district_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("change_district_"))
async def change_district_select(callback: types.CallbackQuery, state: FSMContext):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    district_index = int(callback.data.split("_")[2]) - 1
    selected_district = TYUMEN_DISTRICTS[district_index]
    
    user = db.get_user(user_id)
    if user:
        old_district = user['district']
        db.update_user_district(user_id, selected_district)
        await callback.answer(f"✅ Район изменен на {selected_district}", show_alert=True)
        
        await callback.message.edit_text(
            f"🏘️ Район успешно изменен!\n"
            f"Был: {old_district}\n"
            f"Стал: {selected_district}",
            reply_markup=kb.settings_menu()
        )
    await state.clear()

@dp.callback_query(F.data == "toggle_anon")
async def toggle_anon(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    
    if db.check_banned(user_id):
        await callback.answer("❌ Вы заблокированы", show_alert=True)
        return
    
    db.toggle_anon_mode(user_id)
    user = db.get_user(user_id)
    if user:
        anon_status = "🕵️ Включен" if user['anon_mode'] else "👁️ Выключен"
        
        await callback.message.edit_text(
            f"⚙️ <b>Настройки</b>\n\n"
            f"👤 Твой ник: <b>{user['nickname']}</b>\n"
            f"🏘️ Район: {user['district']}\n"
            f"🕵️ Анонимный режим: {anon_status}\n\n"
            f"<i>Режим изменен</i>",
            reply_markup=kb.settings_menu()
        )
    await callback.answer()

# ========== ЧЕРНЫЙ СПИСОК ==========
@dp.callback_query(F.data == "blacklist")
async def blacklist_menu_callback(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    
    if db.check_banned(user_id):
        await callback.answer("❌ Вы заблокированы", show_alert=True)
        return
    
    blacklist = db.get_blacklist(user_id)
    blacklist_count = len(blacklist)
    
    text = (
        f"🚫 <b>Черный список</b>\n\n"
        f"Всего заблокировано: {blacklist_count} чел.\n\n"
        f"Здесь ты можешь управлять списком людей, "
        f"с которыми не хочешь общаться."
    )
    
    await callback.message.edit_text(text, reply_markup=kb.blacklist_menu())
    await callback.answer()

@dp.callback_query(F.data == "show_blacklist")
async def show_blacklist(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    
    if db.check_banned(user_id):
        await callback.answer("❌ Вы заблокированы", show_alert=True)
        return
    
    blacklist = db.get_blacklist(user_id)
    
    if not blacklist:
        await callback.message.edit_text(
            "📋 Твой черный список пуст.\n\n"
            "Чтобы добавить человека в ЧС, нажми '🚫 В ЧС' после чата с ним.",
            reply_markup=kb.blacklist_menu()
        )
        await callback.answer()
        return
    
    keyboard_buttons = []
    for blocked in blacklist:
        keyboard_buttons.append([
            InlineKeyboardButton(
                text=f"❌ {blocked['nickname']} (рейтинг: {blocked['rating']:.1f}%)", 
                callback_data=f"blacklist_remove_{blocked['blocked_id']}"
            )
        ])
    
    keyboard_buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="blacklist")])
    
    await callback.message.edit_text(
        "🚫 <b>Твой черный список:</b>\n\n"
        "Нажми на пользователя, чтобы убрать его из ЧС:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("blacklist_add_"))
async def blacklist_add(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    target_id = int(callback.data.replace("blacklist_add_", ""))
    
    if db.check_banned(user_id):
        await callback.answer("❌ Вы заблокированы", show_alert=True)
        return
    
    if user_id == target_id:
        await callback.answer("❌ Нельзя добавить себя в ЧС", show_alert=True)
        return
    
    db.add_to_blacklist(user_id, target_id)
    
    target = db.get_user(target_id)
    target_nick = target['nickname'] if target else 'Пользователь'
    
    await callback.message.edit_text(
        f"✅ Пользователь {target_nick} добавлен в черный список.\n\n"
        f"Теперь вы не будете с ним соединяться в чате.",
        reply_markup=kb.main_menu()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("blacklist_remove_"))
async def blacklist_remove(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    target_id = int(callback.data.replace("blacklist_remove_", ""))
    
    if db.check_banned(user_id):
        await callback.answer("❌ Вы заблокированы", show_alert=True)
        return
    
    db.remove_from_blacklist(user_id, target_id)
    await callback.answer(f"✅ Пользователь удален из черного списка", show_alert=False)
    await show_blacklist(callback)

# ========== МЕНЮ ==========
@dp.callback_query(F.data == "menu")
async def back_to_menu(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    user_id = callback.from_user.id
    
    await force_cleanup_user(user_id, db)
    
    if db.check_banned(user_id):
        await callback.answer("❌ Вы заблокированы", show_alert=True)
        return
    
    await delete_bot_messages(user_id)
    await show_main_menu(callback.message, user_id)
    await callback.answer()

@dp.callback_query(F.data == "cancel")
async def cancel_action(callback: types.CallbackQuery, state: FSMContext):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    await state.clear()
    user_id = callback.from_user.id
    
    await force_cleanup_user(user_id, db)
    
    if db.check_banned(user_id):
        await callback.answer("❌ Вы заблокированы", show_alert=True)
        return
    
    await delete_bot_messages(user_id)
    await show_main_menu(callback.message, user_id)
    await callback.answer()

# ========== ПЕРЕСЫЛКА СООБЩЕНИЙ ==========
@dp.message()
async def forward_message(message: types.Message, state: FSMContext):
    # Игнорируем сообщения от самого бота
    if is_bot(message.from_user.id):
        logger.debug(f"Ignoring message from bot itself")
        return
    
    user_id = message.from_user.id
    
    # Проверяем, не админ ли это в режиме рассылки
    if user_id in ADMIN_IDS and user_id in broadcast_data:
        logger.info(f"Admin {user_id} in broadcast mode, skipping")
        return
    
    # Проверяем состояние
    current_state = await state.get_state()
    if current_state is not None:
        logger.info(f"User {user_id} in state {current_state}, skipping")
        return
    
    # Проверяем на спам
    current_time = datetime.datetime.now().timestamp()
    if user_id in user_last_message and current_time - user_last_message[user_id] < 1:
        return
    user_last_message[user_id] = current_time
    
    # Пропускаем команды
    if message.text and message.text.startswith('/'):
        return
    
    # Проверяем, есть ли пользователь в БД
    user = db.get_user(user_id)
    if user is None:
        logger.info(f"User {user_id} not in DB, suggesting to register")
        await message.answer(
            "❌ Вы не зарегистрированы. Нажмите /start для регистрации."
        )
        return
    
    # Проверяем бан
    if db.check_banned(user_id):
        await message.answer("❌ Вы заблокированы.")
        return
    
    # Обновляем активность
    db.update_user_activity(user_id)
    db.update_daily_stats()
    bot_stats["total_messages"] += 1
    
    # Проверяем, в чате ли пользователь
    if user_id not in active_chats:
        if message.text or message.sticker or message.photo:
            error_msg = await message.answer("❌ Ты не в чате. Найди собеседника через меню")
            asyncio.create_task(utils.delete_message_after(error_msg.chat.id, error_msg.message_id, 5))
        return
    
    # Получаем партнера
    partner_id = active_chats[user_id]
    
    # Проверяем партнера
    if partner_id not in active_chats:
        if user_id in active_chats:
            del active_chats[user_id]
        if user_id in active_chat_ids:
            db.end_chat(active_chat_ids[user_id])
            del active_chat_ids[user_id]
        db.update_online_status(user_id, False)
        
        error_msg = await message.answer("❌ Связь с собеседником потеряна. Чат завершен.")
        asyncio.create_task(utils.delete_message_after(error_msg.chat.id, error_msg.message_id, 5))
        return
    
    # Проверяем взаимность
    if active_chats.get(partner_id) != user_id:
        if user_id in active_chats:
            del active_chats[user_id]
        if user_id in active_chat_ids:
            db.end_chat(active_chat_ids[user_id])
            del active_chat_ids[user_id]
        db.update_online_status(user_id, False)
        
        error_msg = await message.answer("❌ Ошибка чата. Чат завершен.")
        asyncio.create_task(utils.delete_message_after(error_msg.chat.id, error_msg.message_id, 5))
        return
    
    # Проверяем, не забанен ли партнер
    if db.check_banned(partner_id):
        await force_cleanup_user(user_id, db)
        await force_cleanup_user(partner_id, db)
        
        error_msg = await message.answer("❌ Собеседник был заблокирован. Чат завершен.")
        asyncio.create_task(utils.delete_message_after(error_msg.chat.id, error_msg.message_id, 5))
        return
    
    partner = db.get_user(partner_id)
    if not partner:
        logger.error(f"Partner {partner_id} not found")
        return
    
    # Определяем имя отправителя
    if user['anon_mode']:
        sender_name = user['nickname']
    else:
        sender_name = message.from_user.full_name or "Пользователь"
        if message.from_user.username:
            sender_name += f" (@{message.from_user.username})"
    
    chat_uuid = active_chat_ids.get(user_id)
    
    # Отправляем сообщение
    try:
        if message.text:
            await bot.send_message(
                partner_id,
                f"<b>{sender_name}:</b> {message.text}"
            )
            if chat_uuid:
                try:
                    db.save_message(chat_uuid, user_id, partner_id, sender_name, partner['nickname'], message.text, "text")
                except Exception as e:
                    logger.error(f"Error saving message: {e}")
        
        elif message.sticker:
            await bot.send_sticker(partner_id, message.sticker.file_id)
            if chat_uuid:
                try:
                    db.save_message(chat_uuid, user_id, partner_id, sender_name, partner['nickname'], None, "sticker", message.sticker.file_id)
                except Exception as e:
                    logger.error(f"Error saving sticker: {e}")
        
        elif message.photo:
            caption = f"<b>{sender_name}:</b> {message.caption or '📸 Фото'}"
            await bot.send_photo(partner_id, message.photo[-1].file_id, caption=caption)
            if chat_uuid:
                try:
                    db.save_message(chat_uuid, user_id, partner_id, sender_name, partner['nickname'], message.caption, "photo", message.photo[-1].file_id)
                except Exception as e:
                    logger.error(f"Error saving photo: {e}")
        
        elif message.voice:
            await bot.send_voice(partner_id, message.voice.file_id)
            if chat_uuid:
                try:
                    db.save_message(chat_uuid, user_id, partner_id, sender_name, partner['nickname'], None, "voice", message.voice.file_id)
                except Exception as e:
                    logger.error(f"Error saving voice: {e}")
        else:
            error_msg = await message.answer("❌ Этот тип сообщений не поддерживается")
            asyncio.create_task(utils.delete_message_after(error_msg.chat.id, error_msg.message_id, 5))
            
    except TelegramBadRequest as e:
        logger.error(f"Telegram error: {e}")
        if "bot was blocked" in str(e):
            await force_cleanup_user(user_id, db)
            await force_cleanup_user(partner_id, db)
            error_msg = await message.answer("❌ Собеседник заблокировал бота. Чат завершен.")
            asyncio.create_task(utils.delete_message_after(error_msg.chat.id, error_msg.message_id, 5))
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

# ========== АДМИН ПАНЕЛЬ ==========

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    await cleanup_invalid_chats(db)
    stats = db.get_all_stats()
    
    uptime = datetime.datetime.now() - bot_stats["start_time"]
    days = uptime.days
    hours = uptime.seconds // 3600
    minutes = (uptime.seconds // 60) % 60
    
    active_users = len(set(active_chats.keys()) | set(waiting_users))
    
    # Получаем статистику по районам
    district_stats = db.get_district_stats()
    district_text = ""
    for stat in district_stats[:5]:
        district_text += f"\n  {stat['district']}: {stat['online_now']} онлайн"
    
    text = (
        "👑 <b>Панель администратора</b>\n\n"
        f"📊 <b>Основные показатели:</b>\n"
        f"• Всего пользователей: {stats['total_users']}\n"
        f"• Забанено: {stats['banned_users']}\n"
        f"• Активных сегодня: {stats['active_today']}\n"
        f"• Сейчас онлайн: {active_users}\n"
        f"• В очереди: {len(waiting_users)}\n"
        f"• Активных чатов: {len(active_chats) // 2}\n\n"
        f"📈 <b>Активность:</b>\n"
        f"• Сообщений всего: {stats['total_messages']}\n"
        f"• Чатов всего: {stats['total_chats']}\n"
        f"• Записей в ЧС: {stats['total_blacklists']}\n\n"
        f"🗺️ <b>Топ районов:</b>{district_text}\n\n"
        f"⏰ Бот работает: {days}д {hours}ч {minutes}м"
    )
    
    await callback.message.edit_text(text, reply_markup=kb.admin_menu())
    await callback.answer()

@dp.callback_query(F.data == "admin_online")
async def admin_online(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    await cleanup_invalid_chats(db)
    online_users = set(active_chats.keys()) | set(waiting_users)
    
    if not online_users:
        await callback.message.edit_text(
            "👥 Сейчас нет онлайн пользователей",
            reply_markup=kb.admin_menu()
        )
        await callback.answer()
        return
    
    # Группируем по статусу
    in_chats = []
    in_queue = []
    
    for uid in online_users:
        user = db.get_user(uid)
        if user:
            user_info = f"• {user['nickname']} (ID: {uid})\n  Район: {user['district']}, Рейтинг: {user['rating']:.1f}%"
            if uid in active_chats:
                partner_id = active_chats[uid]
                partner = db.get_user(partner_id)
                if partner:
                    user_info += f"\n  💬 с: {partner['nickname']}"
                in_chats.append(user_info)
            else:
                in_queue.append(user_info)
    
    text = "👥 <b>Онлайн пользователи:</b>\n\n"
    
    if in_chats:
        text += f"💬 <b>В чатах ({len(in_chats)}):</b>\n" + "\n\n".join(in_chats[:10]) + "\n\n"
    
    if in_queue:
        text += f"⏳ <b>В очереди ({len(in_queue)}):</b>\n" + "\n\n".join(in_queue[:10])
    
    if len(online_users) > 20:
        text += f"\n\n... и ещё {len(online_users) - 20} пользователей"
    
    text += f"\n\nВсего онлайн: {len(online_users)}"
    
    await callback.message.edit_text(text, reply_markup=kb.admin_menu())
    await callback.answer()

@dp.callback_query(F.data == "admin_districts")
async def admin_districts(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    stats = db.get_district_stats()
    online_users = set(active_chats.keys()) | set(waiting_users)
    
    text = "🗺️ <b>Статистика по районам:</b>\n\n"
    
    for stat in stats:
        # Считаем реальных онлайн в районе
        real_online = 0
        for uid in online_users:
            user = db.get_user(uid)
            if user and user['district'] == stat['district']:
                real_online += 1
        
        text += f"{stat['district']}\n"
        text += f"   👥 Всего: {stat['user_count']}\n"
        text += f"   🟢 В БД: {stat['online_now']} | Реально: {real_online}\n\n"
    
    text += "\n<i>Если цифры отличаются, используйте /fix_online</i>"
    
    await callback.message.edit_text(text, reply_markup=kb.admin_menu())
    await callback.answer()

@dp.callback_query(F.data == "admin_search_district")
async def admin_search_district_callback(callback: types.CallbackQuery, state: FSMContext):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    districts_text = "\n".join([f"• {d}" for d in TYUMEN_DISTRICTS])
    
    await callback.message.edit_text(
        "🔍 <b>Поиск пользователей по району</b>\n\n"
        f"Введи название района:\n\n"
        f"<b>Доступные районы:</b>\n{districts_text}",
        reply_markup=kb.cancel_keyboard()
    )
    await state.set_state(States.admin_search_district)
    await callback.answer()

@dp.message(States.admin_search_district)
async def process_admin_search_district(message: types.Message, state: FSMContext):
    if is_bot(message.from_user.id):
        return
    
    admin_id = message.from_user.id
    search_text = message.text.strip()
    
    if admin_id not in ADMIN_IDS:
        await state.clear()
        return
    
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
    
    district = matching_districts[0]
    users = db.get_users_by_district(district)
    
    if not users:
        await message.answer(
            f"👥 В районе {district} пока нет пользователей",
            reply_markup=kb.admin_menu()
        )
        await state.clear()
        return
    
    # Считаем статистику
    total_users = len(users)
    
    online_users = set(active_chats.keys()) | set(waiting_users)
    
    text = f"🏘️ <b>Район: {district}</b>\n\n"
    text += f"👥 Всего пользователей: {total_users}\n"
    text += f"🟢 Сейчас онлайн: {len([u for u in users if u[0] in online_users])}\n"
    text += f"🚫 Забанено: {len([u for u in users if u[9]])}\n\n"
    text += f"<b>Список пользователей:</b>\n\n"
    
    for user in users[:30]:
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

@dp.callback_query(F.data == "admin_search_messages")
async def admin_search_messages_callback(callback: types.CallbackQuery, state: FSMContext):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🔍 <b>Поиск сообщений</b>\n\n"
        "Введи текст для поиска в сообщениях:",
        reply_markup=kb.cancel_keyboard()
    )
    await state.set_state(States.admin_search_messages)
    await callback.answer()

@dp.message(States.admin_search_messages)
async def process_admin_search_messages(message: types.Message, state: FSMContext):
    if is_bot(message.from_user.id):
        return
    
    admin_id = message.from_user.id
    search_text = message.text.strip()
    
    if admin_id not in ADMIN_IDS:
        await state.clear()
        return
    
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
            if isinstance(msg, sqlite3.Row):
                time = msg['timestamp'][:16] if msg['timestamp'] else "неизвестно"
                from_nick = msg['from_nick']
                to_nick = msg['to_nick']
                msg_text = msg['message_text']
                if msg_text and len(msg_text) > 50:
                    msg_text = msg_text[:50] + "..."
            else:
                time = msg[10][:16] if len(msg) > 10 and msg[10] else "неизвестно"
                from_nick = msg[3] if len(msg) > 3 else "?"
                to_nick = msg[4] if len(msg) > 4 else "?"
                msg_text = msg[5] if len(msg) > 5 and msg[5] else ""
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
    
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
            await message.answer(part, reply_markup=kb.admin_menu())
    else:
        await message.answer(text, reply_markup=kb.admin_menu())
    
    await state.clear()

@dp.callback_query(F.data == "admin_daily")
async def admin_daily(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    stats = db.get_all_stats()
    
    text = "📈 <b>Статистика по дням:</b>\n\n"
    
    for day in stats['daily_stats'][:7]:
        text += f"<b>{day['date']}:</b>\n"
        text += f"• Сообщений: {day['total_messages']}\n"
        text += f"• Чатов: {day['total_chats']}\n"
        text += f"• Новых: {day['new_users']}\n"
        text += f"• Активных: {day['active_users']}\n\n"
    
    await callback.message.edit_text(text, reply_markup=kb.admin_menu())
    await callback.answer()

@dp.callback_query(F.data == "admin_user_details")
async def admin_user_details(callback: types.CallbackQuery, state: FSMContext):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    await callback.message.edit_text(
        "👤 <b>Поиск пользователя</b>\n\n"
        "Введи <b>ID</b> или <b>ник</b> пользователя для просмотра:",
        reply_markup=kb.cancel_keyboard()
    )
    await state.set_state(States.admin_get_user)
    await callback.answer()

@dp.message(States.admin_get_user)
async def process_admin_get_user(message: types.Message, state: FSMContext):
    if is_bot(message.from_user.id):
        return
    
    admin_id = message.from_user.id
    search_text = message.text.strip()
    
    if admin_id not in ADMIN_IDS:
        await state.clear()
        return
    
    # Пробуем найти по ID
    try:
        target_id = int(search_text)
        user = db.get_user_details(target_id)
        if user:
            users = [user]
        else:
            users = []
    except ValueError:
        # Ищем по нику
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT u.*, r.likes, r.dislikes, r.rating, r.banned, r.ban_date, r.ban_reason
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
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_user_details"),
         InlineKeyboardButton(text="◀️ В админку", callback_data="admin_menu")]
    ])
    
    await message.answer(text, reply_markup=keyboard)
    await state.clear()

@dp.callback_query(F.data.startswith("admin_ban_"))
async def admin_ban_user(callback: types.CallbackQuery, state: FSMContext):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    admin_id = callback.from_user.id
    target_id = int(callback.data.replace("admin_ban_", ""))
    
    if admin_id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"🔨 <b>Бан пользователя {target_id}</b>\n\n"
        f"Введи причину бана:",
        reply_markup=kb.cancel_keyboard()
    )
    await state.update_data(ban_target=target_id)
    await state.set_state(States.admin_broadcast)

@dp.callback_query(F.data.startswith("admin_unban_"))
async def admin_unban_user(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    admin_id = callback.from_user.id
    target_id = int(callback.data.replace("admin_unban_", ""))
    
    if admin_id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    db.unban_user(target_id)
    db.log_admin_action(admin_id, "unban", target_id, "Разбанен администратором")
    
    await callback.answer(f"✅ Пользователь {target_id} разбанен", show_alert=True)
    
    # Возвращаемся к поиску
    await admin_user_details(callback, None)

@dp.callback_query(F.data == "admin_bans")
async def admin_bans(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    banned_users = db.get_banned_users()
    
    if not banned_users:
        await callback.message.edit_text(
            "✅ Нет забаненных пользователей",
            reply_markup=kb.admin_menu()
        )
        await callback.answer()
        return
    
    text = "🔨 <b>Забаненные пользователи:</b>\n\n"
    
    for user in banned_users[:20]:
        text += f"• {user['nickname']} ({user['district']})\n"
        text += f"  ID: {user['user_id']}\n"
        text += f"  Рейтинг: {user['rating']:.1f}%, 👍 {user['likes']} 👎 {user['dislikes']}\n"
        text += f"  Забанен: {user['ban_date'][:16]}\n"
        if user['ban_reason']:
            text += f"  Причина: {user['ban_reason']}\n"
        text += "\n"
    
    await callback.message.edit_text(text, reply_markup=kb.admin_menu())
    await callback.answer()

# ========== РАССЫЛКА ==========

@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    # Устанавливаем флаг, что ждем текст рассылки
    broadcast_data[callback.from_user.id] = "waiting"
    
    await callback.message.answer(
        "📤 <b>Рассылка</b>\n\n"
        "Отправь текст для рассылки (или /cancel для отмены):"
    )
    await callback.answer()

@dp.message(Command("cancel"))
async def cancel_broadcast(message: types.Message):
    if is_bot(message.from_user.id):
        return
    
    if message.from_user.id not in ADMIN_IDS:
        return
    
    # Очищаем флаг
    if message.from_user.id in broadcast_data:
        del broadcast_data[message.from_user.id]
    
    await message.answer("❌ Рассылка отменена", reply_markup=kb.admin_menu())

@dp.message(lambda message: message.from_user.id in ADMIN_IDS and 
            message.from_user.id in broadcast_data and 
            broadcast_data[message.from_user.id] == "waiting" and
            not message.text.startswith('/'))
async def handle_broadcast_text(message: types.Message):
    if is_bot(message.from_user.id):
        return
    
    admin_id = message.from_user.id
    broadcast_text = message.text
    
    # Меняем флаг на текст
    broadcast_data[admin_id] = broadcast_text
    
    # Создаем клавиатуру для подтверждения
    confirm_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Отправить", callback_data="broadcast_send"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast_cancel")
        ]
    ])
    
    await message.answer(
        f"📤 <b>Подтверждение рассылки</b>\n\n"
        f"Текст сообщения:\n{broadcast_text}\n\n"
        f"Отправить это сообщение всем пользователям?",
        reply_markup=confirm_keyboard
    )

@dp.callback_query(F.data == "broadcast_send")
async def broadcast_send(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    admin_id = callback.from_user.id
    
    if admin_id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    # Получаем сохраненный текст
    broadcast_text = broadcast_data.get(admin_id)
    if not broadcast_text or broadcast_text == "waiting":
        await callback.message.edit_text("❌ Ошибка: текст не найден")
        return
    
    await callback.message.edit_text("⏳ Начинаю рассылку...")
    
    # Получаем всех пользователей
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT user_id FROM users')
    users = cursor.fetchall()
    conn.close()
    
    sent = 0
    failed = 0
    
    for (uid,) in users:
        # Пропускаем забаненных
        if db.check_banned(uid):
            failed += 1
            continue
            
        try:
            await bot.send_message(
                uid,
                f"📢 <b>Сообщение от администрации</b>\n\n{broadcast_text}"
            )
            sent += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Failed to send to {uid}: {e}")
            failed += 1
    
    # Отправляем тестовое сообщение админу
    try:
        await bot.send_message(
            admin_id,
            f"📢 <b>Копия вашей рассылки</b>\n\n{broadcast_text}"
        )
    except:
        pass
    
    # Очищаем временное хранилище
    if admin_id in broadcast_data:
        del broadcast_data[admin_id]
    
    # Отправляем результат
    await callback.message.edit_text(
        f"✅ Рассылка завершена!\n\n"
        f"📨 Отправлено: {sent}\n"
        f"❌ Ошибок: {failed}\n"
        f"👥 Всего пользователей: {len(users)}",
        reply_markup=kb.admin_menu()
    )
    
    # Логируем
    db.log_admin_action(admin_id, "broadcast", details=f"Sent: {sent}, Failed: {failed}")
    await callback.answer()

@dp.callback_query(F.data == "broadcast_cancel")
async def broadcast_cancel(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    admin_id = callback.from_user.id
    
    if admin_id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    # Очищаем временное хранилище
    if admin_id in broadcast_data:
        del broadcast_data[admin_id]
    
    await callback.message.edit_text(
        "❌ Рассылка отменена",
        reply_markup=kb.admin_menu()
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_getdb")
async def admin_getdb(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    await callback.answer("⏳ Загружаю базу данных...")
    
    try:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"tyumenchat_backup_{timestamp}.db"
        shutil.copy2(db.db_name, backup_name)
        
        stats = db.get_all_stats()
        
        await callback.message.answer_document(
            FSInputFile(backup_name),
            caption=f"📊 База данных ТюменьChat\n"
                    f"📅 {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
                    f"👥 Пользователей: {stats['total_users']}\n"
                    f"💬 Сообщений: {stats['total_messages']}\n"
                    f"💫 Чатов: {stats['total_chats']}"
        )
        
        os.remove(backup_name)
        db.log_admin_action(callback.from_user.id, "download_db")
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {e}")

@dp.callback_query(F.data == "admin_logs")
async def admin_logs(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    logs = db.get_admin_logs(30)
    
    if not logs:
        await callback.message.edit_text(
            "📋 Логи администраторов пусты",
            reply_markup=kb.admin_menu()
        )
        await callback.answer()
        return
    
    text = "📋 <b>Последние действия администраторов:</b>\n\n"
    
    for log in logs[:20]:
        admin = db.get_user(log['admin_id'])
        admin_nick = admin['nickname'] if admin else str(log['admin_id'])
        time = log['timestamp'][:16]
        
        text += f"• [{time}] {admin_nick}\n"
        text += f"  Действие: {log['action']}\n"
        if log['target_id']:
            target = db.get_user(log['target_id'])
            target_nick = target['nickname'] if target else str(log['target_id'])
            text += f"  Цель: {target_nick}\n"
        if log['details']:
            text += f"  Детали: {log['details']}\n"
        text += "\n"
    
    if len(logs) > 20:
        text += f"... и ещё {len(logs) - 20} записей"
    
    await callback.message.edit_text(text, reply_markup=kb.admin_menu())
    await callback.answer()

@dp.callback_query(F.data == "admin_menu")
async def back_to_admin_menu(callback: types.CallbackQuery):
    if is_bot(callback.from_user.id):
        await callback.answer()
        return
    
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    await callback.message.edit_text(
        "👑 <b>Панель администратора</b>\n\n"
        "Выберите действие:",
        reply_markup=kb.admin_menu()
    )
    await callback.answer()



async def main():
    print("=" * 50)
    print("✅ ТюменьChat бот запущен!")
    print("=" * 50)
    print(f"📊 База данных: {db.db_name}")
    print(f"👑 Администраторы: {ADMIN_IDS}")
    print(f"🤖 ID бота: {bot.id}")
    print(f"🔧 Режим отладки: {DEBUG}")
    print("=" * 50)
    print("📢 Команды:")
    print("   /start - Главное меню")
    print("   /admin - Админ-панель")
    print("   /debug - Диагностика (админ)")
    print("   /fix_online - Исправить онлайн (админ)")
    print("   /myid - Узнать свой ID")
    print("=" * 50)
    
    async def periodic_cleanup():
        while True:
            await asyncio.sleep(60)
            await cleanup_invalid_chats(db)
            logger.info("Periodic cleanup completed")
    
    asyncio.create_task(periodic_cleanup())
    
    await dp.start_polling(bot)

# ========== ПРИНУДИТЕЛЬНЫЕ ОБРАБОТЧИКИ ==========

@dp.callback_query(F.data == "search_menu")
async def force_search_menu(callback: types.CallbackQuery):
    print("🔥 FORCE: search_menu нажата!")
    await callback.message.edit_text(
        "🔍 <b>Поиск собеседника</b>\n\nВыбери режим:",
        reply_markup=kb.search_menu_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "search_all")
async def force_search_all(callback: types.CallbackQuery):
    print("🔥 FORCE: search_all нажата!")
    user_id = callback.from_user.id
    
    # Проверяем регистрацию
    user = db.get_user(user_id)
    if not user:
        await callback.message.edit_text("❌ Сначала нажми /start", reply_markup=kb.main_menu())
        await callback.answer()
        return
    
    # Просто отправляем сообщение
    await callback.message.edit_text(
        "🔍 <b>Поиск собеседника...</b>\n\n"
        "Ищем подходящего собеседника...\n"
        "Это может занять некоторое время.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="menu")]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data == "search_district")
async def force_search_district(callback: types.CallbackQuery):
    print("🔥 FORCE: search_district нажата!")
    user_id = callback.from_user.id
    
    # Проверяем регистрацию
    user = db.get_user(user_id)
    if not user:
        await callback.message.edit_text("❌ Сначала нажми /start", reply_markup=kb.main_menu())
        await callback.answer()
        return
    
    # Просто отправляем сообщение
    await callback.message.edit_text(
        f"🔍 <b>Поиск собеседника в вашем районе...</b>\n\n"
        f"Район: {user['district']}\n"
        f"Ищем подходящего собеседника...",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="menu")]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data == "cancel_search")
async def force_cancel_search(callback: types.CallbackQuery):
    print("🔥 FORCE: cancel_search нажата!")
    user_id = callback.from_user.id
    
    if user_id in waiting_users:
        waiting_users.remove(user_id)
        db.update_online_status(user_id, False)
    
    await callback.message.edit_text(
        "❌ Поиск отменен.",
        reply_markup=kb.main_menu()
    )
    await callback.answer()

@dp.callback_query(F.data == "menu")
async def force_menu(callback: types.CallbackQuery):
    print("🔥 FORCE: menu нажата!")
    await show_main_menu(callback.message, callback.from_user.id)
    await callback.answer()
if __name__ == "__main__":
    asyncio.run(main())