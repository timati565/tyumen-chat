import random
import datetime
import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Глобальные переменные (будут установлены из bot.py)
bot = None
chat_messages = {}
waiting_users = []
active_chats = {}
active_chat_ids = {}
search_mode = {}
bot_stats = {
    "total_messages": 0,
    "total_messages_today": 0,
    "total_chats": 0,
    "total_chats_today": 0,
    "active_chats": 0,
    "online_users": 0,
    "start_time": datetime.datetime.now(),
}

def set_bot(bot_instance):
    """Устанавливает экземпляр бота для использования в утилитах"""
    global bot
    bot = bot_instance

def generate_tyumen_nickname() -> str:
    """Генерирует тюменский ник"""
    adjectives = ["Сибирский", "Тюменский", "Набережный", "Мостовской", "Солнечный", 
                  "Гилевский", "Тарманский", "Калининский", "Центральный", "Речной",
                  "Нефтяной", "Студенческий", "Уютный", "Вечерний", "Активный"]
    nouns = ["Волк", "Лис", "Медведь", "Соболь", "Кедр", "Тура", "Мост", "Фонтан", 
             "Сквер", "Парк", "Студент", "Нефтяник", "Горожанин", "Сибиряк", "Тюменец"]
    return f"{random.choice(adjectives)} {random.choice(nouns)}"

def get_user_rating_level(rating: float) -> str:
    """Возвращает уровень рейтинга на основе процента"""
    if rating >= 90:
        return "🌟 Легенда Тюмени"
    elif rating >= 70:
        return "⭐ Почётный горожанин"
    elif rating >= 50:
        return "👍 Активный тюменец"
    elif rating >= 30:
        return "👌 Местный житель"
    elif rating >= 10:
        return "🤔 Гость города"
    else:
        return "👎 Нарушитель спокойствия"

async def save_message_id(user_id: int, message_id: int):
    """Сохраняет ID сообщения для последующего удаления"""
    if user_id not in chat_messages:
        chat_messages[user_id] = []
    chat_messages[user_id].append(message_id)
    if len(chat_messages[user_id]) > 50:
        chat_messages[user_id] = chat_messages[user_id][-50:]

async def delete_bot_messages(user_id: int):
    """Удаляет все сообщения бота для пользователя"""
    if user_id in chat_messages:
        for msg_id in chat_messages[user_id]:
            try:
                await bot.delete_message(user_id, msg_id)
            except:
                pass
        chat_messages[user_id] = []

async def delete_message_after(chat_id: int, message_id: int, seconds: int):
    """Удаляет сообщение через указанное количество секунд"""
    await asyncio.sleep(seconds)
    try:
        await bot.delete_message(chat_id, message_id)
    except:
        pass

async def send_temp_message(user_id: int, text: str, reply_markup=None, delete_after: int = None):
    """Отправляет временное сообщение (автоудаление)"""
    if not bot:
        logger.error("Bot instance not set in utils")
        return None
        
    msg = await bot.send_message(user_id, text, reply_markup=reply_markup)
    await save_message_id(user_id, msg.message_id)
    
    if delete_after:
        asyncio.create_task(delete_message_after(user_id, msg.message_id, delete_after))
    
    return msg

async def cleanup_invalid_chats(db):
    """Очищает невалидные чаты"""
    global bot_stats, waiting_users, active_chats, active_chat_ids
    
    to_remove = []
    for user_id, partner_id in list(active_chats.items()):
        if partner_id not in active_chats or active_chats.get(partner_id) != user_id:
            to_remove.append(user_id)
    
    for user_id in to_remove:
        if user_id in active_chats:
            logger.info(f"Cleaning up invalid chat for user {user_id}")
            # Завершаем чат в БД
            if user_id in active_chat_ids:
                db.end_chat(active_chat_ids[user_id])
                del active_chat_ids[user_id]
            
            db.update_online_status(user_id, False)
            del active_chats[user_id]
    
    bot_stats["active_chats"] = len(active_chats) // 2
    bot_stats["online_users"] = len(set(active_chats.keys()) | set(waiting_users))

async def force_cleanup_user(user_id: int, db):
    """Принудительно очищает пользователя из всех очередей и чатов"""
    global bot_stats, waiting_users, active_chats, active_chat_ids, search_mode
    
    # Обновляем онлайн статус
    was_online = user_id in waiting_users or user_id in active_chats
    if was_online:
        db.update_online_status(user_id, False)
    
    if user_id in waiting_users:
        waiting_users.remove(user_id)
    
    if user_id in active_chats:
        partner_id = active_chats.get(user_id)
        if partner_id in active_chats:
            # Завершаем чат в БД
            if partner_id in active_chat_ids:
                db.end_chat(active_chat_ids[partner_id])
                del active_chat_ids[partner_id]
            db.update_online_status(partner_id, False)
            del active_chats[partner_id]
        
        if user_id in active_chat_ids:
            db.end_chat(active_chat_ids[user_id])
            del active_chat_ids[user_id]
        
        del active_chats[user_id]
    
    if user_id in search_mode:
        del search_mode[user_id]
    
    bot_stats["active_chats"] = len(active_chats) // 2
    bot_stats["online_users"] = len(set(active_chats.keys()) | set(waiting_users))