from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from config import TYUMEN_DISTRICTS

def search_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌍 По всей Тюмени", callback_data="search_all")],
        [InlineKeyboardButton(text="🏘️ В моем районе", callback_data="search_district")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu")]
    ])

def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Найти собеседника", callback_data="search_menu")],
        [
            InlineKeyboardButton(text="🗺️ Районы Тюмени", callback_data="districts_menu"),
            InlineKeyboardButton(text="🏆 Топ рейтинг", callback_data="top_rating")
        ],
        [
            InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings"),
            InlineKeyboardButton(text="🚫 Черный список", callback_data="blacklist")
        ]
    ])

def districts_keyboard():
    buttons = []
    row = []
    for i, district in enumerate(TYUMEN_DISTRICTS, 1):
        row.append(InlineKeyboardButton(text=district, callback_data=f"district_{i}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def settings_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Сменить ник", callback_data="change_nick")],
        [InlineKeyboardButton(text="🏘️ Сменить район", callback_data="change_district")],
        [InlineKeyboardButton(text="🕵️ Анонимный режим", callback_data="toggle_anon")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu")]
    ])

def change_district_keyboard():
    buttons = []
    row = []
    for i, district in enumerate(TYUMEN_DISTRICTS, 1):
        row.append(InlineKeyboardButton(text=district, callback_data=f"change_district_{i}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="settings")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def blacklist_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Показать черный список", callback_data="show_blacklist")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu")]
    ])

def admin_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Полная статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="👥 Онлайн пользователи", callback_data="admin_online")],
        [InlineKeyboardButton(text="🗺️ Статистика районов", callback_data="admin_districts")],
        [InlineKeyboardButton(text="🔍 Поиск по району", callback_data="admin_search_district")],
        [InlineKeyboardButton(text="🔍 Поиск сообщений", callback_data="admin_search_messages")],
        [InlineKeyboardButton(text="📈 Статистика по дням", callback_data="admin_daily")],
        [InlineKeyboardButton(text="👤 Детали пользователя", callback_data="admin_user_details")],
        [InlineKeyboardButton(text="🔨 Управление банами", callback_data="admin_bans")],
        [InlineKeyboardButton(text="📤 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="📥 Скачать БД", callback_data="admin_getdb")],
        [InlineKeyboardButton(text="📋 Логи админов", callback_data="admin_logs")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu")]
    ])

def cancel_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])

def chat_actions():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚫 Завершить чат", callback_data="stop")]
    ])

def rating_keyboard(partner_id: int):
    """Клавиатура для оценки после чата"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👍 Лайк", callback_data=f"like_{partner_id}"),
            InlineKeyboardButton(text="👎 Дизлайк", callback_data=f"dislike_{partner_id}")
        ],
        [
            InlineKeyboardButton(text="🚫 В ЧС", callback_data=f"blacklist_add_{partner_id}"),
            InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_menu")
        ]
    ])