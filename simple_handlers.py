# simple_handlers.py
from aiogram import F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import keyboards as kb

# Простые обработчики для теста
async def setup_simple_handlers(dp, bot, db, start_searching, show_main_menu):
    
    @dp.callback_query(F.data == "search_menu")
    async def test_search_menu(callback):
        print("✅ search_menu нажата!")
        await callback.message.edit_text(
            "🔍 Поиск собеседника\n\nВыбери режим:",
            reply_markup=kb.search_menu_keyboard()
        )
        await callback.answer()

    @dp.callback_query(F.data == "search_all")
    async def test_search_all(callback):
        print("✅ search_all нажата!")
        await callback.message.edit_text("🔍 Ищу по всей Тюмени...")
        await start_searching(callback.message, mode='any')
        await callback.answer()

    @dp.callback_query(F.data == "search_district")
    async def test_search_district(callback):
        print("✅ search_district нажата!")
        await callback.message.edit_text("🔍 Ищу в твоем районе...")
        await start_searching(callback.message, mode='district')
        await callback.answer()

    @dp.callback_query(F.data == "menu")
    async def test_menu(callback):
        print("✅ menu нажата!")
        await show_main_menu(callback.message, callback.from_user.id)
        await callback.answer()