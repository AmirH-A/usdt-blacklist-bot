from aiogram import Bot, Dispatcher, Router

from ..config import BOT_TOKEN

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)
