import asyncio
import logging
from aiohttp import ClientSession
from service.signal_service import market_add_database, users_list, add_symbol
from config_data.config import Config, load_config

logger = logging.getLogger(__name__)
config: Config = load_config('.env')


async def continuous_task_bybit(session, http_session):
    """
    Асинхронная задача для добавления рыночных данных в базу.
    """
    while True:
        try:
            await market_add_database(session, http_session)
        except Exception as e:
            logger.error(f"Error in continuous_task_bybit: {e}", exc_info=True)
        await asyncio.sleep(10)  # Задержка перед следующей итерацией


async def continuous_task_user(session, http_session):
    """
    Асинхронная задача для обработки пользователей.
    """
    while True:
        try:
            await users_list(session, http_session)
        except Exception as e:
            logger.error(f"Error in continuous_task_user: {e}", exc_info=True)
        await asyncio.sleep(10)  # Задержка перед следующей итерацией


async def continuous_task_symbol(session, http_session):
    """
    Асинхронная задача для обновления списка символов.
    """
    while True:
        try:
            await add_symbol(session, http_session)
        except Exception as e:
            logger.error(f"Error in continuous_task_symbol: {e}", exc_info=True)
        await asyncio.sleep(1200)  # 20 минут задержки


async def main():
    """
    Основная функция запуска задач.
    """
    logging.basicConfig(
        level=logging.INFO,
        filename=f'{__name__}.log',
        format='%(filename)s:%(lineno)d #%(levelname)-8s '
               '[%(asctime)s] - %(name)s - %(message)s'
    )
    logger.info('Starting bot')

    # Создаем асинхронные сессии для работы с HTTP-запросами
    async with ClientSession() as http_session:
        from pybit.unified_trading import HTTP  # Импортируем внутри, чтобы соответствовать контексту
        session = HTTP(
            testnet=False,
            api_key="your_bybit_api_key",
            api_secret="your_bybit_api_secret"
        )

        # Создание задач
        task_users = asyncio.create_task(continuous_task_user(session, http_session))
        task_bybit = asyncio.create_task(continuous_task_bybit(session, http_session))
        task_symbols = asyncio.create_task(continuous_task_symbol(session, http_session))

        # Ожидание завершения всех задач
        await asyncio.gather(task_users, task_bybit, task_symbols)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}", exc_info=True)