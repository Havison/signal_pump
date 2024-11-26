import asyncio
import logging

from service.signal_service import market_add_database, users_list, add_symbol

logger = logging.getLogger(__name__)


async def countinues_taks_bybit():
    while True:
        await market_add_database()


async def countinues_task_user():
    while True:
        await users_list()


async def countinues_taks_symbol():
    while True:
        await add_symbol()
        await asyncio.sleep(1200)


async def main():
    logging.basicConfig(
        level=logging.INFO,
        filename=f'{__name__}.log',
        format='%(filename)s:%(lineno)d #%(levelname)-8s '
               '[%(asctime)s] - %(name)s - %(message)s')
    logger.info('Starting bot')

    task_users = asyncio.create_task(countinues_task_user())
    task_bybit = asyncio.create_task(countinues_taks_bybit())
    task_symbols = asyncio.create_task(countinues_taks_symbol())

    await asyncio.gather(task_users, task_bybit, task_symbols)


asyncio.run(main())


