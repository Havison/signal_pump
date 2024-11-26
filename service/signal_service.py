import asyncio
import logging
import requests

from datetime import datetime
from pybit.unified_trading import HTTP
from config_data.config import Config, load_config
from database.database import MongoDatabase, MySQLDatabase
from handlers.user import message_bybit_binance, message_bybit, message_binance

logger2 = logging.getLogger(__name__)
handler2 = logging.FileHandler(f"{__name__}.log")
formatter2 = logging.Formatter("%(filename)s:%(lineno)d #%(levelname)-8s "
                               "[%(asctime)s] - %(name)s - %(message)s")

config: Config = load_config('.env')
mongo_db = MongoDatabase()

try:
    session = HTTP(
        testnet=False,
        api_key=config.by_bit.api_key,
        api_secret=config.by_bit.api_secret,
    )
except Exception as e:
    logger2.error(e)


async def market_price():
    try:
        await asyncio.sleep(2)
        url = 'https://fapi.binance.com/fapi/v2/ticker/price'
        response = requests.get(url)
        data_binance = response.json()
        binance_symbol = []
        data_bybit = session.get_tickers(category="linear")
        last_price = {}
        market_data = []
        bybit_symbol = []
        for dicts in data_bybit['result']['list']:
            if 'USDT' in dicts['symbol']:
                market_data.append({'currency': dicts['symbol'],
                             'data': {'price': eval(dicts['lastPrice']),
                                      'oi': eval(dicts['openInterest']),
                                      'dt': datetime.now()}})
                last_price[dicts['symbol']] = (dicts['lastPrice'], dicts['openInterest'], datetime.now())
                bybit_symbol.append((dicts['symbol'], 1))
        for data in data_binance:
            if 'USDT' in data['symbol']:
                if data['symbol'] not in bybit_symbol:
                    market_data.append({'currency': data['symbol'],
                                 'data': {'price': eval(data['price']),
                                          'oi': -1,
                                          'dt': datetime.now()}})
                    last_price[data['symbol']] = (data['price'], None, datetime.now())
                binance_symbol.append((data['symbol'], 0))
        return market_data, bybit_symbol, binance_symbol, last_price
    except Exception as e:
        logger2.error(e)
        await asyncio.sleep(5)
        await market_price()


async def market_add_database():
    data = await market_price()
    await mongo_db.market_add(data[0])
    await asyncio.sleep(3)


async def users_list():
    # try:
    bybit, binance = await MySQLDatabase.symbol_binance_bybit()
    user = await MySQLDatabase.list_premium()
    user_iter = [i[0] for i in user]
    while user_iter:
        tg_id_user = [user_signal_bybit(user, bybit, binance) for user in user_iter[:10]]
        await asyncio.gather(*tg_id_user)
        user_iter = user_iter[10:]
    # except Exception as e:
    #     logger2.error(e)
    #     await asyncio.sleep(2)


async def default_signal_user(user, symbol, a, sml, quantity_interval, interval, pd, bybit, binance,
                              quantity_signal_pd):
    quantity_signal = 0
    hours_signal = {360: 'за 6 часов', 720: 'за 12 часов'}
    signal_state = await user.state_signal()
    if quantity_interval not in hours_signal:
        hours = 'за 24 часа'
    else:
        hours = hours_signal[quantity_interval]
    if not signal_state[0]:
        return
    if await user.quantity(symbol, interval, pd, quantity_interval, quantity_signal_pd):
        await asyncio.sleep(2)
        q = await user.clear_quantity_signal(symbol, pd, quantity_interval)
        quantity_signal += 1
        if quantity_signal > 10:
            return
        if symbol in bybit and symbol in binance:
            await message_bybit_binance(user.tg_id, a, symbol, interval, q, sml, hours)
        elif symbol in bybit:
            await message_bybit(user.tg_id, a, symbol, interval, q, sml, hours)
        else:
            await message_binance(user.tg_id, a, symbol, interval, q, sml, hours)


async def user_signal_bybit(idt, bybit, binance):
    user = MySQLDatabase(idt)
    last_price = await market_price()
    last_price = last_price[3]
    setting = await user.db_setting_selection()
    quantity_interval = setting['interval_signal_pd']
    quantity_interval_min = setting['interval_signal_pm']
    interval_pump = setting['interval_pump']
    interval_dump = setting['interval_short']
    interval_pump_min = setting['interval_pump_min']
    quantity_signal_pd = setting['quantity_signal_pd']
    quantity_signal_pm = setting['quantity_signal_pm']
    setting_user = {
        'pump': (setting['quantity_pump'], setting['interval_pump']),
        'dump': (setting['quantity_short'], setting['interval_short']),
        'pump_min': (setting['quantity_pump_min'], setting['interval_pump_min'])}
    signal_price = await mongo_db.users_market(setting_user, last_price)
    if 'pump' in signal_price:
        for value in signal_price['pump']:
            symbol = value[0]
            a = value[1]
            await default_signal_user(user, symbol, a,
                                      '&#128994;', quantity_interval, interval_pump,
                                      1, bybit, binance, quantity_signal_pd)
    if 'dump' in signal_price:
        for value in signal_price['dump']:
            symbol = value[0]
            a = value[1]
            await default_signal_user(user, symbol, a, '&#128308;', quantity_interval, interval_dump,
                                      0, bybit, binance, quantity_signal_pd)
    if 'pump_min' in signal_price:
        for value in signal_price['pump_min']:
            symbol = value[0]
            a = value[1]
            await default_signal_user(user, symbol, a, '&#x1F4B9;', quantity_interval_min, interval_pump_min,
                                      2, bybit, binance, quantity_signal_pm)


async def add_symbol():
    symbol = await market_price()
    add = symbol[1] + symbol[2]
    await MySQLDatabase.clear_premium()
    await MySQLDatabase.db_symbol_create(add)
