import asyncio
import logging
from pybit.unified_trading import HTTP
from config_data.config import Config, load_config
from database.database import MongoDatabase
from math import ceil

logger2 = logging.getLogger(__name__)
handler2 = logging.FileHandler(f"{__name__}.log")
formatter2 = logging.Formatter("%(filename)s:%(lineno)d #%(levelname)-8s "
                               "[%(asctime)s] - %(name)s - %(message)s")

config: Config = load_config('.env')
mongo_db = MongoDatabase()
client = HTTP(testnet=False, api_key=config.by_bit.api_key, api_secret=config.by_bit.api_secret)


trade_amount = 25 # Размер сделки в USDT
stop_loss_pct = 2  # Стоп-лосс в процентах
take_profit_pct = 4  # Уровень активации трейлинг-стопа
trailing_stop_pct = 1.5
# Расстояние трейлинг-стопа


async def get_candles(symbol, interval="5", limit=2):
    """
    Получение свечей (интервал 5 минут)
    """
    try:
        response = client.get_kline(
            category="linear",
            symbol=symbol,
            interval=interval,
            limit=limit
        )
        return response.get("result", {}).get("list", [])
    except Exception as e:
        print(f"Ошибка при получении данных: {e}")
        return []


async def get_symbol_price(symbol):
    """Получение текущей цены символа"""
    try:
        ticker = client.get_tickers(category="linear", symbol=symbol)
        x = ticker['result']['list'][0]['lastPrice']
        v = ticker['result']['list'][0]['volume24h']
        if abs(float(v) * float(x)) < 5000000:
            logger2.info('Маленький объем, завершаю скрипт')
            return False
        return float(x)
    except Exception as e:
        logger2.error(f"Ошибка при получении цены: {e}")
        return None


async def place_short_trade(symbol, amount, stop_loss, trailing_stop, trigger_profit):
    """Открытие короткой позиции с расчетом количества монет"""
    try:
        symbol_price = await get_symbol_price(symbol)
        if not symbol_price:
            return False
        if symbol_price > 10:
            qty = str(round(amount / symbol_price, 1))
        else:
            qty = str(ceil(amount / symbol_price))
        # Открываем короткую позицию
        order = client.place_order(
            category="linear",
            symbol=symbol,
            side="Sell",
            orderType="Market",
            qty=qty,
            timeInForce="GoodTillCancel",
            positionIdx=2
        )
        logger2.info(f"Открыт шорт: {order}")


        # Установка стоп-лосса и трейлинг-стопа
        stop_loss_price = symbol_price * (1 + stop_loss / 100)
        activation_price = symbol_price * (1 - trigger_profit / 100)
        take_profit = symbol_price * (1 - 4 / 100)
        trailing_stop_distance = activation_price * (trailing_stop / 100)

        while True:
            last_price = await get_symbol_price(symbol)
            result = eval(f'({last_price} - {symbol_price}) / {last_price} * {100}')
            if result <= -1.8:
                stop = last_price - (last_price / 100)
                client.set_trading_stop(
                    category="linear",
                    symbol=symbol,
                    trailing_stop=str(trailing_stop_distance),
                    activePrice=str(activation_price),
                    stoploss=str(stop),
                    positionIdx=2
                )
                logger2.info(f"Установлен стоп и тейк: стоп на цене {stop}, тэйк {take_profit}.")
                break
    except Exception as e:
        logger2.error(f"Ошибка при открытии шорта: {e}")


async def trade(symbol, last_price_trade):
    logger2.info(f"Начинаем мониторинг пары {symbol}...")
    while True:
        try:
            last_price_now = await get_symbol_price(symbol)
            if not last_price_now:
                logger2.info('Недостаточно оборота')
                break
            candles = await get_candles(symbol)
            if len(candles) < 2:
                logger2.info("Недостаточно данных для анализа.")
                continue

            # Анализ последней и предпоследней свечи
            last_candle = candles[-2]  # Предыдущая свеча
            open_price = float(last_candle[1])
            close_price = float(last_candle[4])

            # Проверяем "красную свечу"
            if close_price < open_price:
                last_price_now = await get_symbol_price(symbol)
                price = eval(f'({last_price_trade} - {last_price_now}) / {last_price_trade} * {100}')
                if price <= -4:
                    logger2.info('Цена сильно упала, сделку не открываю')
                    break
                logger2.info(f"Обнаружена красная свеча. Открываем шорт на {trade_amount} USDT.")
                await place_short_trade(
                    symbol,
                    trade_amount,
                    stop_loss_pct,
                    trailing_stop_pct,
                    take_profit_pct
                )
                break

            logger2.info("Нет сигнала для входа. Ждем следующую свечу...")
            await asyncio.sleep(305)  # Пауза 5 минут
        except Exception as e:
            logger2.info(f"Ошибка: {e}")
            break