from aiogram import Bot
import logging
import humanize
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from config_data.config import Config, load_config


config: Config = load_config('.env')
bot = Bot(
    token=config.tg_bot.token,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)


logger3 = logging.getLogger(__name__)
handler3 = logging.FileHandler(f"{__name__}.log")
formatter3 = logging.Formatter("%(filename)s:%(lineno)d #%(levelname)-8s "
                               "[%(asctime)s] - %(name)s - %(message)s")
handler3.setFormatter(formatter3)
logger3.addHandler(handler3)
logger3.info(f"Testing the custom logger for module {__name__}")

_i = humanize.i18n.activate('ru_RU')


async def message_bybit_binance(tg_id, lp, symbol, interval, q, sml, qi='за 24 часа'):
    coinglass = f'https://www.coinglass.com/tv/ru/Bybit_{symbol}'
    bybit = f'https://www.bybit.com/trade/usdt/{symbol}'
    binance = f'https://www.binance.com/ru/futures/{symbol}'
    await bot.send_message(chat_id=tg_id, text=f'{sml}<b>{symbol[0:-4]}</b>\n'
                                               f'<b>⚫ByBit and 🌕Binance</b>\n'
                                               f'<b>Изменения за {interval} минут</b>\n'
                                               f'&#128181;Цена изменилась на: <b>{round(lp, 2)}%</b>\n'
                                               f'&#129535;Кол-во сигналов {qi}: <b>{q}</b>\n'
                                               f'<a href=\"{bybit}\">ByBit</a> | <a href=\"{coinglass}\">CoinGlass</a> | <a href=\"{binance}\">Binance</a>',
                           parse_mode='HTML', disable_web_page_preview=True)


async def message_bybit(tg_id, lp, symbol, interval, q, sml, qi='за 24 часа', ):
    coinglass = f'https://www.coinglass.com/tv/ru/Bybit_{symbol}'
    bybit = f'https://www.bybit.com/trade/usdt/{symbol}'
    await bot.send_message(chat_id=tg_id, text=f'{sml}<b>{symbol[0:-4]}</b>\n'
                                               f'<b>⚫ByBit</b>\n'
                                               f'<b>Изменения за {interval} минут</b>\n'
                                               f'&#128181;Цена изменилась на: <b>{round(lp, 2)}%</b>\n'
                                               f'&#129535;Кол-во сигналов {qi}: <b>{q}</b>\n'
                                               f'<a href=\"{bybit}\">ByBit</a> | <a href=\"{coinglass}\">CoinGlass</a>',
                           parse_mode='HTML', disable_web_page_preview=True)


async def message_binance(tg_id, lp, symbol, interval, q, sml, qi='за 24 часа', ):
    coinglass = f'https://www.coinglass.com/tv/ru/binance_{symbol}'
    binance = f'https://www.binance.com/ru/futures/{symbol}'
    await bot.send_message(chat_id=tg_id, text=f'{sml}<b>{symbol[0:-4]}</b>\n'
                                               f'<b>🌕Binance</b>\n'
                                               f'<b>Изменения за {interval} минут</b>\n'
                                               f'&#128181;Цена изменилась на: <b>{round(lp, 2)}%</b>\n'
                                               f'&#129535;Кол-во сигналов {qi}: <b>{q}</b>\n'
                                               f'<a href=\"{binance}\">Binance</a> | <a href=\"{coinglass}\">CoinGlass</a>',
                           parse_mode='HTML', disable_web_page_preview=True)




