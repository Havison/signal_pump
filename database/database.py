import datetime
import logging

import pymysql
import motor.motor_asyncio
from config_data.config import Config, load_config


logger3 = logging.getLogger(__name__)
handler3 = logging.FileHandler(f"{__name__}.log")
formatter3 = logging.Formatter("%(filename)s:%(lineno)d #%(levelname)-8s "
                               "[%(asctime)s] - %(name)s - %(message)s")


class MongoDatabase:
    def __init__(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')
        self.db = self.client['database']
        self.collection = self.db['price']

    try:
        async def market_add(self, market: list):
            for market in market:
                dt_old = datetime.datetime.now() - datetime.timedelta(minutes=35)
                symbol, price = market.values()
                result = await self.collection.find_one({'currency': symbol}, {'_id': 0})
                if result:
                    currency_value = result['data']
                    for i in result['data']:
                        if i['dt'] < dt_old:
                            currency_value.remove(i)
                    currency_value.append(price)
                    await self.collection.update_one({'currency': symbol}, {'$set': {'data': currency_value}})
                else:
                    await self.collection.insert_one({'currency': symbol, 'data': [(market['data'])]})
    except Exception as e:
        logger3.error('market_add', e)

    try:
        async def users_market(self, setting, last_price):
            result = self.collection.find({}, {'_id': 0})
            dt_pump = datetime.datetime.now() - datetime.timedelta(minutes=setting['pump'][1])
            dt_dump = datetime.datetime.now() - datetime.timedelta(minutes=setting['dump'][1])
            dt_pump_min = datetime.datetime.now() - datetime.timedelta(minutes=setting['pump_min'][1])
            finish_result = {}
            async for item in result:
                if not item['currency'] in last_price:
                    continue
                a = last_price[item['currency']][0]
                price = item['data'][::-1]
                for i in price:
                    t_pump = i['dt'] > dt_pump
                    t_dump = i['dt'] > dt_dump
                    t_pump_min = i['dt'] > dt_pump_min
                    if not any([t_pump, t_dump, t_pump_min]):
                        break
                    result = eval(f'({a} - {i['price']}) / {a} * {100}')
                    if t_pump:
                        if setting['pump'][0] > 0:
                            if setting['pump'][0] <= result:
                                finish_result.setdefault('pump', []).append((item['currency'], result))

                        else:
                            if setting['pump'][0] >= result:
                                finish_result.setdefault('pump', []).append((item['currency'], result))

                    if t_dump:
                        if setting['dump'][0] > 0:
                            if setting['dump'][0] <= result:
                                finish_result.setdefault('dump', []).append((item['currency'], result))

                        else:
                            if setting['dump'][0] >= result:
                                finish_result.setdefault('dump', []).append((item['currency'], result))

                    if t_pump_min:
                        if setting['pump_min'][0] > 0:
                            if setting['pump_min'][0] <= result:
                                finish_result.setdefault('pump_min', []).append((item['currency'], result))

                        else:
                            if setting['pump_min'][0] >= result:
                                finish_result.setdefault('pump_min', []).append((item['currency'], result))

            return finish_result
    except Exception as e:
        logger3.error('users_market', e)


class MySQLDatabase:
    _config: Config = load_config('.env')
    _user = _config.database.user
    _password = _config.database.password
    _host = _config.database.host
    _database = _config.database.database_type
    connect_db = pymysql.connect(host=_host, user=_user, password=_password, database=_database)

    def __init__(self, tg_id = None):
        self.tg_id = tg_id


    try:
        async def db_setting_selection(self):
            with self.connect_db.cursor() as db:
                db.execute('''SELECT * FROM users_settings WHERE tg_id=%s''', self.tg_id)
                value = db.fetchone()
                key = ('quantity_pump', 'interval_pump', 'quantity_short', 'interval_short', 'quantity_pump_min',
                       'interval_pump_min', 'quantity_signal_pd', 'interval_signal_pd', 'quantity_signal_pm',
                       'interval_signal_pm', 'stop_signal', 'tg_id', 'binance', 'bybit')
                result = dict(zip(key, value))
                return result
    except Exception as e:
        logger3.error('db_setting_selection', e)

    try:
        @classmethod
        async def symbol_binance_bybit(cls):
            with cls.connect_db.cursor() as db:
                db.execute('''SELECT symbol, market FROM symbol''')
                result = db.fetchall()
                bybit = []
                binance = []
                for i in result:
                    if i[1] == 1:
                        bybit.append(i[0])
                    else:
                        binance.append(i[0])
                market = (bybit, binance)
                return market
    except Exception as e:
        logger3.error('symbol_binance_bybit', e)


    try:
        @classmethod
        async def db_symbol_create(cls, symbol_list):
            with cls.connect_db.cursor() as db_sql:
                for symbol in symbol_list:
                    db_sql.execute('''SELECT * FROM symbol WHERE symbol = %s AND market = %s''', (symbol[0], symbol[1]))
                    result = db_sql.fetchone()
                    if not result:
                        db_sql.execute('''INSERT INTO symbol(symbol, market) VALUES (%s, %s)''', (symbol[0], symbol[1]))
                cls.connect_db.commit()
    except Exception as e:
        logger3.error('db_symbol_create', e)


    try:
        async def quantity(self, symbol, interval_user, short, interval_signal, quantity_signal):
            with self.connect_db.cursor() as db:
                db.execute('''SELECT 1 FROM quantity WHERE
                tg_id=%s and symbol=%s and pd=%s''', (self.tg_id, symbol, short))
                symbol_signal = db.fetchone()
                dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=interval_signal)
                dt_base = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=interval_user + 1)
                db.execute('''SELECT COUNT(*) FROM quantity WHERE (
                tg_id=%s and symbol=%s and pd=%s and date>%s) ORDER BY date''', (self.tg_id, symbol, short, dt))
                quantity_count = db.fetchone()
                db.execute('''SELECT COUNT(*) FROM quantity WHERE (
                tg_id=%s and symbol=%s and pd=%s and date>%s) ORDER BY date''', (self.tg_id, symbol, short, dt_base))
                quantity_count_base = db.fetchone()
                dt = datetime.datetime.now(datetime.timezone.utc)
                if symbol_signal is None:
                    db.execute('''INSERT INTO quantity (tg_id, symbol, pd, date) VALUES (
                    %s, %s, %s, %s)''', (self.tg_id, symbol, short, dt))
                    self.connect_db.commit()
                    return True
                elif quantity_count[0] < quantity_signal:
                    if quantity_count_base[0] < 1:
                        db.execute('''INSERT INTO quantity (tg_id, symbol, pd, date)
                        VALUES (%s, %s, %s, %s)''', (self.tg_id, symbol, short, dt))
                        self.connect_db.commit()
                        return True
                else:
                    return False
    except Exception as e:
        logger3.error('quantity_create', e)

    try:
        async def clear_quantity_signal(self, symbol, short, interval_signal):
            with self.connect_db.cursor() as db:
                if interval_signal not in [360, 720]:
                    interval_signal = 1440
                dt_cl = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=interval_signal)
                db.execute('''SELECT COUNT(*) FROM quantity WHERE
                        (tg_id=%s and symbol=%s and pd=%s and date>%s) ORDER BY date''',
                                                  (self.tg_id, symbol, short, dt_cl))
                quantity_count = db.fetchone()
                dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=1440)
                db.execute('''DELETE FROM quantity WHERE (
                tg_id=%s and pd=%s and date<%s)''',
                                 (self.tg_id, short, dt))
                self.connect_db.commit()
                return quantity_count[0]
    except Exception as e:
        logger3.error('clear_quantity_signal', e)


    try:
        async def premium_user(self):  #функция проверяет на подписку
            with self.connect_db.cursor() as db:
                self.connect_db.commit()
                db.execute('''SELECT tg_id, data_prem FROM users_prem WHERE (tg_id=%s)''',
                                           (self.tg_id, ))
                premium = db.fetchone()
                if premium:
                    return premium[1]
                else:
                    return False
    except Exception as e:
        logger3.error('premium_user', e)


    try:
        @classmethod
        async def list_premium(cls):
            with cls.connect_db.cursor() as db:
                await cls.clear_premium()
                cls.connect_db.commit()
                db.execute('''SELECT tg_id FROM users_prem''')
                users = db.fetchall()
                return users
    except Exception as e:
        logger3.error('list_premium', e)


    try:
        async def free_premium_user(self):
            with self.connect_db.cursor() as db:
                self.connect_db.commit()
                db.execute('''SELECT tg_id FROM free_prem WHERE tg_id=%s''', (self.tg_id, ))
                free_premium = db.fetchone()
                if free_premium:
                    return True
                else:
                    db.execute('''INSERT INTO free_prem (tg_id) VALUES (%s)''', (self.tg_id,))
                    self.connect_db.commit()
                    await self.premium_setting(1)
                    return False
    except Exception as e:
        logger3.error('free_premium_user', e)


    try:
        @classmethod
        async def clear_premium(cls):
            with cls.connect_db.cursor() as db:
                cls.connect_db.commit()
                today = datetime.datetime.now()
                db.execute('''SELECT tg_id FROM users_prem WHERE (data_prem<%s)''',
                                           (today, ))
                premium = db.fetchall()
                if premium:
                    for i in premium:
                        db.execute('''DELETE FROM users_prem WHERE tg_id=%s''', (i,))
                        cls.connect_db.commit()
                        db.execute('''DELETE FROM users_settings WHERE tg_id=%s''', (i,))
                        cls.connect_db.commit()
                        db.execute('''DELETE FROM setting_oi WHERE tg_id=%s''', (i,))
                        cls.connect_db.commit()
    except Exception as e:
        logger3.error('clear_premium', e)


    try:
        async def state_signal(self):
            with self.connect_db.cursor() as db:
                db.execute('''SELECT stop_signal FROM users_settings WHERE (tg_id=%s)''',
                                           (self.tg_id, ))
                state_signal_user = db.fetchone()
                return state_signal_user
    except Exception as e:
        logger3.error('state_signal', e)
