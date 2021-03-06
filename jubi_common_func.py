"""
通用功能
"""
import time
from jubi_redis import RedisPool
import jubi_mysql as Mysql

day_format = '%Y-%m-%d'

all_coin_key = 'py_all_coins'
email_sending_queue_key = 'py_email_sending_queue'

def get_all_coins():
    """
    获取所有币信息
    :return: dict - {code:name}
    """
    coins = RedisPool.conn.get(all_coin_key)
    if coins is None:
        cursor = Mysql.conn.cursor()
        cursor.execute("select code, name from jb_coin")
        ds = cursor.fetchall()
        Mysql.conn.commit()
        cursor.close()
        coins = {}
        for d in ds:
            code = d[0]
            name = d[1]
            coins[code] = name
        RedisPool.conn.set(all_coin_key, str(coins), nx=True, ex=3600)
    if type(coins) != dict:
        coins = eval(coins)
    return coins

def get_current_tickers(coins):
    """
    获取当前行情信息
    :param coins: 所有币信息 dict - {code:name}
    :return: dict - {coin: (pk, price)}
    """
    if len(coins) == 0:
        return
    pipe = RedisPool.conn.pipeline()
    for key in coins:
        pipe.get("cache_ticker_{}".format(key))
    rs = pipe.execute()
    if len(rs) == 0:
        return
    ts = {}
    for r in rs:
        if r is None:
            continue
        t = eval(r)
        pk = t['pk']
        coin = t['coin']
        price = t['last']
        ts[coin] = (pk, price)
    return ts

def get_day_begin_time_int(t):
    """
    获取当天开始时间( 00:00:00 )的int值
    :return: 
    """
    lt = time.localtime(t)
    s = time.strftime(day_format, lt)
    p = time.strptime(s, day_format)
    return int(time.mktime(p))

def send_email(user_id, subject, content, monitor_type) :
    RedisPool.rconn.rpush(email_sending_queue_key, (user_id, subject, content, monitor_type))