import time
import traceback
from apscheduler import events
from apscheduler.schedulers.blocking import BlockingScheduler

from jubi_common_func import *
from jubi_log import logger
from jubi_email_sender import email_sending_queue_key
from jubi_common_func import send_email

notify_last_pk_cache_key = "notify_last_pk"  # 上次通知用户的时间

def __send_email_to_user(user_id, infos, callback):
    """
    发送邮件给用户
    :param user_id
    :param infos: list - [(coin, price, rate, pk)]
    :param callback: 回调函数
    :return: 
    """
    if len(infos) == 0:
        return

    contents = []
    for info in infos:
        coin = info[0]
        price = info[1]
        rate = info[2]
        pk = info[3]
        cmp_price = info[4]
        content = '当前 {} 价格为：{} 元，价格波动：{}%，对比价格 {} 元,请知悉。'.format(coin.upper(), price, rate, cmp_price)
        contents.append(content)
        params = (user_id, pk, coin)
        callback(params)
    content = '\t\r\n'.join(contents)
    subject = '聚币监控 - 波动提醒'
    send_email(user_id, subject, content, 2)

def __get_user_info(user_id):
    c = Mysql.conn.cursor()
    c.execute('select nickname, email from zx_account where user_id = %s', user_id)
    return c.fetchone()

def __notify():
    """
    发送通知
    :return: 
    """
    m = __aggregate_by_coin()
    if len(m) == 0:
        return
    keys = m.keys()
    coins = get_all_coins()
    cts = __get_current_tickers(coins)
    if len(cts) == 0:
        return
    rs = []
    for coin in keys:
        rs.extend(__get_coin_notify(coin, m[coin], cts[coin]))
    if len(rs) == 0:
        post_notify(cts)
        return

    m = {}
    for r in rs:
        user_id = r[1]
        if user_id not in m:
            m[user_id] = []
        m[user_id].append((r[0], r[2], r[3], r[4], r[5]))
    keys = m.keys()
    for user_id in keys:
        __send_email_to_user(user_id, m[user_id], __mark_user_notify_info)
    post_notify(cts)

def post_notify(tickers):
    """
    发送后的工作
    :param tickers: dict - {coin:(pk, price)}
    :return: 
    """
    if len(tickers) == 0:
        return
    coins = tickers.keys();
    for coin in coins:
        pk = tickers[coin][0]
        price = tickers[coin][1]
        t = (pk, coin, price)
        __clear_old_cache(t)
        __add_ticker(t)

def __get_coin_notify(coin, ss, ticker):
    """
    获取指定币通知信息
    :param coin: 币 
    :param ss: 通知配置; list - [(user_id, coin, span, rate)]
    :param ticker: 当前行情; tuple - (pk, price)
    :return: list - [(coin, user_id, price, rate, pk, cmp_price)]
    """
    if len(ticker) == 0:
        return
    rs = []
    his = __get_history_tickers(coin)
    if len(his) == 0:
        return rs
    for s in ss:
        user_id = s[0]
        span = s[2]
        rate = s[3]
        r = __do_get_coin_notify(coin, user_id, span, rate, ticker, his)
        if r is not None:
            rs.append(r)
    return rs

def __do_get_coin_notify(coin, user_id, span, rate, ticker, his):
    """
    根据币发送通知
    :parma coin: 币
    :param user_id: 用户ID 
    :param span: 时间间隔
    :param rate: 涨跌幅
    :param ticker: 当前行情 (pk, price)
    :param his: 历史数据 dict - {pk: price}
    :return: tuple - (coin, user_id, price, rate, pk, cmp_price)
    """
    cur_pk = ticker[0]
    cur_price = ticker[1]
    last_pk = __get_user_last_notify_time(user_id, coin)
    min, max = __get_history_min_max(span, cur_pk, his, last_pk)
    if min == 0 or max == 0:
        return
    if cur_price > min:
        minr = __get_rate(cur_price, min)
        if abs(minr) >= rate:
            r = (coin, user_id, cur_price, minr, cur_pk, min)
            return r
    if cur_price < max:
        maxr = __get_rate(cur_price, max)
        if abs(maxr) >= rate:
            r = (coin, user_id, cur_price, maxr, cur_pk, max)
            return r
    return

def __get_rate(cp, hp):
    """
    获取涨跌幅度
    :param cp: 当前价格
    :param hp: 历史价格
    :return: 
    """
    return round(((cp - hp) * 100) / hp, 2)

def __get_history_min_max(span, cur_pk, his, last_pk):
    """
    获取历史最大最小值
    :param span: 间隔 
    :param cur_pk: 当前时间 
    :param his: 历史行情
    :param last_pk: 上一次通知时间
    :return: min, max
    """
    pks = his.keys()
    prices = []
    for pk in pks:
        if pk > last_pk and (cur_pk - span * 60) <= pk:
            prices.append(his[pk])
    if len(prices) == 0:
        return 0, 0
    return min(prices), max(prices)

def __aggregate_by_coin():
    """
    通过币聚合配置信息
    :return: dict - {coin: [(user_id, coin, span, rate)]}
    """
    ss = __get_settings()
    if len(ss) == 0:
        return
    m = {}
    for s in ss:
        coin = s[1]
        if coin not in m:
            m[coin] = []
        m[coin].append(s)
    return m

def __get_settings():
    """
    获取所有配置信息
    :return: list - [(user_id, coin, span, rate)]
    """
    c = Mysql.conn.cursor()
    c.execute('select user_id, coin, `span`, rate from jb_price_wave_notify')
    return c.fetchall()

def __get_current_tickers(coins):
    """
    获取当前行情信息
    :param coins: 所有币信息 dict - {code:name}
    :return: dict - {coin: (pk, price)}
    """
    if len(coins) == 0:
        return
    keys = coins.keys()
    pipe = RedisPool.conn.pipeline()
    for key in keys:
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

cache_coin_price_rate = "cache_price_rate_"

def __add_ticker(ticker):
    """
    将当前行情加入历史数据中
    :param ticker: tuple - (pk, coin, price)
    :return: 
    """
    pk = ticker[0]
    coin = ticker[1]
    price = ticker[2]
    RedisPool.conn.hsetnx(cache_coin_price_rate + coin, pk, price)

def __get_history_tickers(coin):
    """
    获取历史行情，一小时内有效行情数据
    :param coin: 币
    :return: dict - {pk: price}
    """
    ds = RedisPool.conn.hgetall(cache_coin_price_rate + coin)
    rs = {}
    for d in ds:
       rs[int(d)] = float(ds[d])
    return rs

def __clear_old_cache(ticker):
    """
    清除旧的行情信息。大于当前时间1小时
    :param ticker: 当前行情; tuple - (pk, coin, price)
    :return: 
    """
    pk = ticker[0]
    coin = ticker[1]
    keys = RedisPool.conn.hkeys(cache_coin_price_rate + coin)
    if len(keys) == 0:
        return
    for key in keys:
        if (pk - int(key)) > 3600:
            RedisPool.conn.hdel(cache_coin_price_rate + coin, key)

def __mark_user_notify_info(data):
    """
    标记用户提醒时间。在提醒后触发。
    :param coin: 
    :return: 
    """
    user_id = data[0]
    pk = data[1]
    coin = data[2]
    info = RedisPool.conn.hget(notify_last_pk_cache_key, user_id)
    if info is None:
        info = {}
    else:
        info = eval(info)
    info[coin] = pk
    RedisPool.conn.hset(notify_last_pk_cache_key, user_id, info)

def __get_user_last_notify_time(user_id, coin):
    """
    获取上一次通知用户的时间
    :param user_id: 
    :param coin: 
    :return: 
    """
    info = RedisPool.conn.hget(notify_last_pk_cache_key, user_id)
    if info is None:
        return 0
    info = eval(info)
    if coin in info:
        pk = info[coin]
    else:
        pk = 0
    return pk

def work():
    print("Price wave notify monitor work")
    __notify()

def err_listener(event):
    if event.exception:
        exstr = traceback.format_exc()
        logger.error('The job crashed with exception : {0}'.format(event.exception))
        logger.error(exstr)

def mis_listener(event):
    logger.warning("Price notify job misfired at {}".format(time.strftime("%Y-%m-%d %X")))

if __name__ == '__main__':
    conf = {
        'apscheduler.job_defaults.coalesce': 'false',
        'apscheduler.job_defaults.max_instances': '1'
    }
    sched = BlockingScheduler(conf)
    sched.add_job(work, 'cron', second='0/10', hour='6-22')
    sched.add_listener(err_listener, events.EVENT_JOB_ERROR)
    sched.add_listener(mis_listener, events.EVENT_JOB_MISSED)

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        exstr = traceback.format_exc()
        logger.error(exstr)
