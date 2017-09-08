import time
import traceback
from itertools import groupby
from apscheduler import events
from apscheduler.schedulers.blocking import BlockingScheduler

from jubi_aop_monitor import monitor
from jubi_common_func import *
from jubi_log import logger
from jubi_common_func import send_email

def __get_monitor_setting():
    """
    获取可被监控的幅度
    :return: 
    """
    rs = []
    c = Mysql.conn.cursor()
    c.execute('select distinct rate from jb_price_rate_monitor_setting')
    rs.extend([d[0] for d in c.fetchall()])
    return rs

subject = '聚币监控 - 涨幅提醒'
cache_rate_notify_price_prev_prefix = "cache_price_rate_notify_prev_"

def __notify():
    """
    发送通知
    :return: 
    """
    coins = get_all_coins()
    monitor_rates = __get_monitor_setting()  # 通知的结点
    if len(coins) == 0:
        return
    cts = get_current_tickers(coins.keys())
    if len(cts) == 0:
        return
    rs = []
    for coin in coins.keys():
        for mr in monitor_rates:
            r = __get_rate_matched_coin_info(coin, cts[coin], mr)
            if r is not None:
                rs.append(r)

    logger.info(rs)
    if len(rs) == 0:
        return

    ucm = __get_user_content_map(rs, cts)
    logger.info(ucm)
    for user_id in ucm.keys():
        content = ucm[user_id]
        content = "\r\n{}".format(content)
        send_email(user_id, subject, content, 3)

    __set_coin_prev_ticker_after_montor_matched(rs, cts)

def __format_time(pk):
    """
    转化时间为 时:分
    :param pk: int
    :return: 
    """
    time_array = time.localtime(pk)
    return time.strftime('%H:%M', time_array)

def __get_user_content_map(rs, cts):
    """
    获取用户，及其通知内容
    :param rs: 满足通知条件的币信息
    :param cts: 当前所有币行情
    :return: dict - {user_id: content}
    """
    user_notifies = []  # 用户，币，监控涨幅。一个用户有多条数据。
    for r in rs:
        coin = r[0]
        monitor_rate = r[1]
        user_notifies.extend([(user_id, coin, monitor_rate) for user_id in __get_notify_users(coin, monitor_rate)])

    user_notifies_map = {}  # 对 user_notifies 以用户分组
    f = lambda p: p[0]
    user_notifies.sort(key=f)
    for key, group in groupby(user_notifies, key=f):
        user_notifies_map[key] = list(group)

    ucm = {}  # 将一个用户多个币的通知合并为一个，生成user_id : content
    for user_id in user_notifies_map.keys():
        items = user_notifies_map[user_id]
        cs = []
        for item in items:
            coin = item[1]
            monitor_rate = item[2]
            content = __get_user_notify_content(coin, monitor_rate, cts)
            cs.append(content)
        ucm[user_id] = '\t' + '\t\r\n'.join(cs)

    return ucm

def __get_user_notify_content(coin, monitor_rate, cts):
    """
    获取用户通知内容
    :param coin: 币
    :param monitor_rate: 监控幅度
    :param cts: dict - {coin: (pk, price)} 所有币当前行情
    :return: 
    """
    cur_ticker = cts[coin]
    prev_ticker = __get_prev_ticker(coin, monitor_rate)
    cur_pk = cur_ticker[0]
    cur_price = cur_ticker[1]
    prev_pk = prev_ticker[0]
    prev_price = prev_ticker[1]
    rate = __get_rate(cur_price, prev_price)
    return '{} 当前价格 {} 元({})， 涨幅 {}%， 对比价格 {} 元({})，请知悉。'.format(coin, cur_price, __format_time(cur_pk), rate, prev_price, __format_time(prev_pk))

def __get_uesr_info(user_id):
    """
    获取用户信息
    :param user_id: 
    :return: tuple - (user_id, nickname, email) 
    """
    c = Mysql.conn.cursor()
    c.execute("select user_id, nickname, email from zx_account where user_id=%s", (user_id,))
    if c.rowcount == 0:
        return
    return c.fetchone()

def __get_notify_users(coin, monitor_rate):
    """
    获取待通知用户
    :param coin: 
    :param monitor_rate: 监控幅度 
    :return: 
    """
    c = Mysql.conn.cursor()
    c.execute("select distinct user_id from jb_price_rate_notify where coin=%s and rate=%s", (coin, monitor_rate))
    if c.rowcount == 0:
        return []
    return set([d[0] for d in c.fetchall()])

def __set_coin_prev_ticker_after_montor_matched(rs, cts):
    """
    满足指定监控涨幅条件后，更新虚拟币指定涨幅的上次行情为当前行情
    :param rs: list - [(coin, monitor_rate)]
    :param cts: dict - {coin: (pk, price)} 所有币当前行情
    :return: 
    """
    if len(rs) == 0:
        return
    for r in rs:
        coin = r[0]
        monitor_rate = r[1]
        ticker = cts[coin]
        __set_prev_ticker(coin, monitor_rate, ticker)

def __get_rate_matched_coin_info(coin, ticker, monitor_rate):
    """
    获取满足监控涨幅的币信息
    :param coin: 币 
    :param ticker: 当前行情; tuple - (pk, price)
    :param monitor_rate: 监控的涨幅
    :return: tuple - (coin, pk, price, prev_pk, prev_price, rate, monitor_rate)
    :return: tuple - (coin, monitor_rate)
    """
    if ticker is None or len(ticker) == 0:
        return
    prev = __get_prev_ticker(coin, monitor_rate)
    if prev is None or len(prev) == 0:
        __set_prev_ticker(coin, monitor_rate, ticker)
        return
    cur_price = ticker[1]
    prev_price = float(prev[1])
    d = None
    if __is_notify_match(cur_price, prev_price, monitor_rate):
        d = (coin, monitor_rate)
    return d

cache_coin_price_rate = "cache_price_rate_"

def __get_prev_ticker(coin, rate):
    """
    获取上次提示行情
    :param coin: 币
    :param rate: 监控幅度 
    :return: tuple - (pk, price)
    """
    prev = RedisPool.conn.hget(__get_prev_price_cache_key(coin, rate), coin)
    return eval(prev) if prev is not None else prev

def __set_prev_ticker(coin, rate, ticker):
    """
    设置上次提示行情，一次通知流程结束后执行
    :param coin: 
    :param rate: 监控幅度
    :return: 
    """
    RedisPool.conn.hset(__get_prev_price_cache_key(coin, rate), coin, ticker)

def __is_notify_match(cur_price, prev_price, rate):
    """
    满足提示条件
    :param cur_price: 当前价格 
    :param prev_price: 上次价格
    :param rate: 提示幅度
    :return: 
    """
    return abs(__get_rate(cur_price, prev_price)) >= rate

def __get_rate(cur_price, prev_price):
    if prev_price == 0:
        return 0
    return round((cur_price - prev_price) / prev_price * 100, 2)

def __get_prev_price_cache_key(coin, rate):
    """
    获取上次价格的缓存键
    :param coin: 
    :param rate: 监控幅度
    :return: 
    """
    return cache_rate_notify_price_prev_prefix + coin + "_" + str(rate)

@monitor("work")
def work():
    logger.info("Price rate notify monitor work")
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
