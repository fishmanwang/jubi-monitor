import time
import traceback
import smtplib
from itertools import groupby
from email.mime.text import MIMEText
from apscheduler import events
from apscheduler.schedulers.blocking import BlockingScheduler

from jubi_common_func import *
from jubi_log import logger
from jubi_email_sender import send_email

def __get_monitor_setting():
    """
    获取可被监控的幅度
    :return: 
    """
    #rs = []
    #c = Mysql.conn.cursor()
    #c.execute('select distinct rate from jb_price_rate_monitor_setting')
    #s.extend([d[0] for d in c.fetchall()])
    #return rs
    return [0.01, 0.02, 0.03]

monitor_rates = __get_monitor_setting()  # 通知的结点

cache_rate_notify_price_prev_prefix = "cache_price_rate_notify_prev_"

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
    user = __get_user_info(user_id)
    if user is None:
        return

    mail_host = 'smtp.163.com'
    mail_port = 465
    mail_user = 'tjwang516@163.com'
    mail_pass = 'Admin123'
    sender = 'tjwang516@163.com'

    nickname = user[0]
    email = user[1]
    contents = []
    for info in infos:
        coin = info[0]
        price = info[1]
        rate = info[2]
        pk = info[3]
        content = '当前 {} 价格为：{} 元，价格波动：{}%，请知悉。'.format(coin.upper(), price, rate)
        contents.append(content)

    try:
        content = nickname + ":\t\r\n" + '\t\r\n'.join(contents)
        server = smtplib.SMTP_SSL(mail_host, mail_port)
        server.login(mail_user, mail_pass)
        msg = MIMEText(content, _charset='utf-8')
        msg['From'] = sender
        msg['To'] = email
        msg['Subject'] = '聚币监控 - 波动提醒'
        server.sendmail(sender, [email], msg.as_string())
        #print(content)
        callback(user_id, pk, coin)
        server.close()
    except smtplib.SMTPException:
        exstr = traceback.format_exc()
        logger.error("Error: 发送邮件失败。内容：" + content + "。原因：" + exstr)

def __get_user_info(user_id):
    """
    获取用户信息
    :param user_id: 
    :return: tuple - (nickname, email)
    """
    c = Mysql.conn.cursor()
    c.execute('select nickname, email from zx_account where user_id = %s', user_id)
    return c.fetchone()

subject = '聚币监控 - 涨幅提醒'

def __notify():
    """
    发送通知
    :return: 
    """
    coins = get_all_coins()
    cts = get_current_tickers(coins.keys())
    if len(cts) == 0:
        return
    rs = []
    for coin in coins.keys():
        for mr in monitor_rates:
            r = __get_rate_matched_coin_info(coin, cts[coin], mr)
            if r is not None:
                rs.append(r)

    print(rs)
    if len(rs) == 0:
        return

    ucm = __get_user_content_map(rs, cts)
    print(ucm)
    for user_id in ucm.keys():
        user = __get_user_info(user_id)
        content = ucm[user_id]
        nickname = user[0]
        email = user[1]
        content = "{}\r\n{}".format(nickname, content)
        send_email(email, subject, content)

    __set_coin_prev_ticker_after_montor_matched(rs, cts)

    # if len(rs) == 0:
    #     post_notify(cts)
    #
    #
    # m = {}
    # for r in rs:
    #     user_id = r[1]
    #     if user_id not in m:
    #         m[user_id] = []
    #     m[user_id].append((r[0], r[2], r[3], r[4]))
    # keys = m.keys()
    # for user_id in keys:
    #     __send_email_to_user(user_id, m[user_id], __mark_user_notify_info)
    # post_notify(cts)

def __format_time(pk):
    """
    转化时间为 时:分
    :param pk: int
    :return: 
    """
    time_array = time.localtime(pk)
    return time.strftime('%M:%S', time_array)

def __get_user_content_map(rs, cts):
    """
    获取用户，及其通知内容
    :param rs: 满足通知条件的币信息
    :param cts: 当前所有币行情
    :return: dict - {user_id: content}
    """
    user_notifies = []
    for r in rs:
        coin = r[0]
        monitor_rate = r[1]
        user_notifies.extend([(user_id, coin, monitor_rate) for user_id in __get_notify_users(coin, monitor_rate)])

    user_notifies_map = {}
    f = lambda p: p[0]
    user_notifies.sort(key=f)
    for key, group in groupby(user_notifies, key=f):
        user_notifies_map[key] = list(group)

    ucm = {}
    for user_id in user_notifies_map.keys():
        items = user_notifies_map[user_id]
        cs = []
        for item in items:
            coin = item[1]
            monitor_rate = item[2]
            content = __get_user_notify_content(coin, monitor_rate, cts)
            cs.append(content)
        ucm[user_id] = '\r\n'.join(cs)

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
    # user = __get_user_info(user_id)
    return '{} 当前价格 {} ({}), 涨幅 {}%, 对比价格 {} ({})'.format(coin, cur_price, __format_time(cur_pk), rate, prev_price, __format_time(prev_pk))

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
    :param rs: list - [(coin, rate, monitor_rate, ticker)]
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

def __aggregate_by_coin():
    """
    通过币聚合配置信息
    :return: dict - {coin: [(user_id, coin, rate)]}
    """
    ss = __get_settings()
    if len(ss) == 0:
        return
    m = {}
    key_f = lambda p: p[1]
    ss = sorted(ss, key=key_f)
    for key, group in groupby(ss, key=key_f):
        m[key] = list(group)
    return m

def __get_settings():
    """
    获取所有配置信息
    :return: list - [(user_id, coin, rate)]
    """
    c = Mysql.conn.cursor()
    c.execute('select user_id, coin, rate from jb_price_rate_notify')
    return c.fetchall()

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
    return round((cur_price - prev_price) / prev_price * 100, 2)

def __get_prev_price_cache_key(coin, rate):
    """
    获取上次价格的缓存键
    :param coin: 
    :param rate: 监控幅度
    :return: 
    """
    return cache_rate_notify_price_prev_prefix + coin + "_" + str(rate)

def __mark_user_notify_info(user_id, pk, coin):
    """
    标记用户发送邮件时间。在发送邮件后需要。
    :param coin: 
    :return: 
    """
    info = RedisPool.conn.hget('', user_id)
    if info is None:
        info = {}
    else:
        info = eval(info)
    info[coin] = pk
    RedisPool.conn.hset('', user_id, info)

def __get_user_last_notify_time(user_id, coin):
    """
    获取上一次通知用户的时间
    :param user_id: 
    :param coin: 
    :return: 
    """
    info = RedisPool.conn.hget('', user_id)
    if info is None:
        return 0
    info = eval(info)
    if coin in info:
        pk = info[coin]
    else:
        pk = 0
    return pk

def work():
    print("Price rate notify monitor work")
    __notify()

def err_listener(event):
    if event.exception:
        exstr = traceback.format_exc()
        logger.error('The job crashed with exception : {0}'.format(event.exception))
        logger.error(exstr)

def mis_listener(event):
    logger.warning("Price notify job misfired at {}".format(time.strftime("%Y-%m-%d %X")))

if __name__ == '__main__':
    work()
    # conf = {
    #     'apscheduler.job_defaults.coalesce': 'false',
    #     'apscheduler.job_defaults.max_instances': '1'
    # }
    # sched = BlockingScheduler(conf)
    # sched.add_job(work, 'cron', second='0/10', hour='6-22')
    # sched.add_listener(err_listener, events.EVENT_JOB_ERROR)
    # sched.add_listener(mis_listener, events.EVENT_JOB_MISSED)
    #
    # try:
    #     sched.start()
    # except (KeyboardInterrupt, SystemExit):
    #     exstr = traceback.format_exc()
    #     logger.error(exstr)
