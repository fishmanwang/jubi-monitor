import time
import traceback
import smtplib
from email.mime.text import MIMEText
from apscheduler import events
from apscheduler.schedulers.blocking import BlockingScheduler

from jubi_common import *

def __send_email_to_user(user_id, infos):
    """
    发送邮件给用户
    :param user_id: 
    :param infos: 
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
    for info in infos:
        price = info[0]
        rate = info[1]
        coin = info[2]
        content = '{}, 当前 {} 价格为：{} 元，涨幅：{}%，请知悉。'.format(nickname, coin, price, rate)
        try:
            server = smtplib.SMTP_SSL(mail_host, mail_port)
            server.login(mail_user, mail_pass)
            content = content
            msg = MIMEText(content, _charset='utf-8')
            msg['From'] = 'tjwang516@163.com'
            msg['To'] = email
            msg['Subject'] = '聚币监控价格提醒'
            server.sendmail(sender, [email], msg.as_string())
            server.close()
        except smtplib.SMTPException:
            exstr = traceback.format_exc()
            logger.error("Error: 发送邮件失败。原因：" + exstr)

def __get_user_info(user_id):
    c = conn.cursor()
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
    cts = __get_current_tickers()
    for coin in keys:
        __do_notify(coin, m[coin], cts)

def __do_notify(coin, ss, tickers):
    """
    发送通知
    :param coin: 币 
    :param ss: 通知配置
    :param tickers: 当前行情
    :return: 
    """
    if len(tickers) == 0:
        return
    his = __get_history_tickers(coin)
    ticker = tickers[coin]
    for s in ss:
        userid = s[0]
        interval = s[2]
        rate = s[3]
    pass

def __do_coin_notify(coin, user_id, interval, rate, ticker, his):
    """
    根据币发送通知
    :parma coin: 币
    :param user_id: 用户ID 
    :param interval: 时间间隔
    :param rate: 涨跌幅
    :param ticker: 当前行情
    :param his: 历史数据
    :return: 
    """
    rs = []
    cur_pk = ticker[0]
    cp = ticker[2]
    min, max = __get_history_min_max(interval, cur_pk, his)
    minr = __get_rate(cp, min)
    if abs(minr) >= rate:
        pass
    maxr = __get_rate(cp, max)
    if abs(maxr) >= rate:
        pass
    pass

def __get_rate(cp, hp):
    """
    获取涨跌幅度
    :param cp: 
    :param hp: 
    :return: 
    """
    return round(((cp - hp) * 100) / hp, 2)

def __get_history_min_max(interval, cur_pk, his):
    """
    获取历史最大最小值
    :param interval: 间隔 
    :param cur_pk: 当前时间 
    :param his: 历史行情
    :return: 
    """
    pks = his.keys()
    prices = []
    for pk in pks:
        if (cur_pk - interval) >= pk:
            prices.append(his[pk])
    if len(prices) == 0:
        return 0, 0
    return min(prices), max(prices)

def __aggregate_by_coin():
    """
    通过币聚合配置信息
    :param ss: 
    :return: 
    """
    ss = __get_settings()
    if len(ss) == 0:
        return
    m = {}
    for s in ss:
        coin = s[1]
        mi = m[coin]
        if mi is None:
            mi = []
            m[coin] = mi
        mi.append(s)
    return m

def __get_settings():
    """
    获取所有配置信息
    :return: 
    """
    c = conn.cursor()
    c.execute('select user_id, coin, `interval`, rate from jb_price_rate_notify')
    return c.fetchall()

all_coin_key = 'py_all_coins'

def __get_all_coins():
    """
    获取所有币信息
    :return: 
    """
    coins = RedisPool.conn.get(all_coin_key)
    if coins is None:
        cursor = conn.cursor()
        cursor.execute("select code, name from jb_coin")
        ds = cursor.fetchall()
        conn.commit()
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

def __get_current_tickers():
    """
    获取当前行情信息
    :return: 
    """
    coins = __get_all_coins()
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
        t = eval(r)
        pk = t['pk']
        coin = t['coin']
        price = t['last']
        ts[coin] = (pk, coin, price)
    return ts

cache_coin_price_rate = "cache_price_rate_"

def __add_ticker(ticker):
    """
    将当前行情加入历史数据中
    :param ticker: 当前行情
    :return: 
    """
    pk = ticker[0]
    coin = ticker[1]
    price = ticker[2]
    RedisPool.conn.hsetnx(cache_coin_price_rate + coin, pk, price)

def __get_history_tickers(coin):
    """
    获取历史行情，一小时内行情数据
    :param coin: 币
    :return: 
    """
    return RedisPool.conn.hgetall(cache_coin_price_rate + coin)


def __clean_old_cache(ticker):
    """
    清除旧的行情信息。大于当前时间1小时
    :param ticker: 当前行情
    :return: 
    """
    pk = ticker[0]
    coin = ticker[1]
    keys = RedisPool.conn.hkeys(cache_coin_price_rate + coin)
    if len(keys) == 0:
        return
    for key in keys:
        if (pk - key) > 3600:
            RedisPool.conn.hdel(cache_coin_price_rate + coin, key)

def work():
    pass

def err_listener(event):
    if event.exception:
        exstr = traceback.format_exc()
        logger.error('The job crashed with exception : {0}'.format(event.exception))
        logger.error(exstr)

def mis_listener(event):
    logger.warning("Price notify job misfired at {}".format(time.strftime("%Y-%m-%d %X")))

if __name__ == '__main__':
    work()
