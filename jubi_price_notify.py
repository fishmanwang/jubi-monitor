import smtplib
from email.mime.text import MIMEText
from apscheduler import events
from apscheduler.schedulers.blocking import BlockingScheduler

from jubi_common import *

def __do_send_email(content, recv):
    mail_host = 'smtp.163.com'
    mail_user = 'tjwang516@163.com'
    mail_pass = 'Admin123'

    sender = r'tjwang516@163.com'
    #recvs = ['379590010@qq.com']
    recvs = [recv]

    #msg = MIMEText(r'阿希币价格已达 7 元', _charset='utf-8')
    msg = MIMEText(content, _charset='utf-8')
    msg['From'] = r'聚币监控程序'
    msg['To'] = ';'.join(recvs)
    msg['Subject'] = r'价格提醒'

    print(msg.as_string())

    try:
        smtp_obj = smtplib.SMTP()
        smtp_obj.connect(mail_host, 25)
        smtp_obj.login(mail_user, mail_pass)
        smtp_obj.sendmail(sender, recvs, msg.as_string())
        print('邮件发送成功')
    except smtplib.SMTPException as e:
        print(e)
        print("Error: 发送邮件失败")

all_coin_key = 'py_all_coins'

def __get_all_coins():
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

def get_candidate_price_notify(coin, price):
    """
    获取待提醒信息
    :param coin: 币代码
    :param name: 币名
    :param price: 
    :return: 
    """
    ups = []
    downs = []
    cursor = conn.cursor()
    cursor.execute("select user_id from jb_price_notify where coin=%s and price >=%s", (coin, price))
    if cursor.rowcount > 0:
        ups = cursor.fetchall()
    cursor.execute("select user_id from jb_price_notify where coin=%s and price <=%s", (coin, 0 - price))
    if cursor.rowcount > 0:
        downs = cursor.fetchall()
    conn.commit()
    cursor.close()
    ds = []
    for up in ups:
        ds.append(up[0])
    for down in downs:
        ds.append(down[0])
    return ds

def __notify_price_to_uesr(user_id, coin, name, price):
    cursor = conn.cursor()
    cursor.execute('select nickname, email from zx_account where user_id=%s', user_id)
    if cursor.rowcount == 0:
        return
    d = cursor.fetchone()
    conn.commit()
    cursor.close()

    nickname = d[0]
    email = d[1]
    content = r'{}{}({})币当前价格为 {} 元，请注意。'.format(nickname, name, coin, price)
    __do_send_email(content, email)


def coin_notify(coin, name):
    ticker_str = RedisPool.conn.get("cache_ticker_" + coin)
    if ticker_str is None:
        return
    ticker = eval(ticker_str)
    price = ticker['last']
    us = get_candidate_price_notify(coin, price)
    for u in us:
        __notify_price_to_uesr(u, coin, name, price)


def notify():
    """
    通知
    :return: 
    """
    coins = __get_all_coins()
    keys = coins.keys()
    for coin in keys:
        coin_notify(coin, coins[coin])


if __name__ == '__main__':
    notify()
    pass
