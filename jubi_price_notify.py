import smtplib
from email.mime.text import MIMEText
import traceback
from apscheduler import events
from apscheduler.schedulers.blocking import BlockingScheduler

from jubi_common import *

def __do_send_email(targets):
    """
    发送邮件给目标组
    :param targets: [(content, recv, callback, args), ...] 
    :return: 
    """
    if targets is None or len(targets) == 0:
        return
    
    mail_host = 'smtp.163.com'
    mail_port = 465
    mail_user = 'tjwang516@163.com'
    mail_pass = 'Admin123'
    sender = 'tjwang516@163.com'
    try:
        server = smtplib.SMTP_SSL(mail_host, mail_port)
        server.login(mail_user, mail_pass)
        for target in targets:
            content = target[0]
            recvs = [target[1]]
            msg = MIMEText(content, _charset='utf-8')
            msg['From'] = 'tjwang516@163.com'
            msg['To'] = ';'.join(recvs)
            msg['Subject'] = '聚币监控 - 价格提醒'
            server.sendmail(sender, recvs, msg.as_string())
            target[2](target[3])
    except smtplib.SMTPException:
        exstr = traceback.format_exc()
        logger.error("Error: 发送邮件失败。原因：" + exstr)

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

def get_candidate_price_notify_info(coin, price):
    """
    获取待提醒用户
    :param coin: 币代码
    :param name: 币名
    :param price: 
    :return: 
    """
    us = []
    cursor = conn.cursor()
    cursor.execute("select id, user_id from jb_price_notify \
                    where coin=%s and ((price <=%s AND price > 0) OR (price <= %s AND price < 0))",
                   (coin, price, 0-price))
    if cursor.rowcount > 0:
        us = cursor.fetchall()

    conn.commit()
    cursor.close()
    return us

def __get_notify_info(user_id, coin, name, price):
    """
    获取通知内容
    :param user_id: 用户ID
    :param coin: xas
    :param name: 阿希币
    :param price: 价格
    :return: 
    """
    cursor = conn.cursor()
    cursor.execute('select nickname, email from zx_account where user_id=%s', user_id)
    if cursor.rowcount == 0:
        return
    d = cursor.fetchone()
    conn.commit()
    cursor.close()

    nickname = d[0]
    email = d[1]
    content = '{}, {}({})当前价格为 {} 元，请知悉。'.format(nickname, name, coin, price)
    r = (content, email)
    return r

def coin_notify(coin, name):
    """
    指定币，发送通知
    :param coin: xas 
    :param name: 阿希币
    :return: 
    """
    ticker_str = RedisPool.conn.get("cache_ticker_" + coin)
    if ticker_str is None:
        return
    ticker = eval(ticker_str)
    price = ticker['last']
    us = get_candidate_price_notify_info(coin, price)
    targets = []
    for u in us:
        userId = u[1]
        ni = __get_notify_info(userId, coin, name, price)
        ni += (__delete_price_notify, u[0])
        targets.append(ni)
    if len(targets) > 0:
        __do_send_email(targets)

def __delete_price_notify(*args):
    """
    通知成功后，删除价格提醒设置
    :param args: 
    :return: 
    """
    pnid = args[0]
    cursor = conn.cursor()
    cursor.execute('delete from jb_price_notify where id = %s', (pnid, ))
    conn.commit()
    cursor.close()

@monitor("notify")
def notify():
    """
    通知
    :return: 
    """
    logger.info("execute price notify job")
    coins = __get_all_coins()
    if len(coins) == 0:
        return
    keys = coins.keys()
    for coin in keys:
        coin_notify(coin, coins[coin])

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
    sched.add_job(notify, 'cron', second='20/30')
    sched.add_listener(err_listener, events.EVENT_JOB_ERROR)
    sched.add_listener(mis_listener, events.EVENT_JOB_MISSED)

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        exstr = traceback.format_exc()
        logger.error(exstr)
