import time
import traceback
import smtplib
from email.mime.text import MIMEText
from apscheduler import events
from apscheduler.schedulers.blocking import BlockingScheduler

from jubi_common import *

def __send_email(recv, content):
    """
    发送短信
    :param recv: 收件人 
    :param content: 内容
    :return: 
    """
    mail_host = 'smtp.163.com'
    mail_port = 465
    mail_user = 'tjwang516@163.com'
    mail_pass = 'Admin123'
    sender = 'tjwang516@163.com'
    try:
        server = smtplib.SMTP_SSL(mail_host, mail_port)
        server.login(mail_user, mail_pass)
        content = content
        recvs = [recv]
        msg = MIMEText(content, _charset='utf-8')
        msg['From'] = 'tjwang516@163.com'
        msg['To'] = ';'.join(recvs)
        msg['Subject'] = '聚币监控价格提醒'
        server.sendmail(sender, recvs, msg.as_string())
        server.close()
    except smtplib.SMTPException:
        exstr = traceback.format_exc()
        logger.error("Error: 发送邮件失败。原因：" + exstr)

def __get_prev_price(coin):
    """
    获取上次提醒价格
    :param coin: 币
    :return: 
    """
    price = RedisPool.conn.get("cache_ticker_prev_" + coin)
    if price is None:
        return 0
    return float(price)

def __set_prev_price(coin, price):
    """
    设置上次提醒价格
    :param coin: 
    :return: 
    """
    if price is None:
        price = 0
    RedisPool.conn.set("cache_ticker_prev_" + coin, price)

def work():
    coin = 'xas'
    recv = '570366997@qq.com'
    #recv = '379590010@qq.com'
    ticker_str = RedisPool.conn.get("cache_ticker_" + coin)
    if ticker_str is None:
        return
    ticker = eval(ticker_str)
    price = ticker['last']
    prev_price = __get_prev_price(coin)
    __set_prev_price(coin, price)
    if prev_price == 0:
        content = '当前阿希币价格为：{} 元，请知悉。'.format(price)
    else:
        rate = str(round((price - prev_price)*100/prev_price, 2)) + "%"
        content = '当前阿希币价格为：{} 元，之前价格：{} 元，涨幅： {} ，请知悉。'.format(price, prev_price, rate)
    __send_email(recv, content)

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
    sched.add_job(work, 'cron', minute='0/20', hour='6-23')
    sched.add_listener(err_listener, events.EVENT_JOB_ERROR)
    sched.add_listener(mis_listener, events.EVENT_JOB_MISSED)

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        exstr = traceback.format_exc()
        logger.error(exstr)
