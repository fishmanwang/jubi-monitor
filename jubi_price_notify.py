import traceback
from apscheduler import events
from apscheduler.schedulers.blocking import BlockingScheduler

from jubi_common_func import send_email
from jubi_common_func import all_coin_key
from jubi_redis import *
import jubi_mysql as Mysql
from jubi_aop_monitor import *

def __get_all_coins():
    """
    获取所有币信息
    :return: 
    """
    coins = RedisPool.conn.get(all_coin_key)
    if coins is None:
        with Mysql.pool as db:
            cursor = db.cursor
            cursor.execute("select code, name from jb_coin")
            ds = cursor.fetchall()
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
    :return: (id, user_id)
    """
    us = []
    with Mysql.pool as db:
        cursor = db.cursor
        cursor.execute("select id, user_id from jb_price_notify \
                        where coin=%s and ((price <=%s AND price > 0) OR (price <= %s AND price < 0))",
                       (coin, price, 0-price))
        if cursor.rowcount > 0:
            us = cursor.fetchall()
    return us


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

    if len(us) == 0:
        return

    subject = '聚币监控 - 价格提醒'
    content = '{}({})当前价格为 {} 元，请知悉。'.format(name, coin.upper(), price)

    user_set = set()
    del_ids = []
    for u in us:
        del_ids.append(u[0])
        user_set.add(u[1])

    for user_id in user_set:
        send_email(user_id, subject, content, 1)

    __delete_price_notify(del_ids)

def __delete_price_notify(ids):
    """
    通知成功后，删除价格提醒设置
    :param ids: list 
    :return: 
    """
    with Mysql.pool as db:
        cursor = db.cursor
        cursor.execute('delete from jb_price_notify where id in %s', (ids, ))
        db.conn.commit()

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
    sched.add_job(notify, 'cron', second='0/10', hour='6-22')
    sched.add_listener(err_listener, events.EVENT_JOB_ERROR)
    sched.add_listener(mis_listener, events.EVENT_JOB_MISSED)

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        exstr = traceback.format_exc()
        logger.error(exstr)
