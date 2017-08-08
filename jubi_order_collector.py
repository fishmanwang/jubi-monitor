#coding=utf-8
import ssl
import time
import random
import traceback
from urllib import request
from apscheduler import events
from apscheduler.schedulers.blocking import BlockingScheduler

from jubi_common import monitor
from jubi_common import RedisPool
from jubi_common import get_all_coins
from jubi_common import logger

# redis中收集订单的队列的key
order_coll_queue = 'order_coll_queue'
# redis中订单键的前缀
order_key_prefix = 'order_'

headers = {"User-Agent": '''Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 
                    (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'''}
context = ssl._create_unverified_context()
depth_url = r"https://www.jubi.com/api/v1/orders/?coin={}&t={}"

execute_span = 40

def __do_collect(coin, pk):
    """
    收集数据
    :param coin: 
    :return: 
    """
    url = depth_url.format(coin, random.random())
    req = request.Request(url=url, headers=headers)

    key = order_key_prefix + str(pk) + "_" + coin
    try:
        if RedisPool.conn.exists(key) == 1:
            print("exists : " + key)
            return

        time.sleep(0.2)
        with request.urlopen(req, timeout=3, context=context) as resp:
            d = resp.read().decode()
            if RedisPool.conn.set(key, d, ex=3600, nx=True) == 1:
                RedisPool.conn.rpush(order_coll_queue, key)
    except:
        exstr = traceback.format_exc()
        logger.warn(exstr)
        logger.warn("collect {} error".format(key))
        time.sleep(2)  # 被服务器拒绝之后，休眠2s


@monitor("collect")
def collect():
    cs = get_all_coins()
    coins = []
    for c in cs:
        coins.append(c[0])

    tn = int(time.time())
    pk = tn - tn % execute_span
    for coin in coins:
        __do_collect(coin, pk)


def err_listener(event):
    if event.exception:
        exstr = traceback.format_exc()
        logger.error('The job crashed with exception : {0}'.format(event.exception))
        logger.error(exstr)


def mis_listener(event):
    logger.warning("Collection job misfired at {}".format(time.strftime("%Y-%m-%d %X")))


if __name__ == '__main__':
    conf = {
        'apscheduler.job_defaults.coalesce': 'false',
        'apscheduler.job_defaults.max_instances': '1'
    }
    sched = BlockingScheduler(conf)
    sched.add_job(collect, 'cron', second='0/{}'.format(execute_span))
    sched.add_listener(err_listener, events.EVENT_JOB_ERROR)
    sched.add_listener(mis_listener, events.EVENT_JOB_MISSED)

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        exstr = traceback.format_exc()
        logger.error(exstr)