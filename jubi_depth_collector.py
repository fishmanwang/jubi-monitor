#coding=utf-8
import json
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

# redis中收集深度的队列的key
depth_coll_queue = 'depth_coll_queue'
# redis中depth键的前缀
depth_key_prefix = 'depth_'

headers = {"User-Agent": '''Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 
                    (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'''}
depth_url = r"https://www.jubi.com/api/v1/depth/?coin={}&t={}"


def __do_collect(coin, pk):
    """
    收集数据
    :param coin: 
    :return: 
    """
    url = depth_url.format(coin, random.random())
    req = request.Request(url=url, headers=headers)
    key = depth_key_prefix + str(pk) + "_" + coin
    try:
        if RedisPool.conn.exists(key) == 1:
            print("exists : " + key)
            return
        with request.urlopen(req, timeout=3) as resp:
            d = resp.read().decode()
            if RedisPool.conn.set(key, d, ex=3600, nx=True) == 1:
                RedisPool.conn.rpush(depth_coll_queue, key)
    except:
        exstr = traceback.format_exc()
        logger.warn(exstr)


@monitor("collect")
def collect(*args):
    coins = args[0]
    tn = int(time.time())
    pk = tn - tn % 60
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
    cs = get_all_coins()
    coins = []
    for c in cs:
        coins.append(c[0])

    conf = {
        'apscheduler.job_defaults.coalesce': 'false',
        'apscheduler.job_defaults.max_instances': '1'
    }
    sched = BlockingScheduler(conf)
    sched.add_job(collect, 'cron', second='0/20', args=(coins,))
    sched.add_listener(err_listener, events.EVENT_JOB_ERROR)
    sched.add_listener(mis_listener, events.EVENT_JOB_MISSED)

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        exstr = traceback.format_exc()
        logger.error(exstr)