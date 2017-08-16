#coding=utf-8
import json
import time
import random
import traceback
from urllib import request
from apscheduler import events
from apscheduler.schedulers.blocking import BlockingScheduler

from jubi_common import cm_monitor
from jubi_common import RedisPool
from jubi_common import logger

headers = {"User-Agent": '''Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 
                    (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'''}

current_tickers_key = "current_tickers_key"
tickers_map_key = "tickers_map_key"

class TickerCollector:
    """
    采集所有币种的行情
    """
    all_ticker_url = r"https://www.jubi.com/api/v1/allticker/?t={0}"
    ts_prefix = 'ts_'

    def __init__(self):
        pass

    @cm_monitor("TickerCollector.collect")
    def collect(self):
        t = int(time.time())
        t = t - t % 60
        self.__do_collect(t)
        pass

    def __do_collect(self, t):
        tickers = self.__get_tickers(t)

        ps = []
        for ticker in tickers:
            coin = ticker[0]
            data = ticker[1]
            ps.append([coin, data])
        logger.debug(t)
        if len(ps) == 0:
            return
        RedisPool.conn.set(current_tickers_key, ps, ex=3600)
        if not RedisPool.conn.hexists(tickers_map_key, t):
            RedisPool.conn.hsetnx(tickers_map_key, t, ps)

    @cm_monitor("TickerCollector.__get_ticker")
    def __get_tickers(self, pk):
        """
        获取币种行情
        :param pk: 抓取时间
        :return: 
        """
        url = self.all_ticker_url.format(random.random())
        req = request.Request(url=url, headers=headers)

        tickers = []
        with request.urlopen(req, timeout=3) as resp:
            d = json.loads(resp.read().decode())
            if len(d) < 20:
                logger.warning("Error response allticker : {}".format(d))
            for item in d.items():
                coin = item[0]
                d = item[1]
                data = (pk, round(d['high'], 6), round(d['low'], 6), round(d['buy'], 6), round(d['sell'], 6),
                        round(d['last'], 6), round(d['vol'], 6), round(d['volume'], 6))
                tickers.append((coin, data))

        return tickers


def err_listener(event):
    if event.exception:
        exstr = traceback.format_exc()
        logger.error('The job crashed with exception : {0}'.format(event.exception))
        logger.error(exstr)


def mis_listener(event):
    logger.warning("Collection job misfired at {}".format(time.strftime("%Y-%m-%d %X")))


if __name__ == '__main__':
    tc = TickerCollector()

    conf = {
        'apscheduler.job_defaults.coalesce': 'false',
        'apscheduler.job_defaults.max_instances': '1'
    }
    sched = BlockingScheduler(conf)
    sched.add_job(tc.collect, 'cron', second='0/10')
    sched.add_listener(err_listener, events.EVENT_JOB_ERROR)
    sched.add_listener(mis_listener, events.EVENT_JOB_MISSED)

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        exstr = traceback.format_exc()
        logger.error(exstr)