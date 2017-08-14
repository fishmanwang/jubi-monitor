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
from jubi_common import tickers_key
from jubi_common import logger

headers = {"User-Agent": '''Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 
                    (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36''',
           "connection": "keep-alive"}

time_span = 5

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
        t = t - t % time_span
        self.__do_collect(t)
        pass

    def __do_collect(self, t):
        tickers = self.__get_tickers(t)

        ps = []
        for ticker in tickers:
            coin = ticker[0]
            data = ticker[1]
            ps.append([coin, data])
        tn = int(time.time())
        keynum = tn - tn % time_span
        logger.debug(keynum)
        try:
            timekey = TickerCollector.ts_prefix + str(keynum)
            if len(ps) == 0:
                logger.debug("emtpy ticker : %s" % timekey)
            if RedisPool.conn.set(timekey, ps, ex=3600, nx=True) == 1:
                RedisPool.conn.rpush(tickers_key, timekey)
            else:
                logger.debug('value exists with key : %s' % timekey)
        except Exception as e:
            RedisPool.conn.delete(timekey)
            exstr = traceback.format_exc()
            logger.error(exstr)

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
        try:
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

        except Exception as e:
            exstr = traceback.format_exc()
            logger.error(exstr)
            time.sleep(10)

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
    sched.add_job(tc.collect, 'cron', second='0/{}'.format(time_span))
    sched.add_listener(err_listener, events.EVENT_JOB_ERROR)
    sched.add_listener(mis_listener, events.EVENT_JOB_MISSED)

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        exstr = traceback.format_exc()
        logger.error(exstr)