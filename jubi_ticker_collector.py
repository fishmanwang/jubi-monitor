#coding=utf-8
import json
import random
import traceback
from urllib import request
from apscheduler import events
from apscheduler.schedulers.blocking import BlockingScheduler

from jubi_aop_monitor import *
from jubi_redis import *

headers = {"User-Agent": '''Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 
                    (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'''}

tickers_map_key = "tickers_map_key"
span = 10  # 抓取时间间隔

class TickerCollector:
    """
    采集所有币种的行情
    """
    all_ticker_url = r"https://www.jubi.com/api/v1/allticker/?t={0}"

    def __init__(self):
        pass

    @cm_monitor("TickerCollector.collect")
    def collect(self):
        t = int(time.time())
        self.__do_collect(t)
        pass

    def __do_collect(self, t):
        pk = t - t % span
        tickers = self.__get_tickers(pk)

        t = t - t % 60  # 每一分钟存储一条数据到数据库
        ps = []
        for ticker in tickers:
            coin = ticker[0]
            data = ticker[1]
            ps.append([coin, data])
        logger.debug(t)
        if len(ps) == 0:
            return
        self.cache_current_tickers(tickers)
        ex = RedisPool.conn.hexists(tickers_map_key, t)
        logger.debug(ex)
        if not ex:
            logger.debug("do add")
            RedisPool.conn.hsetnx(tickers_map_key, t, ps)

    def cache_current_tickers(self, tickers):
        """
        将当当前行情缓存，供web端使用
        :param tickers: 
        :return: 
        """
        for ticker in tickers:
            coin = ticker[0]
            data = ticker[1]
            d = {}
            d["coin"] = coin
            d['pk'] = data[0]
            d['high'] = data[1]
            d['low'] = data[2]
            d['buy'] = data[3]
            d['sell'] = data[4]
            d['last'] = data[5]
            d['vol'] = data[6]
            d['volume'] = data[7]
            RedisPool.conn.set("cache_ticker_{}".format(coin), str(d).replace('\'', '\"'))

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
                data = [pk, round(d['high'], 6), round(d['low'], 6), round(d['buy'], 6), round(d['sell'], 6),
                        round(d['last'], 6), round(d['vol'], 6), round(d['volume'], 6)]
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
    sched.add_job(tc.collect, 'cron', second='0/{}'.format(span))
    sched.add_listener(err_listener, events.EVENT_JOB_ERROR)
    sched.add_listener(mis_listener, events.EVENT_JOB_MISSED)

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        exstr = traceback.format_exc()
        logger.error(exstr)