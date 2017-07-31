#coding=utf-8
import pymysql
import traceback
from jubi_common import ConnectionPool

from jubi_common import RedisPool
from jubi_common import tickers_key
from jubi_common import cm_monitor
from jubi_common import logger


class TickerRepository:
    """
    行情
    """
    __pool = ConnectionPool()

    @staticmethod
    def get_coin_ticker_table_name(coin):
        return '_'.join(['jb', coin, 'ticker'])

    def __init__(self):
        pass

    def add_ticker(self, ts):
        sql = 'insert into jb_coin_ticker(pk, coin, price) values(%s, %s, %s)'
        with self.__pool as db:
            db.cursor.executemany(sql, ts)
            db.conn.commit()


class TickerStorer:
    rep = TickerRepository()

    def store(self):
        while True:
            keys = RedisPool.conn.blpop(tickers_key)
            key = keys[1].decode()
            val = RedisPool.conn.get(key)
            if val is not None:
                self.__do_store(key, val)

    @cm_monitor("store")
    def __do_store(self, key, data):
        if data is not None:
            tickers = eval(data)
            if isinstance(tickers, list) and len(tickers) > 0:
                try:
                    ts = []
                    for ticker in tickers:
                        coin = ticker[0]
                        data = ticker[1]
                        pk = data[0]
                        price = data[5]
                        ts.append((pk, coin, price))
                    self.rep.add_ticker(ts)
                    RedisPool.conn.delete(key)
                except pymysql.err.IntegrityError as e:
                    exstr = traceback.format_exc()
                    logger.error(exstr)
                except Exception as e:
                    RedisPool.conn.lpush(tickers_key, key)
                    exstr = traceback.format_exc()
                    logger.error(exstr)
        else:
            logger.debug("data is is None associated by %s" % key)


if __name__ == '__main__':
    ts = TickerStorer()
    ts.store()