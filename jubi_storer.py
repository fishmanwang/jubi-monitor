#coding=utf-8
import pymysql
import traceback
from jubi_common import ConnectionPool

from jubi_common import RedisPool
from jubi_common import tickers_key
from jubi_common import cm_monitor

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

    def __create_table(self, table_name):
        with self.__pool as db:
            cursor = db.conn.cursor()
            cursor.execute('CREATE TABLE {0}(pk INTEGER NOT NULL PRIMARY KEY, high DECIMAL(18,6) NOT NULL, \
                                    low DECIMAL(18,6) NOT NULL, buy DECIMAL(18,6) NOT NULL, \
                                    sell DECIMAL(18,6) NOT NULL, last DECIMAL(18,6) NOT NULL, \
                                    vol DECIMAL(18,6) NOT NULL, volume DECIMAL(18,6) NOT NULL)'.format(table_name))
            db.conn.commit()

    def add_ticker(self, *args):
        coin = args[0]
        data = args[1]
        if data is None or len(data) == 0:
            print("无效的ticker数据{0}".format(data))
            return

        tn = TickerRepository.get_coin_ticker_table_name(coin)

        try:
            sql = 'insert into {0}(pk, high, low, buy, sell, last, vol, volume) \
                values(%s, %s, %s, %s, %s, %s, %s, %s)'.format(tn)
            with self.__pool as db:
                db.cursor.execute(sql, data)
                db.conn.commit()
        except pymysql.err.ProgrammingError as pe:
            tname = TickerRepository.get_coin_ticker_table_name(coin)
            self.__create_table(tname)
        except Exception as e:
            exstr = traceback.format_exc()
            print(exstr)


class TickerStorer:
    rep = TickerRepository()

    def store(self):

        while True:
            keys = RedisPool.conn.blpop(tickers_key)
            key = keys[1].decode()
            val = RedisPool.conn.get(key)
            self.__do_store(key, val)

    @cm_monitor("store")
    def __do_store(self, key, data):
        if data is not None:
            tickers = eval(data)
            if isinstance(tickers, list) and len(tickers) > 0:
                try:
                    for ticker in tickers:
                        coin = ticker[0]
                        data = ticker[1]
                        self.rep.add_ticker(coin, data)
                        RedisPool.conn.delete(key)
                except Exception as e:
                    RedisPool.conn.lpush(tickers_key, key)
                    exstr = traceback.format_exc()
                    print(exstr)
        else:
            print("data is is None associated by %s" % key)


if __name__ == '__main__':
    ts = TickerStorer()
    ts.store()