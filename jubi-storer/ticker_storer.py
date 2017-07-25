import traceback

from jb_common import *
from redis_repository import *
from ticker_repository import TickerRepository


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
