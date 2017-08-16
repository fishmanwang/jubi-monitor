#coding=utf-8
import pymysql
import traceback

from jubi_common import *

tickers_map_key = "tickers_map_key"
last_pk = 0  # 最后一次抓取时间


def add_ticker(ts):
    sql = 'insert into jb_coin_ticker(pk, coin, price) values(%s, %s, %s)'
    c = conn.cursor()
    c.executemany(sql, ts)
    conn.commit()
    c.close()

def init_last_pk():
    """
    初始化最后一次抓取时间
    :return: 
    """
    global last_pk
    c = conn.cursor()
    c.execute("select pk from jb_coin_ticker order by pk desc limit 1")
    d = c.fetchone()
    if d:
        last_pk = d[0]

def store():
    init_last_pk()

    global last_pk
    while True:
        try:
            keys = RedisPool.conn.hkeys(tickers_map_key)
            for key in keys:
                ik = int(key)
                if ik > last_pk:
                    data = RedisPool.conn.hget(tickers_map_key, key)
                    __do_store(data.decode())
                    last_pk = ik
            for key in keys:
                ik = int(key)
                if ik < last_pk:
                    RedisPool.conn.hdel(tickers_map_key, key)
        except:
            exstr = traceback.format_exc()
            logger.warn(exstr)
        finally:
            time.sleep(10)

@cm_monitor("store")
def __do_store(data):
    tickers = eval(data)
    if not isinstance(tickers, list) or len(tickers) == 0:
        return
    ts = []
    for ticker in tickers:
        coin = ticker[0]
        data = ticker[1]
        pk = data[0]
        price = data[5]
        ts.append((pk, coin, price))
    add_ticker(ts)

if __name__ == '__main__':
    store()