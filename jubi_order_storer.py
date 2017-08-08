import time
import pymysql
import traceback
from jubi_common import conn

from jubi_common import RedisPool
from jubi_common import monitor
from jubi_common import logger



# redis中收集深度的队列的key
order_coll_queue = 'order_coll_queue'
conn.autocommit(False)


def work():
    while True:
        try:
            keys = RedisPool.conn.blpop(order_coll_queue)
            key = keys[1].decode()
            b = key.index("_")
            e = key.rindex("_")
            pk = key[b+1:e]
            coin = key[e+1:]
            val = RedisPool.conn.get(key)
            if val is None or len(val) == 0:
                continue
            __process_data(pk, coin, val)
            RedisPool.conn.expire(key, 60)
        except pymysql.err.OperationalError:
            # mysql异常，将key和值从新放入redis
            exstr = traceback.format_exc()
            logger.error(exstr)
            RedisPool.conn.lpush(order_coll_queue, key)
            # 数据库异常，特指连接断开，休眠1分钟，等待恢复
            time.sleep(60)
        except:
            exstr = traceback.format_exc()
            logger.error(exstr)


@monitor("__process_data")
def __process_data(pk, coin, val):
    trds = eval(val)
    last_tid = get_last_tid(coin)
    ds = []
    for trd in trds:
        tid = int(trd['tid'])
        price = trd['price']
        amount = trd['amount']
        trade_time = trd['date']

        type = trd['type']
        if type == 'sell':
            price = 0 - price

        if tid > last_tid:
            d = (tid, coin, price, amount, trade_time)
            ds.append(d)
    __store(ds)

def get_last_tid(coin):
    conn.connect()
    cursor = conn.cursor()

    cursor.execute('select tid from jb_coin_order where coin = %s order by tid desc limit 1', (coin, ))
    if cursor.rowcount == 0:
        return 0
    r = cursor.fetchone()[0]

    conn.commit()
    cursor.close()
    return r


def __store(ds):
    conn.connect()
    cursor = conn.cursor()

    cursor.executemany("insert into jb_coin_order(tid, coin, price, amount, trade_time) VALUES (%s, %s, %s, %s, %s)",
                       ds)

    conn.commit()
    cursor.close()

if __name__ == '__main__':
    work()
    pass