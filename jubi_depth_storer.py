import time
import pymysql
import traceback
from jubi_common import conn

from jubi_common import RedisPool
from jubi_common import monitor
from jubi_common import logger


# redis中收集深度的队列的key
depth_coll_queue = 'depth_coll_queue'


def work():
    while True:
        try:
            keys = RedisPool.conn.blpop(depth_coll_queue)
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
        except pymysql.err.DatabaseError:
            # mysql异常，将key和值从新放入redis
            exstr = traceback.format_exc()
            logger.error(exstr)
            RedisPool.conn.lpush(depth_coll_queue, key)
            # 数据库异常，特指连接断开，休眠1分钟，等待恢复
            time.sleep(60)
        except:
            exstr = traceback.format_exc()
            logger.error(exstr)


@monitor("__process_data")
def __process_data(pk, coin, val):
    depths = eval(val)
    asks = depths['asks']
    bids = depths['bids']
    __store(pk, coin, asks, bids)


def __store(pk, coin, asks, bids):
    conn.connect()
    cursor = conn.cursor()
    cursor.execute("insert into jb_coin_depth(pk, coin, asks, bids) VALUES (%s, %s, %s, %s)",
                   (pk, coin, str(asks), str(bids)))
    conn.commit()
    cursor.close()


if __name__ == '__main__':
    work()
    pass