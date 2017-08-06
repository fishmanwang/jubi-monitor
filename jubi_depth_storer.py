import time
import decimal
import pymysql
import traceback
from jubi_common import conn

from jubi_common import RedisPool
from jubi_common import monitor
from jubi_common import logger


# redis中收集深度的队列的key
depth_coll_queue = 'depth_coll_queue'
conn.autocommit(False)


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

    cursor.execute("select price from jb_coin_ticker where coin=%s and pk <= %s ORDER BY pk DESC limit 1", (coin, pk))
    if cursor.rowcount == 0:
        conn.commit()
        return

    price = float(cursor.fetchone()[0])
    ps = __infer_plus(price, asks)
    ms = __infer_minus(price, bids)

    rate = 0
    pt = ps[len(ps)-1]
    mt = ms[len(ms)-1]
    if pt != 0 and mt != 0:
        rate = round(mt / pt, 2)

    val = (pk, coin, price) + ps + ms + (rate,)

    cursor.execute("insert into jb_coin_depth(pk, coin, asks, bids) VALUES (%s, %s, %s, %s)",
                   (pk, coin, str(asks), str(bids)))
    cursor.execute("insert into jb_coin_depth_rate(pk, coin, price, three_p, five_p, eight_p,\
                    ten_p, twenty_p, total_p, three_m, five_m, eight_m, ten_m, twenty_m, total_m, rate) values \
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", val)

    conn.commit()
    cursor.close()


def __infer_plus(price, asks):
    """
    推测涨幅
    :param price: 当前价格 
    :param asks: 挂单卖
    :return: 
    """
    three = price * 1.03
    five = price * 1.05
    eight = price * 1.08
    ten = price * 1.1
    twenty = price * 1.2

    three_p = 0
    five_p = 0
    eight_p = 0
    ten_p = 0
    twenty_p = 0
    total_p = 0

    max_price = asks[0][0]

    for ask in asks:
        p = ask[0]  # 委单价
        v = ask[1]  # 委单量
        total_p += p * v
        if max_price >= twenty and p <= twenty:
            twenty_p += p * v
        if max_price >= ten and p <= ten:
            ten_p += p * v
        if max_price >= eight and p <= eight:
            eight_p += p * v
        if max_price>= five and p <= five:
            five_p += p * v
        if max_price >= three and p <= three:
            three_p += p * v

    ret = (int(three_p), int(five_p), int(eight_p), int(ten_p), int(twenty_p), int(total_p))
    return ret


def __infer_minus(price, bids):
    """
    推测跌幅
    :param price: 当前价格 
    :param bids: 挂单卖
    :return: 
    """
    three = price * 0.97
    five = price * 0.95
    eight = price * 0.92
    ten = price * 0.9
    twenty = price * 0.8

    three_m = 0
    five_m = 0
    eight_m = 0
    ten_m = 0
    twenty_m = 0
    total_m = 0

    min_price = bids[len(bids) - 1][0]

    for bid in bids:
        p = bid[0]  # 委单价
        v = bid[1]  # 委单量
        total_m += p * v
        if min_price <= twenty and p >= twenty:
            twenty_m += p * v
        if min_price <= ten and p >= ten:
            ten_m += p * v
        if min_price <= eight and p >= eight:
            eight_m += p * v
        if min_price <= five and p >= five:
            five_m += p * v
        if min_price <= three and p >= three:
            three_m += p * v

    ret = (int(three_m), int(five_m), int(eight_m), int(ten_m), int(twenty_m), int(total_m))
    return ret


if __name__ == '__main__':
    work()
    pass