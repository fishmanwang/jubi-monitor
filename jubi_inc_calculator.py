#coding=utf-8

from jubi_common import *
from apscheduler.schedulers.blocking import BlockingScheduler

#涨幅计算

__pool = ConnectionPool()


class TickerRepository(object):

    def __init__(self, pool):
        self.pool = pool

    @cm_monitor("get_base_pk")
    def get_base_pk(self, time):
        """
        获取
        :param time: 指定时间
        :return: 
        """
        ds = self.get_all_tickers(time)
        print(ds)

    @staticmethod
    def get_next_pk(tb_name, time):
        """
        获取下一个取值点(比指定时间大一分钟（包含）且小于二分钟（不包含）第一个有值的pk)
        :param time: 上一次的取值点
        :return: 
        """
        next_time = (time - (time % 60)) + 60
        val = 0
        # 大于等于下一分钟小于后2分钟
        cursor = conn.cursor()
        cursor.execute("select pk from {} where pk >= %s limit 1".format(tb_name), (next_time,))
        if cursor.rowcount > 0:
            val = cursor.fetchone()[0]
        conn.commit()
        cursor.close()

        return val

    def get_all_tickers(self, time):
        """
        获取指定时间所有币的行情
        :param time: 
        :return: 
        """
        tbs = TickerRepository.get_tickers_table_name()
        ts = []
        cursor = conn.cursor()
        for tb in tbs:
            l = tb.index("_") + 1
            r = tb.rindex("_")
            coin = tb[l:r]

            t = TickerRepository.get_next_pk(tb, time)
            if t == 0:
                ts.append((coin, 0))
                continue

            cursor.execute('select last from {} where pk = %s'.format(tb), (t, ))
            if cursor.rowcount == 0:
                ts.append((coin, 0))
                continue

            d = cursor.fetchone()
            ts.append((coin, d[0]))
        conn.commit()
        cursor.close()
        return ts

    @staticmethod
    def get_tickers_table_name():
        """
        获取所有行情表表名
        :return: 
        """
        c = op_conn.cursor()
        c.execute('SELECT table_name FROM information_schema.tables WHERE table_schema = %s and table_name like %s', ('jubi', '%ticker'))
        raws = c.fetchall()
        op_conn.commit()
        c.close()
        return [raw[0] for raw in raws]


class TickerIncRepository(object):
    """
    行情涨幅
    """
    def __init__(self, pool):
        self.pool = pool

    def get_last_item(self):
        """
        获取数据库中最晚的时间
        :return: 
        """
        with self.pool as db:
            cursor = db.cursor
            cursor.execute('select pk from jb_coin_increase order by pk limit 1')
            if cursor.rowcount == 0:
                return 0
            raw = cursor.fetchone()
            return raw[0]


if __name__ == '__main__':
    tir = TickerIncRepository(__pool)
    r = tir.get_last_item()

    tr = TickerRepository(__pool)
    tr.get_base_pk(0)
