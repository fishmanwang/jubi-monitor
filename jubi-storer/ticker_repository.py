import pymysql
import traceback
from mysql_connection import ConnectionPool


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