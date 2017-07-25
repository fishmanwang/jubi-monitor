import pymysql
from DBUtils.PooledDB import PooledDB

import mysql_config as Config


class ConnectionPool:
    __pool = None

    def __enter__(self):
        self.conn = self.__get_conn()
        self.cursor = self.conn.cursor()
        return self

    def __get_conn(self):
        if self.__pool is None:
            self.__pool = PooledDB(creator=pymysql, mincached=Config.DB_MIN_CACHED, maxcached=Config.DB_MAX_CACHED,
                                   maxshared=Config.DB_MAX_SHARED, maxconnections=Config.DB_MAX_CONNECYIONS,
                                   blocking=Config.DB_BLOCKING, maxusage=Config.DB_MAX_USAGE,
                                   setsession=Config.DB_SET_SESSION,
                                   host=Config.DB_HOST, port=Config.DB_PORT,
                                   user=Config.DB_USER, passwd=Config.DB_PASSWORD,
                                   db=Config.DB_DBNAME, use_unicode=False, charset=Config.DB_CHARSET);
        return self.__pool.connection()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()