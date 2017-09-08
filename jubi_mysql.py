import pymysql
from DBUtils.PooledDB import PooledDB

import mysql_config as config

class MysqlPool:
    __pool = None

    def __enter__(self):
        self.conn = self.__get_conn()
        self.cursor = self.conn.cursor()
        return self

    def __get_conn(self):
        if self.__pool is None:
            self.__pool = PooledDB(creator=pymysql, mincached=config.DB_MIN_CACHED, maxcached=config.DB_MAX_CACHED,
                                   maxshared=config.DB_MAX_SHARED, maxconnections=config.DB_MAX_CONNECYIONS,
                                   blocking=config.DB_BLOCKING, maxusage=config.DB_MAX_USAGE,
                                   setsession=config.DB_SET_SESSION,
                                   host=config.DB_HOST, port=config.DB_PORT,
                                   user=config.DB_USER, passwd=config.DB_PASSWORD,
                                   db=config.DB_DBNAME, use_unicode=True, charset=config.DB_CHARSET)
        return self.__pool.connection()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

pool = MysqlPool()

# 普通连接
conn = pymysql.connect(host=config.DB_HOST, port=config.DB_PORT, user=config.DB_USER, passwd=config.DB_PASSWORD,
                       db=config.DB_DBNAME, charset="utf8")
conn.autocommit(True)