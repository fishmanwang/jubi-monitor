#coding=utf-8
import time
import pymysql
import redis
from functools import wraps
from DBUtils.PooledDB import PooledDB

import mysql_config as config

tickers_key = "tickers"

def cm_monitor(text):
    """
    类方法(class method)上监控调用时间
    :param text: 
    :return: 
    """
    def decorate(f):
        @wraps(f)
        def wrap(self, *args, **kw):
            start = int(time.time() * 1000)
            val = f(self, *args, **kw)
            end = int(time.time() * 1000)
            print("{0} time used: {1}".format(text, (end - start)))
            return val
        return wrap
    return decorate

class ConnectionPool:
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
                                   db=config.DB_DBNAME, use_unicode=False, charset=config.DB_CHARSET);
        return self.__pool.connection()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

class RedisPool:
    conn = redis.StrictRedis(host='127.0.0.1', port=6379, db=0, max_connections=10)