#coding=utf-8
import sys
import time
import pymysql
import redis
import logging
from functools import wraps
from DBUtils.PooledDB import PooledDB
from logging.handlers import RotatingFileHandler

import mysql_config as config

# redis中搜集tickers的队列名
tickers_key = "tickers"

# 操作information_schema连接
op_conn = pymysql.connect(host=config.DB_HOST, port=config.DB_PORT, user=config.DB_USER, passwd=config.DB_PASSWORD,
                          db="information_schema", charset="utf8")
# 普通连接
conn = pymysql.connect(host=config.DB_HOST, port=config.DB_PORT, user=config.DB_USER, passwd=config.DB_PASSWORD,
                       db=config.DB_DBNAME, charset="utf8")
conn.autocommit(True)

def monitor(text):
    """
    普通方法(class method)上监控调用时间
    :param text: 
    :return: 
    """
    def decorate(f):
        @wraps(f)
        def wrap(*args, **kw):
            start = int(time.time() * 1000)
            val = f(*args, **kw)
            end = int(time.time() * 1000)
            u = (end - start)
            if u < 1000:
                logger.debug("{0} time used: {1}".format(text, u))
            else:
                logger.warning("{0} time used: {1}".format(text, u))

            return val
        return wrap
    return decorate

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
            u = (end - start)
            if u < 1000:
                logger.debug("{0} time used: {1}".format(text, u))
            else:
                logger.warning("{0} time used: {1}".format(text, u))

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
                                   db=config.DB_DBNAME, use_unicode=False, charset=config.DB_CHARSET)
        return self.__pool.connection()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()


class RedisPool:
    conn = redis.StrictRedis(host='127.0.0.1', port=6379, db=0, max_connections=10)


logpath = sys.argv[1]

__formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

__fh = RotatingFileHandler(logpath + "/jubi.log", maxBytes=1024*1024*10, backupCount=3)
__fh.setLevel(logging.DEBUG)
__fh.setFormatter(__formatter)

__sh = logging.StreamHandler()
__sh.setLevel(logging.DEBUG)
__sh.setFormatter(__formatter)

logger = logging.getLogger("jubi")
logger.setLevel(logging.INFO)
logger.addHandler(__fh)
logger.addHandler(__sh)
logger.propagate = True

def get_all_coins():
    """
    获取所有币信息
    :return: [(code, name)]
    """
    cursor = conn.cursor()
    cursor.execute('select code, name from jb_coin')
    ds = cursor.fetchall()
    cursor.close()
    return ds