#coding=utf-8
import os
import sys
import time
import pymysql
import redis
import logging
from functools import wraps
from logging.handlers import RotatingFileHandler

import mysql_config as config

# redis中搜集tickers的队列名
tickers_key = "tickers"

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


class RedisPool:
    conn = redis.StrictRedis(host='127.0.0.1', port=6379, db=0, max_connections=10)

day_format = '%Y-%m-%d'

def get_day_time(t):
    """
    将数字转化为 年-月-日
    :param t: 
    :return: 
    """
    ta = time.localtime(t)
    return time.strftime(day_format, ta)

def get_day_begin_time_int(t):
    """
    获取当天开始时间( 00:00:00 )的int值
    :return: 
    """
    lt = time.localtime(t)
    s = time.strftime(day_format, lt)
    p = time.strptime(s, day_format)
    return int(time.mktime(p))

if len(sys.argv) > 1:
    log_path = sys.argv[1]
else:
    fn = os.path.splitext(os.path.split(sys.argv[0])[1])[0]
    path = '/var/projects/jubi-monitor/logs'
    if not os.path.exists(path):
        os.makedirs(path)
    log_path = path + '/{}.log'.format(fn)


__formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

__fh = RotatingFileHandler(log_path, maxBytes=1024*1024*10, backupCount=3)
__fh.setLevel(logging.DEBUG)
__fh.setFormatter(__formatter)

__sh = logging.StreamHandler()
__sh.setLevel(logging.DEBUG)
__sh.setFormatter(__formatter)

logger = logging.getLogger("jubi")
logger.setLevel(logging.DEBUG)
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