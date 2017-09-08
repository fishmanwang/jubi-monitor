import time
from functools import wraps

from jubi_log import logger

def monitor(text, long_time=1000):
    """
    普通方法(class method)上监控调用时间
    :param text: 监控的行为名称
    :param long_time: 超过时长的记录
    :return: 
    """
    def decorate(f):
        @wraps(f)
        def wrap(*args, **kw):
            start = int(time.time() * 1000)
            val = f(*args, **kw)
            end = int(time.time() * 1000)
            u = (end - start)
            if u < long_time:
                logger.debug("{0} time used: {1}".format(text, u))
            else:
                logger.warning("{0} time used: {1}".format(text, u))

            return val
        return wrap
    return decorate

def cm_monitor(text, long_time=1000):
    """
    类方法(class method)上监控调用时间
    :param text: 监控的行为名称
    :param long_time: 超过时长的记录
    :return: 
    """
    def decorate(f):
        @wraps(f)
        def wrap(self, *args, **kw):
            start = int(time.time() * 1000)
            val = f(self, *args, **kw)
            end = int(time.time() * 1000)
            u = (end - start)
            if u < long_time:
                logger.debug("{0} time used: {1}".format(text, u))
            else:
                logger.warning("{0} time used: {1}".format(text, u))

            return val
        return wrap
    return decorate