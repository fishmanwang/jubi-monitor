import traceback
from apscheduler import events
from apscheduler.schedulers.blocking import BlockingScheduler

from jubi_common_func import *
from jubi_aop_monitor import *
import jubi_mysql as Mysql

def get_days_ago_begin_time(t, n):
    """
    获取n天前初始时间(00:00:00)
    :param t: 
    :param n: 
    :return: 
    """
    return get_day_begin_time_int(t - 86400 * n)

@monitor("clean_coin_ticker")
def clean_coin_ticker():
    """
    清理行情表。
    1. 保留一天完整数据，其他的以一分钟为单位
    2. 七天以前的数据，以十分钟为单位
    :return: 
    """
    try:
        now = time.time()
        t = get_day_begin_time_int(now)
        seven_t = get_days_ago_begin_time(now, 7)

        with Mysql.pool as db:
            cursor = db.cursor
            cursor.execute("delete from jb_coin_ticker where (pk mod 600) != 0 and pk < %s", (seven_t,))
            db.conn.commit()
    except Exception:
        exstr = traceback.format_exc()
        logger.error("clean_coin_ticker failed")
        logger.error(exstr)

@monitor("clean_coin_rate")
def clean_coin_rate():
    """
    清理涨幅表，七天以前的数据，以十分钟为单位
    :return: 
    """
    try:
        now = time.time()
        three_t = get_days_ago_begin_time(now, 3)
        seven_t = get_days_ago_begin_time(now, 7)

        with Mysql.pool as db:
            cursor = db.cursor
            cursor.execute("delete from jb_coin_rate where (pk mod 600) != 0 and pk < %s", (three_t,))
            db.conn.commit()
    except Exception:
        exstr = traceback.format_exc()
        logger.error("clean_coin_ticker failed")
        logger.error(exstr)

@monitor("clean_coin_depth")
def clean_coin_depth():
    """
    清除三天前数据
    :return: 
    """
    try:
        now = time.time()
        t = get_days_ago_begin_time(now, 3)
        with Mysql.pool as db:
            cursor = db.cursor
            cursor.execute("delete from jb_coin_depth where pk < %s", (t,))
            db.conn.commit()
    except Exception:
        exstr = traceback.format_exc()
        logger.error("clean_coin_ticker failed")
        logger.error(exstr)

def do_clean():
    """
    清理数据
    :return: 
    """
    clean_coin_ticker()
    clean_coin_rate()
    clean_coin_depth()
    logger.info("clean job finished")

def err_listener(event):
    if event.exception:
        exstr = traceback.format_exc()
        logger.error('The cleaner job crashed with exception : {0}'.format(event.exception))
        logger.error(exstr)


def mis_listener(event):
    logger.warning("The cleaner job misfired at {}".format(time.strftime("%Y-%m-%d %X")))


if __name__ == '__main__':
    conf = {
        'apscheduler.job_defaults.coalesce': 'false',
        'apscheduler.job_defaults.max_instances': '1'
    }
    sched = BlockingScheduler(conf)
    sched.add_job(do_clean, 'cron', hour='1')
    sched.add_listener(err_listener, events.EVENT_JOB_ERROR)
    sched.add_listener(mis_listener, events.EVENT_JOB_MISSED)

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        exstr = traceback.format_exc()
        logger.error(exstr)