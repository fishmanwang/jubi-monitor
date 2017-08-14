import traceback

from jubi_common import *
from apscheduler import events
from apscheduler.schedulers.blocking import BlockingScheduler


def clean_coin_ticker():
    """
    清理行情表，保留一天完整数据，其他的以一分钟为单位
    :return: 
    """
    t = get_day_begin_time_int()
    cursor = conn.cursor()
    cursor.execute("delete from jb_coin_ticker where (pk mod 60) != 0 and pk < %s", (t,))
    conn.commit()
    cursor.close()

def do_clean():
    """
    清理数据
    :return: 
    """
    pass

if __name__ == '__main__':
    pass