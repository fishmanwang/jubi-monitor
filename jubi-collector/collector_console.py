import time
import traceback
from apscheduler import events
from apscheduler.schedulers.blocking import BlockingScheduler

from ticker_collector import TickerCollector


def err_listener(event):
    if event.exception:
        print('The job crashed with exception : {0}'.format(event.exception))


def mis_listener(event):
    print("Collection job misfired at {}".format(time.strftime("%Y-%m-%d %X")))


if __name__ == '__main__':
    tc = TickerCollector()

    conf = {
        'apscheduler.job_defaults.coalesce': 'false',
        'apscheduler.job_defaults.max_instances': '1'
    }
    sched = BlockingScheduler(conf)
    sched.add_job(tc.collect, 'cron', second='0/5')
    sched.add_listener(err_listener, events.EVENT_JOB_ERROR)
    sched.add_listener(mis_listener, events.EVENT_JOB_MISSED)

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        exstr = traceback.format_exc()
        print(exstr)