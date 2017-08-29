import os
import sys
import logging
from logging.handlers import RotatingFileHandler

if len(sys.argv) > 1:
    log_path = sys.argv[1]
else:
    fn = os.path.basename(sys.argv[0]).split('.')[0]

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
logger.setLevel(logging.INFO)
logger.addHandler(__fh)
logger.addHandler(__sh)
logger.propagate = True