
pip install apscheduler
pip install pymysql
pip install DBUtils
pip install redis

python jubi_collector.py /var/projects/jubi-monitor/logs/collector
python jubi_storer.py /var/projects/jubi-monitor/logs/storer
python jubi_inc_calculator.py /var/projects/jubi-monitor/logs/storer