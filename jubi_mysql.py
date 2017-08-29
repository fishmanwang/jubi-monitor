import pymysql

import mysql_config as config

# 普通连接
conn = pymysql.connect(host=config.DB_HOST, port=config.DB_PORT, user=config.DB_USER, passwd=config.DB_PASSWORD,
                       db=config.DB_DBNAME, charset="utf8")
conn.autocommit(True)

