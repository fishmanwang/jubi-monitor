import datetime
import traceback
import smtplib
from email.mime.text import MIMEText

from jubi_redis import *
import jubi_mysql as Mysql
from jubi_log import logger

# 发送短信服务

email_sending_queue_key = 'email_sending_queue'


def __send_email(user_id, subject, content, notify_type):
    u = __get_uesr_info(user_id)
    if not u:
        logger.warn("id为 {} 的用户不存在".format(user_id))
        return
    nickname = u[0]
    email = u[1]
    content = nickname + ":\r\n" + content

    succ, reason = __do_send_email(email, subject, content)
    __record(user_id, notify_type, email, succ, content, reason)

    pass


def __do_send_email(email, subject, content):
    mail_host = 'smtp.163.com'
    mail_port = 465
    mail_user = 'tjwang516@163.com'
    mail_pass = 'Admin123'
    sender = 'tjwang516@163.com'

    try:
        server = smtplib.SMTP_SSL(mail_host, mail_port)
        server.login(mail_user, mail_pass)
        msg = MIMEText(content, _charset='utf-8')
        msg['From'] = sender
        msg['To'] = email
        msg['Subject'] = subject
        server.sendmail(sender, [email], msg.as_string())
        # print(content)
        server.close()
        return True, ''
    except smtplib.SMTPException:
        exstr = traceback.format_exc()
        logger.error("Error: 发送邮件失败。内容：" + content + " 原因：" + exstr)
        return False, exstr


def __record(user_id, notify_type, email, succ, content, reason):
    content = content[:60].replace('\r\n', ' ')
    reason = reason[:60]
    send_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c = Mysql.conn.cursor()
    c.execute("insert into jubi_email_send_record(user_id, notify_type, email, succ, content, send_time, reason) \
              values (%s, %s, %s, %s, %s, %s, %s)", (user_id, notify_type, email, succ, content, send_time, reason))
    Mysql.conn.commit()
    c.close()


def __get_uesr_info(user_id):
    """
    获取用户信息
    :param user_id: 
    :return: tuple - (nickname, email) 
    """
    c = Mysql.conn.cursor()
    c.execute("select nickname, email from zx_account where user_id=%s", (user_id,))
    if c.rowcount == 0:
        return
    return c.fetchone()


def __work():
    while True:
        try:
            info = RedisPool.conn.blpop(email_sending_queue_key)  # (user_id, subject, content, notify_type)
            if not info:
                continue
            info = eval(info[1])
            user_id = info[0]
            subject = info[1]
            content = info[2]
            notify_type = info[3]
            __send_email(user_id, subject, content, notify_type)
        except:
            exstr= traceback.format_exc()
            logger.warn(exstr)

if __name__ == '__main__':
    __work()