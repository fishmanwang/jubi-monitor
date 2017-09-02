import traceback
import smtplib
from email.mime.text import MIMEText

from jubi_common import logger

def send_email(target, subject, content, callback=None, params=None):
    """
    发送邮件
    :param target: 目标邮箱 
    :param subject: 标题
    :param content: 内容
    :param callback: 回调函数
    :param params: 回调函数参数
    :return: 
    """
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
        msg['To'] = target
        msg['Subject'] = subject
        server.sendmail(sender, [target], msg.as_string())
        #print(content)
        if callback:
            callback(params)
        server.close()
    except smtplib.SMTPException:
        exstr = traceback.format_exc()
        logger.error("Error: 发送邮件失败。内容：" + content + "。原因：" + exstr)