#!/usr/bin/python
# -*- coding: UTF-8 -*-
from buildings.comm_across import *
import smtplib
import sys
import email.mime.multipart
import email.mime.text
import email.mime.application
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
# from email.mime.application import MIMEApplication
senders_desc = {'robot@weicheche.cn': '数据监控小能手', '502202879@qq.com': '我只是个临时的数据小助手'}


def msg_add_file(msg, filepath):
    try:
        filename = filepath.strip(os.sep)[-1]  # re.split('[\\\/]', filepath)[-1]
        print(filename)
        part = email.mime.application.MIMEApplication(open(filepath, 'rb+').read())
        part.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(part)
    except Exception as err:
        print(err)


def send_email(smtpHost, senders, passwd, recipients, subject='', content='', files=None):
    msg = email.mime.multipart.MIMEMultipart()
    sender_title = senders_desc[senders] if senders in senders_desc else senders
    msg['from'] = "%s<%s>" % (sender_title, senders)
    msg['to'] = str(recipients)
    msg['subject'] = subject
    content = content
    txt = email.mime.text.MIMEText(content, 'plain', 'utf-8')
    msg.attach(txt)

    if files and type(files) is str:
        msg_add_file(msg, files)
    elif files and type(files) is list:
        for filepath in files:
            msg_add_file(msg, filepath)

    smtp = smtplib.SMTP()
    smtp.connect(smtpHost, '25')
    print(smtp)
    smtp.login(senders, passwd)
    smtp.sendmail(senders, recipients, str(msg))
    print("发送成功！")
    smtp.quit()


def send_ex163_email(recipients, subject='', content='', files=None):
    if recipients:
        return send_email('smtp.ym.163.com', 'xuechen@zhiyi.ai', 'z313249172.', recipients, subject, content, files)
    else:
        print('你需要添加收信人recipients')
        return 0


def send_qq_email(recipients, subject='', content='', files=None):
    if recipients:
        return send_email('smtp.qq.com', '502202879@qq.com', 'sajlzbwapfsjcaff', recipients, subject, content, files)
    else:
        print('你需要添加收信人recipients')
        return 0


if __name__ == '__main__':
    print('hello', sys.argv)
    if len(sys.argv) > 2 and sys.argv[1] == 'ex163':
        send_ex163_email(sys.argv[2:])
    else:
        send_qq_email('1084883095@qq.com', 'demo', '啥都可以的吧' + time_replace(ss='%Y-%m-%d %H:%M:%S'), ['Readme.md'])
