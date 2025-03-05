import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# 163邮箱的SMTP服务器地址和端口
smtp_server = 'smtp.163.com'
smtp_port = 465

# 你的163邮箱账号和密码
email_account = '15779474931@163.com'
email_password = 'CNqYxe34auDakktg'

# 邮件内容
subject = '财富密码来了'
arr = [1, 23, 444]
body = f'aaa{str(arr)}'

# 创建 MIMEMultipart 对象
msg = MIMEMultipart()
msg['From'] = email_account
msg['Subject'] = subject

# 将邮件内容添加到 MIMEMultipart 对象中
msg.attach(MIMEText(body, 'html'))

# 连接到163邮箱的SMTP服务器
server = smtplib.SMTP_SSL(smtp_server, smtp_port)
server.login(email_account, email_password)  # 登录邮箱

# 定义发送邮件的函数
def send_message(emails):
    """
    发送邮件到多个邮箱
    :param emails: 收件人邮箱列表，例如 ['970488001@qq.com', 'another_email@example.com']
    """
    try:
        # 发送邮件
        server.sendmail(email_account, emails, msg.as_string())
        print(f'邮件发送成功！收件人: {", ".join(emails)}')
    except Exception as e:
        print(f'邮件发送失败: {e}')
    finally:
        server.quit()  # 关闭连接

# 调用函数发送邮件
recipients = ['970488001@qq.com']  # 收件人列表
send_message(recipients)