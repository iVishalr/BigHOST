import os
import time
import smtplib
from dotenv import load_dotenv
from redis import Redis, ConnectionPool
from common.redisqueue import RedisQueue

os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()
load_dotenv()
mail_user = os.getenv('MAIL_USER')
mail_passwd = os.getenv('MAIL_PASSWD')

mail_server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
mail_server.ehlo()
mail_server.login(mail_user, mail_passwd)

pool = ConnectionPool(host='localhost', port=6379, db=0)
mail_broker = Redis(connection_pool=pool)
mail_queue = RedisQueue(broker=mail_broker, queue_name="mail-queue")