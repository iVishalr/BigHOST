import os
import time
import smtplib
from dotenv import load_dotenv
from pymongo import MongoClient
from redis import Redis, ConnectionPool
from queues.redisqueue import RedisQueue

os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()

load_dotenv()

client_rr = MongoClient(os.getenv('MONGO_URI_RR'), connect=False)
db_rr = client_rr['bd']
dbuser_rr = db_rr['user']

client_ec = MongoClient(os.getenv('MONGO_URI_EC'), connect=False)
db_ec = client_ec['bd']
dbuser_ec = db_ec['user']

mail_user_rr = os.getenv('MAIL_USER_RR')
mail_passwd_rr = os.getenv('MAIL_PASSWD_RR')

mail_user_ec = os.getenv('MAIL_USER_EC')
mail_passwd_ec = os.getenv('MAIL_PASSWD_EC')

mail_server_rr = smtplib.SMTP_SSL('smtp.gmail.com', 465)
mail_server_rr.ehlo()
mail_server_rr.login(mail_user_rr, mail_passwd_rr)

mail_server_ec = smtplib.SMTP_SSL('smtp.gmail.com', 465)
mail_server_ec.ehlo()
mail_server_ec.login(mail_user_ec, mail_passwd_ec)

pool = ConnectionPool(host='localhost', port=6379, db=0)
mail_broker = Redis(connection_pool=pool)
mail_queue = RedisQueue(broker=mail_broker, queue_name="mail-queue")