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

mail_user = os.getenv('MAIL_USER')
mail_passwd = os.getenv('MAIL_PASSWD')

mail_server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
mail_server.ehlo()
mail_server.login(mail_user, mail_passwd)

pool = ConnectionPool(host='localhost', port=6379, db=0)
mail_broker = Redis(connection_pool=pool)
mail_queue = RedisQueue(broker=mail_broker, queue_name="mail-queue")