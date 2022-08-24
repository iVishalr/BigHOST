import os
import time
from dotenv import load_dotenv 
from pymongo import MongoClient
from redis import Redis, ConnectionPool
from queues.redisqueue import RedisQueue

os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()

load_dotenv(os.path.join(os.getcwd(), '.env'))

client_rr = MongoClient(os.getenv('MONGO_URI_RR'), connect=False)
db_rr = client_rr['bd']
submissions_rr = db_rr['submissions']

client_ec = MongoClient(os.getenv('MONGO_URI_EC'), connect=False)
db_ec = client_ec['bd']
submissions_ec = db_ec['submissions']

pool = ConnectionPool(host='localhost', port=6379, db=0)
broker = Redis(connection_pool=pool)
queue = RedisQueue(broker=broker, queue_name='output-queue')