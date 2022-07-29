import os
from dotenv import load_dotenv 
from pymongo import MongoClient
from redis import Redis, ConnectionPool
from queues.redisqueue import RedisQueue
from .email import EmailingService

load_dotenv(os.path.join(os.getcwd(), '.env'))
client = MongoClient(os.getenv('MONGO_URI'), connect=False)
db = client['bd']
submissions = db['submissions']
es = EmailingService()

pool = ConnectionPool(host='localhost', port=6379, db=0)
broker = Redis(connection_pool=pool)
queue = RedisQueue(broker=broker, queue_name='output-queue')