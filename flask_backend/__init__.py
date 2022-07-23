from redis import Redis, ConnectionPool
from queues.redisqueue import RedisQueue
import os
from dotenv import load_dotenv
from pymongo import MongoClient
pool = ConnectionPool(host='localhost', port=6379, db=0)
broker = Redis(connection_pool=pool)
queue = RedisQueue(broker=broker, queue_name="sanity-queue")

load_dotenv(os.path.join(os.getcwd(), '.env'))

client = MongoClient(os.getenv('MONGO_URI'))
db = client['bd']
submissions = db['submissions']