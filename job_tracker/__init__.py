import os
from .job import Job
from output_processor import queue as output_queue
from queues.redisqueue import RedisQueue
from redis import Redis, ConnectionPool
from pymongo import MongoClient
from dotenv import load_dotenv 

pool = ConnectionPool(host='localhost', port=6379, db=0)
broker = Redis(connection_pool=pool)
queue = RedisQueue(broker=broker, queue_name="jobqueue")

load_dotenv(os.path.join(os.getcwd(), '.env'))

client = MongoClient(os.getenv('MONGO_URI'), connect=False)
db = client['bd']
submissions = db['submissions']