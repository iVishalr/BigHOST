import os
import docker
import time
from .job import Job
from dotenv import load_dotenv 
from pymongo import MongoClient
from redis import Redis, ConnectionPool
from queues.redisqueue import RedisQueue
from output_processor import queue as output_queue

pool = ConnectionPool(host='localhost', port=6379, db=0)
broker = Redis(connection_pool=pool)
queue = RedisQueue(broker=broker, queue_name="jobqueue")

load_dotenv(os.path.join(os.getcwd(), '.env'))

client = MongoClient(os.getenv('MONGO_URI'), connect=False)
db = client['bd']
submissions = db['submissions']

docker_client = docker.from_env()

BACKEND_INTERNAL_IP = os.getenv('BACKEND_INTERNAL_IP')
BACKEND_EXTERNAL_IP = os.getenv('BACKEND_EXTERNAL_IP')

os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()