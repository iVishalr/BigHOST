import os
import docker
import time
from job_tracker.job import MRJob, SparkJob, KafkaJob
from dotenv import load_dotenv 
from pymongo import MongoClient
from redis import Redis, ConnectionPool
from queues.redisqueue import RedisQueue
from output_processor import queue as output_queue

pool = ConnectionPool(host='localhost', port=6379, db=0)
broker = Redis(connection_pool=pool)
queue = RedisQueue(broker=broker, queue_name="jobqueue")

load_dotenv(os.path.join(os.getcwd(), '.env'))

client_rr = MongoClient(os.getenv('MONGO_URI_RR'), connect=False)
db_rr = client_rr['bd']
submissions_rr = db_rr['submissions']

client_ec = MongoClient(os.getenv('MONGO_URI_EC'), connect=False)
db_ec = client_ec['bd']
submissions_ec = db_ec['submissions']

docker_client = docker.from_env()

BACKEND_INTERNAL_IP = os.getenv('BACKEND_INTERNAL_IP')
BACKEND_EXTERNAL_IP = os.getenv('BACKEND_EXTERNAL_IP')

os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()