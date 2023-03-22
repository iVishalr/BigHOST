import os
import docker
import time

from dotenv import load_dotenv 
from redis import Redis, ConnectionPool
from common.redisqueue import RedisQueue
from output_processor import queue as output_queue

pool = ConnectionPool(host='localhost', port=6379, db=0)
broker = Redis(connection_pool=pool)
queue = RedisQueue(broker=broker, queue_name="jobqueue")

load_dotenv(os.path.join(os.getcwd(), '.env'))

docker_client = docker.from_env()

BACKEND_INTERNAL_IP = os.getenv('BACKEND_INTERNAL_IP')

os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()