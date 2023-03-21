import os
import time
from redis import Redis, ConnectionPool
from queues.redisqueue import RedisQueue

os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()

pool = ConnectionPool(host='localhost', port=6379, db=0)
broker = Redis(connection_pool=pool)
queue = RedisQueue(broker=broker, queue_name='output-queue')