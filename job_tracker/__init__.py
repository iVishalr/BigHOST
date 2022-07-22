from .job import Job

from queues.redisqueue import RedisQueue
from redis import Redis, ConnectionPool

pool = ConnectionPool(host='localhost', port=6379, db=0)
broker = Redis(connection_pool=pool)
queue = RedisQueue(broker=broker, queue_name="jobqueue")