import multiprocessing
from redis import Redis, ConnectionPool
from common.redisqueue import RedisQueue

pool = ConnectionPool(host='localhost', port=6379, db=0)
broker = Redis(connection_pool=pool)
queue = RedisQueue(broker=broker, queue_name="sanity-queue")

manager = multiprocessing.Manager()
executor_table = manager.dict()