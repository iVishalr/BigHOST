from time import sleep
import requests
import json
import pickle

from redis import Redis
from queues.redisqueue import RedisQueue

def worker_fn(docker_ip: str, docker_port: int, docker_route: str):
    broker = Redis("localhost")
    queue = RedisQueue(broker, "jobqueue")

    request_url = f"http://{docker_ip}:{docker_port}/{docker_route}"

    interval = 0.25
    timeout = 0.25

    while True:
        
        if len(queue) == 0:
            sleep(interval)
            timeout += 0.25
            interval += timeout

        else:
            interval = 0.25
            timeout = 0.25

        if len(queue) > 16:

            queue_name, serialized_job = queue.dequeue()
            job = pickle.loads(serialized_job)

            r = requests.post(request_url, data=job)
            print(r.status_code)
            # res = json.loads(r.text)
            # print(res)
            r.close()
