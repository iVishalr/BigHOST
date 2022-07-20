import sys
import time
import pickle
import requests

from time import sleep
from redis import Redis
from datetime import datetime
from queues.redisqueue import RedisQueue


def worker_fn(rank: int, team_dict: dict, docker_ip: str, docker_port: int, docker_route: str):

    class Tee(object):
        def __init__(self, *files):
            self.files = files
        def write(self, obj):
            for f in self.files:
                f.write(obj)
        def flush(self):
            pass

    f = open(f'./worker{rank}_logs.txt', 'w+')
    backup = sys.stdout
    sys.stdout = Tee(sys.stdout, f)

    def get_datetime() -> str:
        now = datetime.now()
        timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
        return timestamp

    broker = Redis("localhost")
    queue = RedisQueue(broker, "jobqueue")

    request_url = f"http://{docker_ip}:{docker_port}/{docker_route}"

    interval = 0.05
    timeout = 0.05
    process_slept = 0

    while True:
        if len(queue) == 0:
            timeout += 0.05
            interval += timeout
            if interval > 60:
                interval = 60

            process_slept = 1
            print(f"[{get_datetime()}] [worker_{rank}]\tSleeping Worker Process for {interval:.04f} seconds.")
            sleep(interval)
            continue
        else:
            interval = 0.05
            timeout = 0.05
            if process_slept:
                print(f"[{get_datetime()}] [worker_{rank}]\tWaking up Worker Process.")
                process_slept = 0

        queue_data = queue.dequeue()

        if queue_data is None:
            process_slept = 1
            print(f"[{get_datetime()}] [worker_{rank}]\tSleeping Worker Process for {interval:.04f} seconds.")

            sleep(interval)
            continue

        queue_name, serialized_job = queue_data
        job = pickle.loads(serialized_job)
        start = time.time()
        try:
            key = job["team_id"]+"_"+job["assignment_id"]
            if key not in team_dict:
                team_dict[key] = 0
            team_dict[key] += 1
            r = requests.post(request_url, data=job)
            team_dict[key] -= 1
            print(f"[{get_datetime()}] [worker_{rank}]\t{key} Job completed Successfully | Time Taken : {time.time()-start:.04f}s Status Code : {r.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"[{get_datetime()}] [worker_{rank}]\t{e}. {key.split('_')[0]} is a potential blacklist.")
        r.close()


    print(f"[{get_datetime()}] [worker_{rank}]\tWorker Stopped.")
    sys.exit(0)