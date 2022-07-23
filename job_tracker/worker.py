import sys
import time
import json
import pickle
import requests
import threading
import signal

from time import sleep
from typing import List
from datetime import datetime

def worker_fn(worker_rank: int, team_dict: dict, docker_ip: str, docker_port: int, docker_route: str, num_threads: int):

    class Tee(object):
        def __init__(self, *files):
            self.files = files
        def write(self, obj):
            for f in self.files:
                f.write(obj)
        def flush(self):
            pass

    f = open(f'./worker{worker_rank}_logs.txt', 'w+')
    backup = sys.stdout
    sys.stdout = Tee(sys.stdout, f)

    def get_datetime() -> str:
        now = datetime.now()
        timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
        return timestamp

    from job_tracker import queue

    request_url = f"http://{docker_ip}:{docker_port}/{docker_route}"

    def thread_fn(rank, event: threading.Event):
        interval = 0.05
        timeout = 0.05
        process_slept = 0
        blacklist_threshold = 3

        while not event.is_set():

            queue_length = len(queue)

            if queue_length == 0:
                timeout += 0.05
                interval += timeout
                if interval > 60:
                    interval = 60

                process_slept = 1
                print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\tSleeping Worker Process for {interval:.04f} seconds.")
                sleep(interval)
                continue
            else:
                interval = 0.05
                timeout = 0.05
                if process_slept:
                    print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\tWaking up Worker Process.")
                    process_slept = 0

            queue_data = queue.dequeue()

            if queue_data is None:
                process_slept = 1
                print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\tSleeping Worker Process for {interval:.04f} seconds.")
                sleep(interval)
                continue

            queue_name, serialized_job = queue_data
            job = pickle.loads(serialized_job)
            start = time.time()

            key = job["team_id"]+"_"+job["assignment_id"]
            if key not in team_dict:
                team_dict[key] = 0
            
            team_dict[key] += 1
            r = requests.post(request_url, data=job)
            res = json.loads(r.text)
            
            if res['status'] != "KILLED":
                team_dict[key] -= 1
                print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\t{key} Job Executed Successfully | Job : {res['status']} Message : {res['job_output']} Time Taken : {time.time()-start:.04f}s Status Code : {r.status_code}")
            else:
                if team_dict[key] <= blacklist_threshold:
                    print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\t{key} Job Executed Successfully | Job : {res['status']} Message : {res['job_output']} Time Taken : {time.time()-start:.04f}s Status Code : {r.status_code}. Team : {job['team_id']} is {blacklist_threshold - team_dict[key]} submissions away from being blacklisted.")
                else:
                    print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\t{key} Job Executed Successfully | Job : {res['status']} Message : {res['job_output']} Time Taken : {time.time()-start:.04f}s Status Code : {r.status_code}. Team : {job['team_id']} is blacklisted.")

            r.close()
    
    threads : List[threading.Thread] = []
    thread_events : List[threading.Event] = []
    
    for i in range(num_threads):
        e = threading.Event()
        t = threading.Thread(target=thread_fn, args=(i+1,e,))
        threads.append(t)
        thread_events.append(e)

    print(f"[{get_datetime()}] [worker_{worker_rank}]\tStarting {num_threads} threads.")
    for t in threads:
        t.start()

    def signal_handler(sig, frame):
        print(f'[{get_datetime()}] [worker_{worker_rank}]\tStopping.')
        for i in thread_events:
            i.set()
        for i in threads:
            i.join()
        for i in threads:
            if i.is_alive():
                i.join()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)