import os
import sys
import time
import json
import signal
import pickle
import requests
import threading
import socket
import socket
from contextlib import closing
from time import sleep
from typing import List
from smtp import mail_queue
from datetime import datetime
from job_tracker import output_queue, submissions


def updateSubmission(marks, message, data):
    doc = submissions.find_one({'teamId': data['team_id']})
    doc['assignments'][data['assignment_id']]['submissions'][str(data['submission_id'])]['marks'] = marks
    doc['assignments'][data['assignment_id']]['submissions'][str(data['submission_id'])]['message'] = message
    doc = submissions.find_one_and_update({'teamId': data['team_id']}, {'$set': {'assignments': doc['assignments']}})
    
    mail_data = {}
    mail_data['teamId'] = data['team_id']
    mail_data['submissionId'] = str(data['submission_id'])
    mail_data['submissionStatus'] = message
    mail_data = pickle.dumps(mail_data)
    mail_queue.enqueue(mail_data)


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

    def find_free_port():
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    def start_container(cpu_limit, memory_limit, rank):
        rm_port = find_free_port()
        dn_port = find_free_port()
        hadoop_port = find_free_port()
        jobhis_port = find_free_port()

        docker_command = f"docker run -p {rm_port}:8088 -p {dn_port}:9870 -p {hadoop_port}:10000 -p {jobhis_port}:19888 -v $PWD/output:/output -m {memory_limit} --cpus={cpu_limit} --name hadoop-c{str(worker_rank)+str(rank)} -d hadoop-3.2.2:0.1"
        print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\tStarting docker container with command: {docker_command}")

        os.system(docker_command)
        time.sleep(30)

        request_url = f"http://{docker_ip}:{hadoop_port}/{docker_route}"

        return request_url

    def thread_fn(rank, event: threading.Event):
        
        request_url = start_container("2", "3000m", rank)
        
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

            updateSubmission(marks=-1, message='Executing', data=job)

            key = job["team_id"]+"_"+job["assignment_id"]
            if key not in team_dict:
                team_dict[key] = 0
            
            team_dict[key] += 1
            status_code = 500
            
            try:
                r = requests.post(request_url, data=job, timeout=150)
                status_code = r.status_code
                res = json.loads(r.text)
                res['team_id'] = job['team_id']
                res['assignment_id'] = job['assignment_id']
                res['submission_id'] = job['submission_id']
                res['blacklisted'] = False
                r.close()
            except:
                print("in execept")
                res = {}
                res['team_id'] = job['team_id']
                res['assignment_id'] = job['assignment_id']
                res['submission_id'] = job['submission_id']
                res['blacklisted'] = False
                res['status'] = 'FAILED'
                res['job_output'] = 'You destroyed our container :('
                os.system(f"docker stop hadoop-c{str(worker_rank)+str(rank)} && docker rm hadoop-c{str(worker_rank)+str(rank)}")
                time.sleep(5)
                request_url = start_container("2", "3000m", rank)

            
            if res['status'] != "FAILED":
                team_dict[key] -= 1
                print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\t{key} Job Executed Successfully | Job : {res['status']} Message : {res['job_output']} Time Taken : {time.time()-start:.04f}s Status Code : {status_code}")
            else:
                if team_dict[key] <= blacklist_threshold:
                    print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\t{key} Job Executed Successfully | Job : {res['status']} Message : {res['job_output']} Time Taken : {time.time()-start:.04f}s Status Code : {status_code}. Team : {job['team_id']} is {blacklist_threshold - team_dict[key]} submissions away from being blacklisted.")
                else:
                    res['blacklisted'] = True
                    print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\t{key} Job Executed Successfully | Job : {res['status']} Message : {res['job_output']} Time Taken : {time.time()-start:.04f}s Status Code : {status_code}. Team : {job['team_id']} is blacklisted.")
            
            serialized_job_message = pickle.dumps(res)
            output_queue.enqueue(serialized_job_message)
    
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