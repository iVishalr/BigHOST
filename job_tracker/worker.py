import os
import sys
import time
import json
import signal
import pickle
import socket
import requests
import threading
import subprocess

from time import sleep
from typing import Dict, List, Union

from common import mail_queue
from common.db import DataBase
from common.utils import Tee, get_datetime, Logger

from contextlib import closing
from queue import PriorityQueue
from datetime import datetime, timedelta
from job_tracker import output_queue, docker_client
from job_tracker.job import MRJob, SparkJob, KafkaJob

def worker_fn(
        worker_rank: int,
        worker_log_path: str,
        team_dict: dict,
        running_dict: dict,
        worker_lock,
        worker_timeout: int,
        docker_ip: str, 
        docker_port: int, 
        docker_route: str,
        docker_image: str, 
        num_threads: int, 
        cpu_limit: int, 
        mem_limit: str,
        taskset: bool, 
        mem_swappiness: int,
        host_output_dir: str, 
        container_output_dir: str,
        container_spawn_wait: int,
        blacklist_threshold: int,
        blacklist_duration: int
    ):

    # f = open(os.path.join(worker_log_path, f'worker{worker_rank}_logs.txt'), 'a+')
    # backup = sys.stdout
    # sys.stdout = Tee(sys.stdout, f)
    sys.stdout = Logger(os.path.join(worker_log_path, f'worker{worker_rank}_logs.txt'), 'a+')
    db = DataBase()

    if not os.path.exists(host_output_dir):
        os.makedirs(host_output_dir)

    def updateSubmission(marks, message, data, send_mail=False):
        timestamp = int(str(time.time_ns())[:10])
        team_id = data['teamId']
        assignment_id = data["assignmentId"]
        submission_id = str(data["submissionId"])
        doc = db.update("submissions", team_id, None, assignment_id, submission_id, marks, message, timestamp)
        
        if send_mail:
            mail_data = {
                'teamId': data['teamId'],
                'submissionId': submission_id,
                'submissionStatus': message,
                'attachment': ""
            }
            mail_data = pickle.dumps(mail_data)
            mail_queue.enqueue(mail_data)

    from job_tracker import queue

    def find_free_port():
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    def start_container(rank: int, image: str, cpu_limit: int, memory_limit: str, taskset: bool, mem_swappiness: int, host_output_dir: str, docker_output_dir: str):
        worker_lock.acquire()
        rm_port = find_free_port()
        dn_port = find_free_port()
        hadoop_port = find_free_port()
        jobhis_port = find_free_port()
        worker_lock.release()

        if taskset:
            number_of_cores = num_threads * cpu_limit
            cpu_set = f"{0 + ((worker_rank-1) * number_of_cores) + (rank-1)*cpu_limit}-{0 + ((worker_rank-1) * number_of_cores) + (rank*cpu_limit) - 1}"
            print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\tSetting processor affinity = {cpu_set} for container hadoop-c{worker_rank}{rank}.")
            docker_container = docker_client.containers.run(
                image=image, 
                auto_remove=True,
                detach=True,
                name=f"hadoop-c{worker_rank}{rank}",
                cpuset_cpus=cpu_set,
                mem_limit=str(memory_limit),
                mem_swappiness=mem_swappiness,
                volumes={
                    f'{host_output_dir}': {'bind': docker_output_dir, 'mode': 'rw'},
                },
                ports={
                    '8088/tcp': rm_port,
                    '9870/tcp': dn_port,
                    f'{docker_port}/tcp': hadoop_port,
                    '19888/tcp': jobhis_port,
                },
            )
        else:
            docker_container = docker_client.containers.run(
                image=image, 
                auto_remove=True,
                detach=True,
                name=f"hadoop-c{worker_rank}{rank}",
                cpu_period=int(cpu_limit)*100000,
                mem_limit=str(memory_limit),
                mem_swappiness=mem_swappiness,
                volumes={
                    f'{host_output_dir}': {'bind': docker_output_dir, 'mode': 'rw'},
                },
                ports={
                    '8088/tcp': rm_port,
                    '9870/tcp': dn_port,
                    f'{docker_port}/tcp': hadoop_port,
                    '19888/tcp': jobhis_port,
                },
            )
        
        print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\tStarting docker container.")
        time.sleep(container_spawn_wait)

        request_url = f"http://{docker_ip}:{hadoop_port}/{docker_route}"

        return request_url, docker_container, [rm_port, dn_port, hadoop_port, jobhis_port]

    def blacklist_thread_fn(blacklist_queue: PriorityQueue[Dict], event: threading.Event):
        interval = 0.05
        sleep(container_spawn_wait)
        print(f"[{get_datetime()}] [worker_{worker_rank}] [blacklist_thread]\tStarting.")
        while not event.is_set():
            try:
                data = blacklist_queue.get_nowait()
            except:
                print(f"[{get_datetime()}] [worker_{worker_rank}] [blacklist_thread]\tSleeping for 120s.")
                sleep(120)
                continue

            if datetime.now() < data[0]:
                blacklist_queue.put(data)
                interval = (data[0] - datetime.now()).seconds
                print(f"[{get_datetime()}] [worker_{worker_rank}] [blacklist_thread]\tSleeping for {interval+5}s.")
                sleep(interval+5)
                continue
            
            _, data = data
            timestamp = int(str(time.time_ns())[:10])
            doc = db.unblacklist("submissions", data['team_id'], "", timestamp)
            
            prefix = data['team_id']+"_"+data['assignment_id'][:-2]
            
            if prefix+"T1" in team_dict:
                team_dict[prefix+"T1"] = 0
            
            if prefix+"T2" in team_dict:
                team_dict[prefix+"T2"] = 0
            
            if prefix+"T1" in running_dict:
                running_dict[prefix+"T1"] = 0
            
            if prefix+"T2" in running_dict:
                running_dict[prefix+"T2"] = 0
            
            print(f"[{get_datetime()}] [worker_{worker_rank}] [blacklist_thread]\tUnblacklisted Team : {data['team_id']}.")

    def thread_fn(rank, event: threading.Event):
        request_url, docker_container, port_list = start_container(
            rank, 
            "hadoop-3.2.2:0.1", 
            cpu_limit, 
            mem_limit, 
            taskset,
            mem_swappiness, 
            host_output_dir, 
            container_output_dir
        )
        
        interval = 0.05
        timeout = 0.05
        prev_interval = 0.05
        process_slept = 0

        while not event.is_set():

            queue_length = len(queue)

            if queue_length == 0:
                timeout += 0.05
                prev_interval = interval
                interval += timeout
                if interval > worker_timeout:
                    interval = worker_timeout

                process_slept = 1
                print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\tSleeping Worker Process for {interval:.04f} seconds.")
                sleep(interval)
                continue
            else:
                prev_interval = interval
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
            job: Union[MRJob, SparkJob, KafkaJob] = pickle.loads(serialized_job)
            job.record("processing_queue_exit")
            start = time.time()

            key = job.team_id + "_" + job.assignment_id
            if key not in team_dict:
                team_dict[key] = 0
                running_dict[key] = 0
            else:
                # TODO : make it work for Job Class
                # if team already in team_dict, check whether they have been blacklisted currently, but still managed to submit a submission.
                if blacklist_threshold - team_dict[key] < 0:
                    job.blacklisted = True
                    job.end_time = None
                    job.status = 'BLACKLISTED_BEFORE'
                    job.message = f"Job not processed as you were blacklisted!"
                    
                    job.record("output_queue_entry")
                    serialized_job = pickle.dumps(job)
                    output_queue.enqueue(serialized_job)
                    continue

            team_dict[key] += 1
            running_dict[key] = 1
            status_code = 500

            updateSubmission(marks=-1, message='Executing', data=job.get_db_fields())
            job.record("processing_start")

            try:
                r = requests.post(request_url, data=job.__dict__, timeout=float(job.timeout))
                res = json.loads(r.text)
                status_code = r.status_code
                job.status = res["status"]
                r.close()

            except requests.exceptions.Timeout:
                if job.assignment_id != "A3T2":
                    # A3T2 does not use YARN to run jobs. Hence we cannot kill using YARN. 
                    kill_url = f"http://{docker_ip}:{port_list[2]}/kill_job"
                    kill_request = requests.get(kill_url)
                    kill_status = kill_request.status_code
                job.status = 'FAILED'
                job.message = f"Job Killed. Time Limit Exceeded!"
            
            except:
                job.status = 'FAILED'

                if blacklist_threshold - team_dict[key] >= 0:
                    job.message = f'Container Crashed. Memory Limit Exceeded. Incident logged and tracked. {blacklist_threshold - team_dict[key]} Submissions away from being blacklisted.'
                else:
                    job.message = f'Container Crashed. Memory Limit Exceeded. Incident logged and tracked. Team Blacklisted!'

                docker_kill_process = subprocess.Popen([f"docker stop hadoop-c{worker_rank}{rank} && docker rm hadoop-c{worker_rank}{rank}"], shell=True, text=True)
                _ = docker_kill_process.wait()

                docker_container = None

                request_url, docker_container, port_list = start_container(
                    rank, 
                    docker_image, 
                    cpu_limit, 
                    mem_limit, 
                    taskset,
                    mem_swappiness, 
                    host_output_dir, 
                    container_output_dir
                )

            job.record('processing_end')
            
            if job.status != "FAILED":
                team_dict[key] -= 1
                running_dict[key] = 0
                print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\t{key}_{job.submission_id} Job Executed Successfully | Job : {job.status} Message : {job.message} Time Taken : {time.time()-start:.04f}s Status Code : {status_code}")
            else:
                running_dict[key] = 0
                if "Container Crashed" in job.message:
                    if team_dict[key] <= blacklist_threshold:
                        print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\t{key}_{job.submission_id} Job Executed Successfully | Job : {job.status} Message : {job.message} Time Taken : {time.time()-start:.04f}s Status Code : {status_code}. Team : {job.team_id} is {blacklist_threshold - team_dict[key]} submissions away from being blacklisted.")
                    else:
                        end_time =  datetime.now() + timedelta(minutes=blacklist_duration)
                        blacklist_queue.put((end_time, {'team_id': job.team_id, 'assignment_id': job.assignment_id}))
                        job.blacklisted = True
                        job.end_time = end_time
                        print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\t{key}_{job.submission_id} Job Executed Successfully | Job : {job.status} Message : {job.message} Time Taken : {time.time()-start:.04f}s Status Code : {status_code}. Team : {job.team_id} is blacklisted.")
                else:
                    team_dict[key] -= 1
                    print(f"[{get_datetime()}] [worker_{worker_rank}] [thread {rank}]\t{key}_{job.submission_id} Job Executed Successfully | Job : {job.status} Message : {job.message} Time Taken : {time.time()-start:.04f}s Status Code : {status_code}.")
            
            
            job.record("output_queue_entry")
            serialized_job = pickle.dumps(job)
            output_queue.enqueue(serialized_job)

        if event.is_set():
            docker_kill_process = subprocess.Popen([f"docker stop hadoop-c{worker_rank}{rank} && docker rm hadoop-c{worker_rank}{rank}"], shell=True, text=True)
            _ = docker_kill_process.wait()

    threads : List[threading.Thread] = []
    thread_events : List[threading.Event] = []
    
    for i in range(num_threads):
        e = threading.Event()
        t = threading.Thread(target=thread_fn, args=(i+1,e,))
        threads.append(t)
        thread_events.append(e)

    blacklist_queue = PriorityQueue()
    e = threading.Event()
    t = threading.Thread(target=blacklist_thread_fn, args=(blacklist_queue,e,))
    threads.append(t)
    thread_events.append(e)

    print(f"[{get_datetime()}] [worker_{worker_rank}]\tStarting {num_threads} threads.")
    for t in threads:
        t.start()

    def signal_handler(sig, frame):
        print(f'[{get_datetime()}] [worker_{worker_rank}]\tStopping.')
        for i in thread_events:
            i.set()
        for i in threads[:-1]:
            i.join()
        for i in threads[:-1]:
            if i.is_alive():
                i.join()
        threads[-1].join(30)
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)