#!/usr/bin/sudo /usr/bin/python3
import os
import sys
import json
import time
import pickle
import signal
import filecmp
import threading

from time import sleep
from typing import List
from .preprocess import *
from common import mail_queue
from common.utils import Tee, get_datetime

def output_processor_fn(rank: int, event: threading.Event, num_threads: int, op_timeout: int, submission_output_dir: str, answer_key_path: str):
    '''
    Takes a submissions output and compares to the expected output
    '''

    f = open(f'./output_processor_logs.txt', 'a+')
    backup = sys.stdout
    sys.stdout = Tee(sys.stdout, f)

    from common.db import DataBase
    from output_processor import queue, broker

    db = DataBase()
    FILEPATH = submission_output_dir
    CORRECT_OUTPUT = answer_key_path

    def thread_fn(rank, event: threading.Event):
        interval = 0.05
        timeout = 0.05
        process_slept = 0
        while not event.is_set():

            if len(queue)==0:
                timeout += 0.05
                interval += timeout
                
                if interval > op_timeout:
                    interval = op_timeout

                process_slept = 1
                print(f"[{get_datetime()}] [output_processor] [thread {rank}]\tSleeping Output Processor for {interval:.04f} seconds.")
                sleep(interval)
                continue

            else:
                interval = 0.05
                timeout = 0.05
                if process_slept:
                    print(f"[{get_datetime()}] [output_processor] [thread {rank}]\tWaking up Output Processor.")
                    process_slept = 0

            output_data = queue.dequeue()

            if output_data is None:
                process_slept = 1
                print(f"[{get_datetime()}] [output_processor] [thread {rank}]\tSleeping Output Processor for {interval:.04f} seconds.")
                sleep(interval)
                continue

            queue_name, serialized_data = output_data
            data = pickle.loads(serialized_data)

            teamId = data['team_id']
            assignmentId = data['assignment_id']
            status = data['status']
            submissionId = str(data['submission_id'])
            message = data['job_output']
            teamBlacklisted = data['blacklisted']
            end_time = data['end_time']

            FILEPATH_TEAM = os.path.join(FILEPATH, teamId, assignmentId, submissionId)
            print(FILEPATH_TEAM)
            os.system(f"sudo chown -R $USER:$USER {FILEPATH}/{teamId}")

            if not os.path.exists(FILEPATH_TEAM):
                status = "FAILED"

            timestamp = int(str(time.time_ns())[:10])
            
            # If status is false, directly put 0
            if status == "FAILED":
                blacklisted = None
                if teamBlacklisted:
                    blacklisted = db.gen_blacklisted_record(True, f"You are blocked from submitting. Come back at {end_time.strftime('%d/%m/%Y %H:%M:%S')} to submit again.", timestamp)
                doc = db.update("submissions", teamId, blacklisted, assignmentId, submissionId, 0, message, timestamp)
                print(f"[{get_datetime()}] [output_processor]\tTeam : {teamId} Assignment ID : {assignmentId} Result : Failed Message : {message}")

                error_logs = ""

                if "Time Limit" in message: 
                    error_logs = f"Team ID : {teamId}\nAssignment ID : {assignmentId}\nSubmission ID : {submissionId}\n\nTime Limit Exceeded! Make your code run faster and more memory efficient.\n"
                elif "Memory Limit" in message:
                    error_logs = f"Team ID : {teamId}\nAssignment ID : {assignmentId}\nSubmission ID : {submissionId}\n\nMemory Limit Exceeded! Make sure you are not storing anything in memory. Storing the entire dataset will exhaust memory resources. You will be flagged if you repeat this mistake.\n"
                else:
                    if os.path.exists(os.path.join(FILEPATH_TEAM, "error.txt")):
                        with open(os.path.join(FILEPATH_TEAM, "error.txt"), "r") as f:
                            error_logs = f.read()

                mail_data = {
                    'teamId': teamId,
                    'submissionId': str(submissionId),
                    'submissionStatus': message,
                    'attachment': error_logs
                }
                mail_data = pickle.dumps(mail_data)
                mail_queue.enqueue(mail_data)
            
            elif status == 'BLACKLISTED_BEFORE': # needed if a team gets blacklisted but team's submissions still exists in queue
                print(f"[{get_datetime()}] [output_processor]\tTeam : {teamId} Assignment ID : {assignmentId} Result : BLACKLISTED_BEFORE Message : {message}")
                doc = db.update("submissions", teamId, None, assignmentId, submissionId, 0, message, timestamp)
            else:
                # Has given outuput, need to check if it is corect
                # preprocess the output files to match answer key if output follows certain conditions
                output = None

                if "A1" in assignmentId:
                    preprocess_A1_output(teamId=teamId, assignmentId=assignmentId, output_path=os.path.join(FILEPATH_TEAM, "part-00000"), key_path=os.path.join(CORRECT_OUTPUT, assignmentId, "part-00000"))

                    if os.path.exists(os.path.join(FILEPATH_TEAM, "part-00000")):
                        output = filecmp.cmp(os.path.join(FILEPATH_TEAM, "part-00000"), os.path.join(CORRECT_OUTPUT, assignmentId, "part-00000"), shallow=False)

                elif "A2" in assignmentId:
                    convergence_limit = 0.05
                    preprocess_A2_output(teamId=teamId, assignmentId=assignmentId, output_path=os.path.join(FILEPATH_TEAM, "part-00000"), key_path=os.path.join(CORRECT_OUTPUT, assignmentId, "part-00000"), convergence_limit=convergence_limit)

                    if os.path.exists(os.path.join(FILEPATH_TEAM, "part-00000")):
                        output = filecmp.cmp(os.path.join(FILEPATH_TEAM, "part-00000"), os.path.join(CORRECT_OUTPUT, assignmentId, "part-00000"), shallow=False)

                elif "A3" in assignmentId:
                    if "T2" in assignmentId:
                        output = preprocess_A3_output(teamId=teamId, assignmentId=assignmentId, output_path=os.path.join(FILEPATH_TEAM, "output.json"), key_path=os.path.join(CORRECT_OUTPUT, assignmentId, "output.json"))
                    else:
                        if os.path.exists(os.path.join(FILEPATH_TEAM, "part-00000")):
                            output = filecmp.cmp(os.path.join(FILEPATH_TEAM, "part-00000"), os.path.join(CORRECT_OUTPUT, assignmentId, "part-00000"), shallow=False)
                
                if output:
                    print(f"[{get_datetime()}] [output_processor]\tTeam : {teamId} Assignment ID : {assignmentId}_{submissionId} Result : Passed")
                    doc = db.update("submissions", teamId, None, assignmentId, submissionId, 1, 'Passed', timestamp)
                    message = 'PASSED. Submission has passed our test cases. Good Job!'
                else:
                    print(f"[{get_datetime()}] [output_processor]\tTeam : {teamId} Assignment ID : {assignmentId}_{submissionId} Result : Failed")
                    doc = db.update("submissions", teamId, None, assignmentId, submissionId, 0, 'Wrong Answer', timestamp)
                    message = 'FAILED. Submission did not passed our test cases. Try Again!'
                
                mail_data = {
                    'teamId': teamId,
                    'submissionId': str(submissionId),
                    'submissionStatus': message,
                    'attachment': ""
                }
                mail_data = pickle.dumps(mail_data)
                mail_queue.enqueue(mail_data)

    #end of thread_fn
    threads : List[threading.Thread] = []
    thread_events : List[threading.Event] = []
    
    for i in range(num_threads):
        e = threading.Event()
        t = threading.Thread(target=thread_fn, args=(i+1,e,))
        threads.append(t)
        thread_events.append(e)

    print(f"[{get_datetime()}] [output_processor]\tStarting {num_threads} threads.")
    for t in threads:
        t.start()

    def signal_handler(sig, frame):
        print(f'[{get_datetime()}] [output_processor]\tStopping.')
        for i in thread_events:
            i.set()
        for i in threads:
            i.join()
        for i in threads:
            if i.is_alive():
                i.join(30)
        broker.close()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)    

if __name__ == "__main__":
    config_path = os.path.join(os.getcwd(), "config", "evaluator.json")

    configs = None
    with open(config_path, "r") as f:
        configs = json.loads(f.read())

    executor_config = configs["executor"]
    docker_config = configs["docker"]

    output_processor_fn(
        rank=1, 
        event=threading.Event(), 
        num_threads=1,
        op_timeout=executor_config["timeout"],
        submission_output_dir=docker_config["shared_output_dir"], 
        answer_key_path=os.path.join(os.getcwd(), "answer")
    )
    signal.pause()