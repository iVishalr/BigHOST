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
from typing import Dict, List
from common import mail_queue
from datetime import datetime

def output_processor_fn(rank: int, event: threading.Event, num_threads: int, submission_output_dir: str, answer_key_path: str):
    '''
    Takes a submissions output and compares to the expected output
    '''

    class Tee(object):
        def __init__(self, *files):
            self.files = files
        def write(self, obj):
            for f in self.files:
                f.write(obj)
        def flush(self):
            pass

    def get_datetime() -> str:  
        now = datetime.now()
        timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
        return timestamp

    f = open(f'./output_processor_logs.txt', 'a+')
    backup = sys.stdout
    sys.stdout = Tee(sys.stdout, f)

    from output_processor import queue, broker
    from common.db import DataBase

    db = DataBase()
    FILEPATH = submission_output_dir
    CORRECT_OUTPUT = answer_key_path

    def preprocess_A1_output(teamId, assignmentId, output_path, key_path):
        pass

    def preprocess_A2_output(teamId, assignmentId, output_path, key_path, convergence_limit):
        if assignmentId == "A2T1":
            pass

        elif assignmentId == "A2T2":
            preprocessed_output = []
            output_file = open(output_path,"r")
            answer_file = open(key_path, "r")
            flag = True

            for output_line, answer_line in zip(output_file, answer_file):
                op_page, op_rank = output_line.strip().split(",")
                answer_page, answer_rank = answer_line.strip().split(",")

                if op_page != answer_page:
                    flag = False
                    break

                op_rank = float(op_rank)
                answer_rank = float(answer_rank)

                if abs(op_rank - answer_rank) <= convergence_limit:
                    preprocessed_output.append((op_page, answer_rank))

            output_file.close()
            answer_file.close()

            if not flag:
                preprocessed_output = []
            else:
                for i in range(len(preprocessed_output)):
                    page, rank = preprocessed_output[i]
                    preprocessed_output[i] = f"{page},{rank:.2f}\n"

                output_file = open(output_path, "w")
                preprocessed_output = "".join(preprocessed_output)
                output_file.write(preprocessed_output)
                output_file.close()

    def preprocess_A3_output(teamId, assignmentId, output_path, key_path):
        if not os.path.exists(output_path):
            return False

        with open(key_path) as f:
            answer_key = f.read()

        with open(output_path) as f:
            output = f.read()

        answer_key: Dict = json.loads(answer_key)

        try:
            output: Dict = json.loads(output)
        except:
            return False

        return answer_key == output

    def thread_fn(rank, event: threading.Event):
        interval = 0.05
        timeout = 0.05
        process_slept = 0
        while not event.is_set():

            if len(queue)==0:
                timeout += 0.05
                interval += timeout
                
                if interval > 60:
                    interval = 60

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
            message = data["job_output"]
            teamBlacklisted = data["blacklisted"]
            end_time = data["end_time"]

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

                mail_data = {}
                mail_data['teamId'] = teamId
                mail_data['submissionId'] = str(submissionId)
                mail_data['submissionStatus'] = message
                mail_data['attachment'] = error_logs
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
                
                mail_data = {}
                mail_data['teamId'] = teamId
                mail_data['submissionId'] = str(submissionId)
                mail_data['submissionStatus'] = message
                mail_data['attachment'] = ""
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
                i.join()
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

    output_processor_fn(rank=1, event=threading.Event(), num_threads=1, submission_output_dir=docker_config["shared_output_dir"], answer_key_path=os.path.join(os.getcwd(), "answer"))
    signal.pause()