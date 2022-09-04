import os
import sys
import json
import signal
import filecmp
import requests
import threading
import pickle
from time import sleep
from typing import List
from smtp import mail_queue
from datetime import datetime, timedelta

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

    def get_datetime(add_hours=0) -> str:  
        now = datetime.now() + timedelta(hours=add_hours)
        timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
        return timestamp

    f = open(f'./output_processor_logs.txt', 'a+')
    backup = sys.stdout
    sys.stdout = Tee(sys.stdout, f)

    from output_processor import queue, broker
    from output_processor import submissions_rr, submissions_ec

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

            FILEPATH_TEAM = os.path.join(FILEPATH, teamId, assignmentId)

            if not os.path.exists(FILEPATH_TEAM):
                status = "FAILED"

            if '1' == data['team_id'][2]:
                # check if the team is from RR campus
                submissions = submissions_rr
            else:
                submissions = submissions_ec

            # If status is false, directly put 0
            if status == "FAILED":
                doc = submissions.find_one({'teamId': teamId})
                print(f"[{get_datetime()}] [output_processor]\tTeam : {teamId} Assignment ID : {assignmentId} Result : Failed Message: {message}")
                doc['assignments'][assignmentId]['submissions'][submissionId]['marks'] = 0
                doc['assignments'][assignmentId]['submissions'][submissionId]['message'] = message
                doc['blacklisted']['status'] = teamBlacklisted
                
                if teamBlacklisted:
                    doc['blacklisted']['message'] = f"You are blocked from submitting. Come back at {get_datetime(add_hours=5)} to submit again."
                doc = submissions.find_one_and_update({'teamId': teamId}, {'$set': {'assignments': doc['assignments'], "blacklisted":  doc['blacklisted']}})
                
                mail_data = {}
                mail_data['teamId'] = teamId
                mail_data['submissionId'] = str(submissionId)
                mail_data['submissionStatus'] = message
                mail_data = pickle.dumps(mail_data)
                mail_queue.enqueue(mail_data)

            else:
                # Has given outuput, need to check if it is corect
                if os.path.exists(os.path.join(FILEPATH_TEAM, "part-00000")):
                    output = filecmp.cmp(os.path.join(FILEPATH_TEAM, "part-00000"), os.path.join(CORRECT_OUTPUT, assignmentId, "part-00000"), shallow=False)
                else:
                    output = None

                doc = submissions.find_one({'teamId': teamId})
                
                if output:
                    print(f"[{get_datetime()}] [output_processor]\tTeam : {teamId} Assignment ID : {assignmentId} Result : Passed")
                    doc['assignments'][assignmentId]['submissions'][submissionId]['marks'] = 1
                    doc['assignments'][assignmentId]['submissions'][submissionId]['message'] = 'Passed'
                    message = 'PASSED. Submission has passed our test cases. Good Job!'
                else:
                    print(f"[{get_datetime()}] [output_processor]\tTeam : {teamId} Assignment ID : {assignmentId} Result : Failed")
                    doc['assignments'][assignmentId]['submissions'][submissionId]['marks'] = 0
                    doc['assignments'][assignmentId]['submissions'][submissionId]['message'] = 'Failed'
                    message = 'FAILED. Submission did not passed our test cases. Try Again!'

                doc = submissions.find_one_and_update({'teamId': teamId}, {'$set': {'assignments': doc['assignments']}})
                
                mail_data = {}
                mail_data['teamId'] = teamId
                mail_data['submissionId'] = str(submissionId)
                mail_data['submissionStatus'] = message
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
    output_processor_fn(rank=1, event=threading.Event(), num_threads=1, submission_output_dir=os.path.join(os.getcwd(),"output"), answer_key_path=os.path.join(os.getcwd(), "answer"))
    signal.pause()