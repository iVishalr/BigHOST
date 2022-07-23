import os
import sys
import json
import signal
import filecmp
import requests
import threading

from time import sleep
from typing import List
from datetime import datetime

def output_processor_fn(rank: int, event: threading.Event, num_threads: int):
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

    f = open(f'./output_processor_logs.txt', 'w+')
    backup = sys.stdout
    sys.stdout = Tee(sys.stdout, f)

    from output_processor import queue, broker
    from output_processor import submissions

    FILEPATH = os.path.join(os.getcwd(), 'output')
    CORRECT_OUTPUT = os.path.join(os.getcwd(), 'correct_output')

    def thread_fn():
        interval = 0.05
        timeout = 0.05

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
            data = json.loads(serialized_data)

            teamId = data['teamId']
            assignmentId = data['assignmentId']
            status = data['status']
            submissionId = data['submissionId']
            message = data["job_output"]

            # If status is false, directly put 0
            if not status == "FAILED":
                doc = submissions.find_one({'teamId': data['teamId']})
                doc['assignments'][data['assignmentId']]['submissions'][data['submissionId']]['marks'] = 0
                doc['assignments'][data['assignmentId']]['submissions'][data['submissionId']]['message'] = message
                doc = submissions.find_one_and_update({'teamId': data['teamId']}, {'$set': {'assignments': doc['assignments']}})
            else:
                # Has given outuput, need to check if it is corect
                output = filecmp.cmp(os.path.join(FILEPATH, teamId, submissionId), os.path.join(CORRECT_OUTPUT, assignmentId))
                doc = submissions.find_one({'teamId': data['teamId']})
                if output:
                    doc['assignments'][data['assignmentId']]['submissions'][data['submissionId']]['marks'] = 1
                    doc['assignments'][data['assignmentId']]['submissions'][data['submissionId']]['message'] = 'Passed'
                else:
                    doc['assignments'][data['assignmentId']]['submissions'][data['submissionId']]['marks'] = 0
                    doc['assignments'][data['assignmentId']]['submissions'][data['submissionId']]['message'] = 'Failed'
                doc = submissions.find_one_and_update({'teamId': data['teamId']}, {'$set': {'assignments': doc['assignments']}})

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
    output_processor_fn(1, threading.Event(), 1)