import os
import sys
import json
import time
import pickle
import requests
import subprocess

from pprint import pprint
from smtp import mail_queue
from datetime import datetime
from dotenv import load_dotenv
from flask_backend import queue
from pymongo import MongoClient
from flask_cors import cross_origin
from flask import Flask, request, jsonify
from signal import signal, SIGPIPE, SIG_DFL

signal(SIGPIPE, SIG_DFL) 

def createApp():

    load_dotenv(os.path.join(os.getcwd(), '.env'))

    client_rr = MongoClient(os.getenv('MONGO_URI_RR'), connect=False)
    db_rr = client_rr['bd']
    submissions_rr = db_rr['submissions']

    client_ec = MongoClient(os.getenv('MONGO_URI_EC'), connect=False)
    db_ec = client_ec['bd']
    submissions_ec = db_ec['submissions']

    evaluator_internal_ip = os.getenv('EVALUATOR_INTERNAL_IP')
    evaluator_external_ip = os.getenv('EVALUATOR_EXTERNAL_IP')
    
    os.environ['TZ'] = 'Asia/Kolkata'
    time.tzset()

    app = Flask(__name__)  

    class Tee(object):
        def __init__(self, *files):
            self.files = files
        def write(self, obj):
            for f in self.files:
                f.write(obj)
        def flush(self):
            pass

    f = open(f'./sanity_checker_logs.txt', 'a+')
    backup = sys.stdout
    sys.stdout = Tee(sys.stdout, f)

    def get_datetime() -> str:
        now = datetime.now()
        timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
        return timestamp
    
    def delete_files():
        for file in os.listdir('compile-test'):
            if file.endswith('.py'):
                os.remove('compile-test/' + file)
        
    def update_submission(marks, message, data, send_mail=False):
        if '1' == data['teamId'][2]:
            # check if the team is from RR campus
            submissions = submissions_rr
        else:
            submissions = submissions_ec

        doc = submissions.find_one({'teamId': data['teamId']})
        # if doc is None:
        #     doc = {
        #         'teamId': data['teamId'],
        #         'blacklisted': {
        #             'status': False,
        #             'message': "",
        #             'timestamp': str(time.time_ns)[:13],
        #         },
        #         'assignments': {
        #             data['assignmentId']: {
        #                 'submissions': {
        #                     str(data['submissionId']): {
        #                         'marks': marks,
        #                         'message': message
        #                     }
        #                 }
        #             }
        #         }
        #     }
        #     submissions.insert_one(doc)
        # else:
        if str(data['submissionId']) not in doc['assignments'][data['assignmentId']]['submissions']:
            doc['assignments'][data['assignmentId']]['submissions'][str(data['submissionId'])] = {
                str(data["submissionId"]): {
                    "data": {
                        "mapper": data["mapper"],
                        "reducer": data["reducer"]
                    },
                "timestamp": int(str(time.time_ns())[:10]),
                "marks": marks,
                "message": message
                }
            }
            doc = submissions.find_one_and_update({'teamId': data['teamId']}, {'$set': {'assignments': doc['assignments']}})
        else:
            doc['assignments'][data['assignmentId']]['submissions'][str(data['submissionId'])]['marks'] = marks
            doc['assignments'][data['assignmentId']]['submissions'][str(data['submissionId'])]['message'] = message
            doc = submissions.find_one_and_update({'teamId': data['teamId']}, {'$set': {'assignments': doc['assignments']}})
        
        if send_mail:
            mail_data = {}
            mail_data['teamId'] = data['teamId']
            mail_data['submissionId'] = str(data['submissionId'])
            mail_data['submissionStatus'] = message
            mail_data['attachment'] = ""
            mail_data = pickle.dumps(mail_data)
            mail_queue.enqueue(mail_data)


    @app.route('/sanity-check', methods=["POST"])
    @cross_origin()
    def sanity_check():
        '''
        Currently assuming the assignment to be a MR Job
        '''
        jobs = json.loads(request.data)
        data = jobs
        
        update_submission(marks=-1, message='Sanity Checking', data=data)
        
        mapper_data = data["mapper"]
        reducer_data = data['reducer']
        mapper_name = f"{data['teamId']}-{data['assignmentId']}-mapper.py"
        reducer_name = f"{data['teamId']}-{data['assignmentId']}-reducer.py"

        if not os.path.exists(os.path.join(os.getcwd(), "compile-test")):
            os.makedirs(os.path.join(os.getcwd(), "compile-test"))

        if mapper_data.strip().split("\n")[0] != '#!/usr/bin/env python3':
            print(f"[{get_datetime()}] [sanity_checker]\tTeam : {data['teamId']} Assignment ID : {data['assignmentId']} Message : Mapper Shebang Not Present. Check if your file is in LF format.")
            update_submission(marks=-1, message='Mapper Shebang Not Present. Make sure shebang is present as the first line and check if file is in LF format.', data=data, send_mail=True)
            res = {"msg": "Mapper shebang not present", "len": len(queue)}
            return jsonify(res)

        if reducer_data.strip().split("\n")[0] != '#!/usr/bin/env python3':
            print(f"[{get_datetime()}] [sanity_checker]\tTeam : {data['teamId']} Assignment ID : {data['assignmentId']} Message : Reducer Shebang Not Present. Check if your file is in LF format.")
            update_submission(marks=-1, message='Reducer Shebang Not Present. Make sure shebang is present as the first line and check if file is in LF format.', data=data, send_mail=True)
            res = {"msg": "Reducer shebang not present", "len": len(queue)}
            return jsonify(res)

        mapper = open(f'compile-test/{mapper_name}', 'w')
        mapper.write(mapper_data)
        mapper.close()

        reducer = open(f'compile-test/{reducer_name}', 'w')
        reducer.write(reducer_data)
        reducer.close()

        process = subprocess.Popen([f'pylint --disable=I,R,C,W {os.path.join(os.getcwd(), "compile-test/")}'], shell=True, stdout=subprocess.PIPE, text=True)
        exit_code = process.wait()

        output = process.communicate()[0]
        delete_files()

        if exit_code != 0:
            print(f"[{get_datetime()}] [sanity_checker]\tTeam : {data['teamId']} Assignment ID : {data['assignmentId']} Message : Failed Sanity Check. Check your files for syntax errors or illegal module imports. Error Log : {output}")
            update_submission(marks=-1, message="Failed Sanity Check. Check your files for syntax errors or illegal module imports. Error Log : "+output, data=data, send_mail=True)
            res = {"msg": "Error", "len": len(queue)}
            return jsonify(res)
        elif exit_code == 0:
            print(f"[{get_datetime()}] [sanity_checker]\tTeam : {data['teamId']} Assignment ID : {data['assignmentId']} Message : Passed Sanity Check.")
            update_submission(marks=-1, message='Sanity Check Passed', data=data)

        data['timeout'] = 30
        update_submission(marks=-1, message='Queued for Execution', data=data)

        data = pickle.dumps(data)
        queue.enqueue(data)
        
        res = {"msg": "Queued", "len": len(queue)}
        return jsonify(res)

    @app.route('/get-jobs', methods=['GET'])
    @cross_origin()
    def get_jobs():
        prefetch_factor = int(request.args.get("prefetch_factor"))

        if prefetch_factor is None: prefetch_factor = 1

        if len(queue) == 0:
            res = {"msg": "Submission Queue is currently empty.", "len": len(queue), "num_submissions": 0}
            return jsonify(res)

        data = []
        i = 0
        while i < prefetch_factor:
            queue_data = queue.dequeue()

            if queue_data is None:
                break
            
            queue_name, serialized_job = queue_data
            job = pickle.loads(serialized_job)
            data.append(job)
            i += 1  

        length = len(data)
        data = json.dumps(data)

        request_url = f"http://{evaluator_internal_ip}:10001/submit-job"

        r = requests.post(request_url, data=data)
        res = r.text

        res = {"msg": f"Dequeued {length} submissions from queue.", "num_submissions": length, "len": len(queue)}
        # res = {"msg": "dequeued from submission queue", "len": len(queue), "server_response": res}
        return jsonify(res)


    @app.route("/queue-length", methods=["GET"])
    def queue_length():
        msg = {"length": len(queue)}
        return jsonify(msg)

    @app.route("/empty-queue", methods=["GET"])
    def empty_queue():
        queue.empty_queue()
        res = {"msg": "Queue Emptied"}
        return jsonify(res)

    return app

if __name__ == "__main__":
    createApp()
    # app.run(port=9000, debug=False)