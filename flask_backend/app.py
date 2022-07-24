import os
import sys
import json
import requests
import subprocess
import pickle

from pprint import pprint
from dotenv import load_dotenv
from flask_cors import cross_origin
from flask_backend import queue
from flask import Flask, request, jsonify
from signal import signal, SIGPIPE, SIG_DFL
from pymongo import MongoClient, ReturnDocument


signal(SIGPIPE, SIG_DFL) 

def createApp():

    load_dotenv(os.path.join(os.getcwd(), '.env'))

    client = MongoClient(os.getenv('MONGO_URI'), connect=False)
    db = client['bd']
    submissions = db['submissions']

    app = Flask(__name__)  
    def delete_files():
        for file in os.listdir('compile-test'):
            if file.endswith('.py'):
                os.remove('compile-test/' + file)
        
    def update_submission(marks, message, data):
        
        doc = submissions.find_one({'teamId': data['teamId']})
        # if doc is None:
        #     doc = {
        #         'teamId': data['teamId'],
        #         'teamBlacklisted': False,
        #         'assignments': {
        #             data['assignmentId']: {
        #                 'submissions': {
        #                     data['submissionId']: {
        #                         'marks': marks,
        #                         'message': message
        #                     }
        #                 }
        #             }
        #         }
        #     }
        #     submissions.insert_one(doc)
        # else:
        doc['assignments'][data['assignmentId']]['submissions'][data['submissionId']]['marks'] = marks
        doc['assignments'][data['assignmentId']]['submissions'][data['submissionId']]['message'] = message
        doc = submissions.find_one_and_update({'teamId': data['teamId']}, {'$set': {'assignments': doc['assignments']}})


    @app.route('/sanity-check', methods=["POST"])
    @cross_origin()
    def sanity_check():
        '''
        Currently assuming the assignment to be a MR Job
        '''
        jobs = json.loads(request.data)
        for submission in jobs:
            data = submission
            update_submission(marks=-1, message='testing', data=data)
            mapper_data = data["mapper"]
            reducer_data = data['reducer']
            mapper_name = f"{data['teamId']}-{data['assignmentId']}-mapper.py"
            reducer_name = f"{data['teamId']}-{data['assignmentId']}-reducer.py"

            if not os.path.exists(os.path.join(os.getcwd(), "compile-test")):
                os.makedirs(os.path.join(os.getcwd(), "compile-test"))

            if mapper_data.strip().split("\n")[0] != '#!/usr/bin/env python3':
                update_submission(marks=-1, message='Mapper shebang not present', data=data)
                res = {"msg": "Mapper shebang not present", "len": len(queue)}
                return jsonify(res)

            if reducer_data.strip().split("\n")[0] != '#!/usr/bin/env python3':
                update_submission(marks=-1, message='Reducer shebang not present', data=data)
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
                update_submission(marks=-1, message=output, data=data)
                res = {"msg": "Error", "len": len(queue)}
                return jsonify(res)
            elif exit_code == 0:
                update_submission(marks=1, message='Sanity Check Passed', data=data)

            data = pickle.dumps(data)
            queue.enqueue(data)
        
        res = {"msg": "Queued", "len": len(queue)}
        return jsonify(res)

    @app.route('/get-jobs', methods=['GET'])
    @cross_origin()
    def get_jobs():
    #     #print(request.data)
    #     # data = json.loads(request.data)
    #     # Number of jobs
    #     num = int(request.args.get("prefetch_factor"))
    #     print(num)
    #     if queue.is_empty():
    #         return None

    #     jobs = []
    #     if len(queue) > 0:
    #         if len(queue) <= num:
    #             while not queue.is_empty():
    #                 queue_data = queue.dequeue()
    #                 if queue_data is not None:
    #                     queue_name, job = queue_data
    #                 if job is not None:
    #                     jobs.append(pickle.loads(job))

    #         else:
    #             for i in range(num):
    #                 queue_data = queue.dequeue()
    #                 if queue_data is not None:
    #                     queue_name, job = queue_data
    #                 if job is not None:
    #                     jobs.append(pickle.loads(job))
            
    #     r = requests.post('http://localhost:10001/submit-job', json=jobs)
    #     res = {"msg": f"Dequeued {len(jobs)} submissions from queue.", "num_submissions": len(jobs), "len": len(queue)}
    #     print(res)
    # # res = {"msg": "dequeued from submission queue", "len": len(queue), "server_response": res}
    #     return jsonify(res)
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

        request_url = f"http://localhost:10001/submit-job"

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