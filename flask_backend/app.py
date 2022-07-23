import os
import sys
import json
from pkg_resources import require
import requests
import subprocess
import pickle
from redis import Redis
from pprint import pprint
from dotenv import load_dotenv
from flask_cors import cross_origin
from flask_backend import queue, broker
from flask import Flask, request, jsonify
from signal import signal, SIGPIPE, SIG_DFL
from flask_backend import queue, broker, submissions
from pymongo import MongoClient, ReturnDocument
# sys.path.append(os.path.join(os.getcwd(), '..'))

signal(SIGPIPE, SIG_DFL)
app = Flask(__name__)   


def delete_files():
    for file in os.listdir('compile-test'):
        if file.endswith('.py'):
            os.remove('compile-test/' + file)
    
def update_submission(marks, message, data):
    doc = submissions.find_one({'teamId': data['teamId']})
    if doc is None:
        doc = {
            'teamId': data['teamId'],
            'teamBlacklisted': False,
            'assignments': {
                data['assignmentId']: {
                    'submissions': {
                        data['submissionId']: {
                            'marks': marks,
                            'message': message
                        }
                    }
                }
            }
        }
        submissions.insert_one(doc)
    else:
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
        # data = json.loads(request.data)
        data = submission

        mapper_data = data["mapper"]
        reducer_data = data['reducer']

        mapper = open('compile-test/mapper.py', 'w')
        mapper.write(mapper_data)
        mapper.close()
        mapper = open('compile-test/mapper.py', 'r')
        mapper_content = mapper.readlines()
        if mapper_content[0].strip() != '#!/usr/bin/env python3':
            delete_files()
            update_submission(marks=-1, message='Mapper shebang not present', data=data)
            res = {"msg": "Mapper shebang not present", "len": len(queue)}
            return jsonify(res)
        mapper.close()

        reducer = open('compile-test/reducer.py', 'w')
        reducer.write(reducer_data)
        reducer.close()
        reducer = open('compile-test/reducer.py', 'r')
        reducer_content = reducer.readlines()
        if reducer_content[0].strip() != '#!/usr/bin/env python3':
            delete_files()
            update_submission(marks=-1, message='Reducer shebang not present', data=data)
            res = {"msg": "Reducer shebang not present", "len": len(queue)}
            return jsonify(res)
        reducer.close()

        # for sub in submissions.find({'teamId': data['teamId']}):
        #     pprint(sub)

        process = subprocess.Popen(['pylint', '--disable=I,R,C,W', 'compile-test/'], stdout=subprocess.PIPE, text=True)
        exit_code = process.wait()
        #print(f'Exit Code: {exit_code}')
        output = process.communicate()[0]
        #print(output.decode('utf-8'), file=sys.stderr)
        delete_files()

        if exit_code != 0:
            doc = submissions.find_one({'teamId': data['teamId']})
            #print(doc)
            update_submission(marks=-1, message=output, data=data)
            res = {"msg": "Error", "len": len(queue)}
            return jsonify(res)

        data = pickle.dumps(data)
        queue.enqueue(data)
    
    res = {"msg": "Queued", "len": len(queue)}
    return jsonify(res)

@app.route('/get-jobs', methods=['GET'])
@cross_origin()
def get_jobs():
    data = json.loads(request.data)
    # Number of jobs
    num = data['prefetch_factor']

    if queue.is_empty():
        return None

    jobs = []
    if len(queue) <= num:
        while not queue.is_empty():
            queue_name, job = queue.dequeue()
            if job is not None:
                jobs.append(job)

    else:
        for i in range(num):
            queue_name, job = queue.dequeue()
            if job is not None:
                jobs.append(job)
    
    requests.post('http://localhost:10001/submit-job', json=jobs)

@app.route("/queue-length", methods=["GET"])
def queue_length():
    msg = {"length": len(queue)}
    return jsonify(msg)

@app.route("/empty-queue", methods=["GET"])
def empty_queue():
    queue.empty_queue()
    res = {"msg": "Queue Emptied"}
    return jsonify(res)

if __name__ == "__main__":
    app.run(port=9000, debug=False)