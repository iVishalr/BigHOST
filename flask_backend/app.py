import os
import sys
import json
import requests
import subprocess

from redis import Redis
from pprint import pprint
from dotenv import load_dotenv
from flask_cors import cross_origin
from flask_backend import queue, broker
from flask import Flask, request, jsonify
from signal import signal, SIGPIPE, SIG_DFL
from pymongo import MongoClient, ReturnDocument
# sys.path.append(os.path.join(os.getcwd(), '..'))

signal(SIGPIPE, SIG_DFL)
app = Flask(__name__)   
load_dotenv(os.path.join(os.getcwd(), '..', '.env'))

client = MongoClient(os.getenv('MONGO_URI'))
db = client['bd']
submissions = db['submissions']

def delete_files():
    for file in os.listdir('compile-test'):
        if file.endswith('.py'):
            os.remove('compile-test/' + file)
    


@app.route('/sanity-check', methods=["POST"])
@cross_origin()
def sanity_check():
    '''
    Currently assuming the assignment to be a MR Job
    '''
    data = json.loads(request.data)

    mapper_data = data["mapper"]
    reducer_data = data['reducer']

    mapper = open('compile-test/mapper.py', 'w')
    mapper.write(mapper_data)
    mapper.close()
    mapper = open('compile-test/mapper.py', 'r')
    mapper_content = mapper.readlines()
    if mapper_content[0].strip() != '#!/usr/bin/env python3':
        delete_files()
        return "mapper shebang not present"
    mapper.close()

    reducer = open('compile-test/reducer.py', 'w')
    reducer.write(reducer_data)
    reducer.close()
    reducer = open('compile-test/reducer.py', 'r')
    reducer_content = reducer.readlines()
    if reducer_content[0].strip() != '#!/usr/bin/env python3':
        delete_files()
        return "reducer shebang not present"
    reducer.close()

    for sub in submissions.find({'teamId': data['teamId']}):
        pprint(sub)

    process = subprocess.Popen(['pylint', '--disable=I,R,C,W', 'compile-test/'], stdout=subprocess.PIPE)
    output = process.communicate()[0]
    
    delete_files()

    if "syntax-error" in output.decode('utf-8'):
        doc = submissions.find_one({'teamId': data['teamId']})
        doc['assignments'][data['assignmentId']]['submissions'][data['submissionId']]['marks'] = -1
        doc['assignments'][data['assignmentId']]['submissions'][data['submissionId']]['message'] = 'test'
        doc = submissions.find_one_and_update({'teamId': data['teamId']}, {'$set': {'assignments': doc['assignments']}})
        return "error"

    queue.enqueue(data)
    
    return "received"

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

if __name__ == "__main__":
    app.run(debug=False)