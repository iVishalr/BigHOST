from flask import Flask, request, jsonify
import sys
import os
sys.path.append(os.path.join(os.getcwd(), '..'))
from queues.redisqueue import RedisQueue
from flask_cors import cross_origin
import subprocess
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL)
import json
import requests
from redis import Redis
app = Flask(__name__)   
from pymongo import MongoClient
from dotenv import load_dotenv
from pprint import pprint
load_dotenv(os.path.join(os.getcwd(), '..', '.env'))
import filecmp

broker = Redis('localhost')
queue = RedisQueue(broker=broker, queue_name='output-queue')

client = MongoClient(os.getenv('MONGO_URI'))
db = client['bd']
submissions = db['submissions']

FILEPATH = os.path.join(os.getcwd(), 'output')
CORRECT_OUTPUT = os.path.join(os.getcwd(), 'correct_output')

@app.route('/output-check', methods=["POST"])
@cross_origin()
def output_check():
    '''
    Takes a submissions output and compares to the expected output
    '''

    data = json.loads(request.data)

    teamId = data['teamId']
    assignmentId = data['assignmentId']
    status = data['status']
    submissionId = data['submissionId']

    # If status is false, directly put 0
    if not status:
        doc = submissions.find_one({'teamId': data['teamId']})
        doc['assignments'][data['assignmentId']]['submissions'][data['submissionId']]['marks'] = 0
        doc['assignments'][data['assignmentId']]['submissions'][data['submissionId']]['message'] = 'Compilation error'
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

    return 'Data Updated'
