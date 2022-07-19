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

broker = Redis('localhost')
queue = RedisQueue(broker=broker, queue_name='sanity-queue')

client = MongoClient(os.getenv('MONGO_URI'))
# print(client.list_database_names())
db = client['bd']
submissions = db['submissions']
# print(db.list_collection_names())
# for post in submissions.find():
 #   pprint(post)

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
    reducer = open('compile-test/reducer.py', 'w')
    reducer.write(reducer_data)
    reducer.close()

    for sub in submissions.find({'teamID': data['teamID']}):
        pprint(sub)

    process = subprocess.Popen(['pylint', '--disable=I,R,C,W', 'compile-test/'], stdout=subprocess.PIPE)
    output = process.communicate()[0]
    for file in os.listdir('compile-test'):
        if file.endswith('.py'):
            os.remove('compile-test/' + file)
    
    if "syntax-error" in output.decode('utf-8'):
        return "error"

    queue.enqueue(data)
    db.findO
    return "received"

@app.route('/get-jobs', methods=['GET'])
@cross_origin()
def get_jobs():
    data = json.loads(request.data)
    # Number of jobs
    num = data['jobs']

    if queue.is_empty():
        # TODO
        pass

    jobs = []
    if len(queue) <= num:
        while not queue.is_empty():
            queue_name, job = queue.dequeue()
            if job is not None:
                jobs.append(job)
            #jobs.append(job)
    else:
        for i in range(num):
            queue_name, job = queue.dequeue()
            if job is not None:
                jobs.append(job)
    
    requests.post('http://localhost:10001/submit-job', json=jobs)

if __name__ == "__main__":
    app.run(debug=True)