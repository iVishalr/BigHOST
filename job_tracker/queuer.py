import json
import pickle
from output_processor import queue as output_queue
from job_tracker import queue
from job_tracker.job import Job
from flask import Flask, request, jsonify
import os
from pymongo import MongoClient

app = Flask(__name__)

PORT = 10001

client = MongoClient(os.getenv('MONGO_URI'), connect=False)
db = client['bd']
submissions = db['submissions']

def updateSubmission(marks, message, data):
    doc = submissions.find_one({'teamId': data['teamId']})
    doc['assignments'][data['assignmentId']]['submissions'][str(data['submissionId'])]['marks'] = marks
    doc['assignments'][data['assignmentId']]['submissions'][str(data['submissionId'])]['message'] = message
    doc = submissions.find_one_and_update({'teamId': data['teamId']}, {'$set': {'assignments': doc['assignments']}})

@app.route("/submit-job", methods=["POST"])
def submit_job():

    submission_data = json.loads(request.data)

    for i in range(len(submission_data)):

        submission = submission_data[i]
        print(submission)

        TEAM_ID = submission["teamId"]
        ASSIGNMENT_ID = submission["assignmentId"]
        SUBMISSION_ID = submission['submissionId']
        TIMEOUT = float(submission["timeout"])
        # TASK = submission["task"]
        MAPPER = submission["mapper"]
        REDUCER = submission["reducer"] 

        job = Job(  team_id = TEAM_ID,
                    assignment_id = ASSIGNMENT_ID,
                    timeout = TIMEOUT,
                    submission_id = SUBMISSION_ID,
                    mapper = MAPPER,
                    reducer = REDUCER,
                )

        data = job.__dict__

        serialized_job = pickle.dumps(data)
        queue.enqueue(serialized_job)

        updateSubmission(marks=-1, message='In Queue', data=submission)

    res = {"msg": "Queued", "len": len(queue)}
    return jsonify(res)

@app.route("/get-job", methods=["GET"])
def get_job():
    queue_name, serialized_job = queue.dequeue()
    job = pickle.loads(serialized_job)
    res = {"msg": "dequeued", "len": len(queue), "job": job}
    return jsonify(res)

@app.route("/queue-length", methods=["GET"])
def queue_length():
    res = {"length": len(queue), "queue_name": queue.queue_name}
    return jsonify(res)

@app.route("/empty-queue", methods=["GET"])
def empty_queue():
    queue.empty_queue()
    res = {"msg": "Queue Emptied"}
    return jsonify(res)

@app.route("/output-queue-length", methods=["GET"])
def output_queue_length():
    res = {"length": len(output_queue), "queue_name": output_queue.queue_name}
    return jsonify(res)

@app.route("/empty-output-queue", methods=["GET"])
def empty_output_queue():
    output_queue.empty_queue()
    res = {"msg": "Output Queue Emptied"}
    return jsonify(res)

if __name__ == "__main__":
    app.run("0.0.0.0", PORT)