import os
import json
import pickle
from pymongo import MongoClient
from job_tracker.job import MRJob, SparkJob, KafkaJob
from flask import Flask, request, jsonify
from output_processor import queue as output_queue
from job_tracker import queue, submissions_rr, submissions_ec

app = Flask(__name__)

PORT = 10001

def updateSubmission(marks, message, data):
    if '1' == data['teamId'][2]:
        # check if the team is from RR campus
        submissions = submissions_rr
    else:
        submissions = submissions_ec
    doc = submissions.find_one({'teamId': data['teamId']})
    doc['assignments'][data['assignmentId']]['submissions'][str(data['submissionId'])]['marks'] = marks
    doc['assignments'][data['assignmentId']]['submissions'][str(data['submissionId'])]['message'] = message
    doc = submissions.find_one_and_update({'teamId': data['teamId']}, {'$set': {'assignments': doc['assignments']}})

@app.route("/submit-job", methods=["POST"])
def submit_job():

    submission_data = json.loads(request.data)

    for i in range(len(submission_data)):

        submission = submission_data[i]

        TEAM_ID = submission["teamId"]
        ASSIGNMENT_ID = submission["assignmentId"]
        SUBMISSION_ID = submission['submissionId']
        TIMEOUT = float(submission["timeout"])
        # TASK = submission["task"]
        
        if "A3" not in ASSIGNMENT_ID:
            MAPPER = submission["mapper"]
            REDUCER = submission["reducer"] 
            job = MRJob(  
                    team_id = TEAM_ID,
                    assignment_id = ASSIGNMENT_ID,
                    timeout = TIMEOUT,
                    submission_id = SUBMISSION_ID,
                    mapper = MAPPER,
                    reducer = REDUCER,
                )
        else:
            if "T1" in ASSIGNMENT_ID:
                SPARK = submission["spark"]
                job = SparkJob(
                    team_id = TEAM_ID,
                    assignment_id = ASSIGNMENT_ID,
                    timeout = TIMEOUT,
                    submission_id = SUBMISSION_ID,
                    spark = SPARK
                )
            elif "T2" in ASSIGNMENT_ID:
                PRODUCER = submission["producer"]
                CONSUMER = submission["consumer"]
                job = KafkaJob(
                    team_id = TEAM_ID,
                    assignment_id = ASSIGNMENT_ID,
                    timeout = TIMEOUT,
                    submission_id = SUBMISSION_ID,
                    producer = PRODUCER,
                    consumer = CONSUMER
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