import os
import json
import time
import pickle

from job_tracker import queue
from common.db import DataBase
from flask import Flask, request, jsonify
from flask_cors import cross_origin
from job_tracker.job import MRJob, SparkJob, KafkaJob
from output_processor import queue as output_queue

app = Flask(__name__)

PORT = 10001
db = DataBase()

def updateSubmission(marks, message, data):
    timestamp = int(str(time.time_ns())[:10])
    doc = db.update("submissions", data['teamId'], None, data['assignmentId'], str(data['submissionId']), marks, message, timestamp)

@app.route("/submit-job", methods=["POST"])
@cross_origin()
def submit_job():
    submission_data = json.loads(request.data)

    for i in range(len(submission_data)):

        submission = submission_data[i]

        TEAM_ID = submission["teamId"]
        ASSIGNMENT_ID = submission["assignmentId"]
        SUBMISSION_ID = submission["submissionId"]
        TIMEOUT = float(submission["timeout"])
        
        if "A1" in ASSIGNMENT_ID or "A2" in ASSIGNMENT_ID:
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
        elif "A3" in ASSIGNMENT_ID:
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
        else:
            continue

        data = job.__dict__

        serialized_job = pickle.dumps(data)
        queue.enqueue(serialized_job)

        updateSubmission(marks=-1, message='In Queue', data=submission)
        submission = None

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