from job_tracker.job import Job
from queues import RedisQueue

import json
import pickle
from flask import Flask, request, jsonify
from redis import Redis
from pprint import pprint

broker = Redis("localhost")
queue = RedisQueue(broker=broker, queue_name="jobqueue")

app = Flask(__name__)

PORT = 10001

@app.route("/submit-job", methods=["POST"])
def submit_job():

    submission_data = json.loads(request.data)

    for i in range(len(submission_data)):

        submission = submission_data[i]

        TEAM_ID = submission["team_id"]
        ASSIGNMENT_ID = submission["assignment_id"]
        TIMEOUT = float(submission["timeout"])
        TASK = submission["task"]
        MAPPER = submission["mapper"]
        REDUCER = submission["reducer"] 

        job = Job(  team_id = TEAM_ID,
                    assignment_id = ASSIGNMENT_ID,
                    timeout = TIMEOUT,
                    task = TASK,
                    mapper = MAPPER,
                    reducer = REDUCER,
                )

        data = job.__dict__

        serialized_job = pickle.dumps(data)
        queue.enqueue(serialized_job)

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

if __name__ == "__main__":
    app.run("0.0.0.0", PORT)