from re import L
from redis import Redis, ConnectionPool
from job_tracker import Job
from queues.redisqueue import RedisQueue
from flask import Flask, request, jsonify

import json
import pickle
import requests

pool = ConnectionPool(host='localhost', port=6379, db=0)
broker = Redis(connection_pool=pool)
queue = RedisQueue(broker, "submissionqueue")

app = Flask(__name__)

PORT = 9000

EVALUATION_ENGINE_IP = "localhost"
EVALUATION_ENGINE_FLASK_PORT = 10001
FLASK_ROUTE = "submit-job"

@app.route("/get-submissions", methods=["GET"])
def get_submissions():
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

    request_url = f"http://{EVALUATION_ENGINE_IP}:{EVALUATION_ENGINE_FLASK_PORT}/{FLASK_ROUTE}"

    r = requests.post(request_url, data=data)
    res = r.text

    res = {"msg": f"Dequeued {length} submissions from queue.", "num_submissions": length, "len": len(queue)}
    # res = {"msg": "dequeued from submission queue", "len": len(queue), "server_response": res}
    return jsonify(res)

@app.route("/empty-queue", methods=["GET"])
def empty_queue():
    queue.empty_queue()
    res = {"msg": "Queue Emptied"}
    return jsonify(res)

@app.route("/submit-job", methods=["POST"])
def submit_job():

    submission_data = json.loads(request.data)

    # print(submission_data)

    for i in range(len(submission_data)):

        submission = submission_data[i]
        # submission = json.loads(submission)

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

@app.route("/queue-length", methods=["GET"])
def queue_length():
    msg = {"length": len(queue)}
    return jsonify(msg)

if __name__ == "__main__":
    app.run("0.0.0.0", PORT)