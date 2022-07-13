from redis import Redis
from job_tracker import Job
from queues.redisqueue import RedisQueue
from flask import Flask, request, jsonify

import json
import pickle
import requests

broker = Redis("localhost")
queue = RedisQueue(broker, "submissionqueue")

app = Flask(__name__)

PORT = 9000

EVALUATION_ENGINE_IP = "localhost"
EVALUATION_ENGINE_FLASK_PORT = 10001
FLASK_ROUTE = "submit-job"

@app.route("/get-submissions", methods=["GET"])
def get_submissions():
    queue_name, serialized_job = queue.dequeue()
    job = pickle.loads(serialized_job)

    request_url = f"http://{EVALUATION_ENGINE_IP}:{EVALUATION_ENGINE_FLASK_PORT}/{FLASK_ROUTE}"

    r = requests.post(request_url, data=job)
    res = r.text

    res = {"msg": "dequeued from submission queue", "len": len(queue), "server_response": res}
    return jsonify(res)

@app.route("/empty-queue", methods=["GET"])
def empty_queue():
    queue.empty_queue()
    res = {"msg": "Queue Emptied"}
    return jsonify(res)

@app.route("/submit-job", methods=["POST"])
def submit_job():

    TEAM_ID = request.form["team_id"]
    ASSIGNMENT_ID = request.form["assignment_id"]
    TIMEOUT = float(request.form["timeout"])
    TASK = request.form["task"]
    MAPPER = request.form["mapper"]
    REDUCER = request.form["reducer"] 

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
    app.run()