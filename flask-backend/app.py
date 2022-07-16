from flask import Flask, request, jsonify
import os
from queues import RedisQueue
from flask_cors import cross_origin
import subprocess
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL)
import json
from redis import Redis
app = Flask(__name__)

broker = Redis('localhost')
queue = RedisQueue(broker=broker, queue_name='sanity-queue')

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

    process = subprocess.Popen(['pylint', '--disable=I,R,C,W', 'compile-test/'], stdout=subprocess.PIPE)
    output = process.communicate()[0]
    for file in os.listdir('compile-test'):
        if file.endswith('.py'):
            os.remove('compile-test/' + file)
    
    if "syntax-error" in output.decode('utf-8'):
        return "error"

    queue.enqueue(data)

    return "received"

if __name__ == "__main__":
    app.run(debug=True)