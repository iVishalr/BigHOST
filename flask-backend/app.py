from functools import reduce
from urllib import response
from flask import Flask, request, jsonify
from flask_cors import cross_origin
import requests
import sys
import os
import subprocess
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL)
import json

app = Flask(__name__)


@app.route('/sanity-check', methods=["POST"])
@cross_origin()
def sanity_check():
    '''
    Currently assuming the assignment to be a MR Job
    '''
    data = json.loads(request.data)

    mapper_data = data["mapper"]
    reducer_data = data['reducer']

    mapper = open('mapper.py', 'w')
    mapper.write(mapper_data)
    mapper.close()
    reducer = open('reducer.py', 'w')
    reducer.write(reducer_data)
    reducer.close()

    process = subprocess.Popen(['pylint', '--disable=I,R,C,W', 'mapper.py'], stdout=subprocess.PIPE)
    mapper_output = process.communicate()[0]
    os.system('rm mapper.py')
    process = subprocess.Popen(['pylint', '--disable=I,R,C,W', 'reducer.py'], stdout=subprocess.PIPE)
    reducer_output = process.communicate()[0]
    os.system('rm reducer.py')

    if "error" in mapper_output.decode('utf-8') or "error" in reducer_output.decode('utf-8'):
        return jsonify({"msg": "error"})

    return "received"

if __name__ == "__main__":
    app.run(debug=True)