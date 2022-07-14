from urllib import response
from flask import Flask, request, jsonify
import requests
import sys
import os
import subprocess
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL)

app = Flask(__name__)

@app.route('/sanity-check', methods=["POST"])
def sanity_check():
    '''
    Currently assuming the assignment to be a MR Job
    '''
    files = request.files.to_dict()
    mapper = files['Mapper']
    reducer = files['Reducer']
    team_ID = request.form.get('Team ID')
    assignment_ID = request.form.get('Assignment ID')
    timeout = request.form.get('Timeout')
    task = request.form.get('Task')

    #print(request.get_json(), file=sys.stderr)
    #print(request.form, file=sys.stderr)
    # Mapper part
    mapper.save(mapper.filename)
    process = subprocess.Popen(['pylint', '--disable=I,R,C,W', f'{mapper.filename}'], stdout=subprocess.PIPE)
    mapper_output = process.communicate()[0]
    #print(mapper, file=sys.stderr)
    #print(type(output))
    os.system(f'rm {mapper.filename}')
    # Reducer Part
    reducer.save(reducer.filename)
    process = subprocess.Popen(['pylint', '--disable=I,R,C,W', f'{reducer.filename}'], stdout=subprocess.PIPE)
    reducer_output = process.communicate()[0]
    #print(type(output))
    os.system(f'rm {reducer.filename}')
    
    #print(output, file=sys.stderr)
    if "error" in mapper_output.decode('utf-8') or "error" in reducer_output.decode('utf-8'):
        message = "Error"
        status_code = 400
    else:
        message = "No Error"
        status_code = 200
        # Send request to submit job
        #print(request.form.to_dict(flat=True), file=sys.stderr)
        requests.post('http://localhost:10001/submit-job', data=request.form.to_dict(flat=True), files={"Mapper": mapper, "Reducer": reducer})

    return jsonify({'message': message}), status_code


if __name__ == "__main__":
    app.run(debug=False)