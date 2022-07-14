from urllib import response
from flask import Flask, request, jsonify
import sys
import os
import subprocess
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL)

app = Flask(__name__)

@app.route('/sanity-check', methods=["POST"])
def sanity_check():
    files = request.files.to_dict()
    file = files['File']
    
    file.save(file.filename)
    process = subprocess.Popen(['pylint', '--disable=I,R,C,W', f'{file.filename}'], stdout=subprocess.PIPE)
    output = process.communicate()[0]
    #print(type(output))
    os.system(f'rm {file.filename}')
    
    #print(output, file=sys.stderr)
    if "error" in output.decode('utf-8'):
        message = "Error"
        status_code = 400
    else:
        message = "No Error"
        status_code = 200

    return jsonify({'message': message}), status_code


if __name__ == "__main__":
    app.run(debug=True)