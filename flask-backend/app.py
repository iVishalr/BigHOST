from urllib import response
from flask import Flask, request, jsonify
import sys
import os
import subprocess

app = Flask(__name__)

@app.route('/sanity-check', methods=["POST"])
def sanity_check():
    files = request.files.to_dict()
    file = files['File']
    
    file.save(file.filename)
    process = subprocess.run(['pylint', '--disable=I,R,C,W', f'{file.filename}'], stdout=subprocess.PIPE)
    os.system(f'rm {file.filename}')
    

    if "error" in process.stdout.decode('utf-8'):
        message = "Error"
        status_code = 400
    else:
        message = "No Error"
        status_code = 200

    return jsonify({'message': message}), status_code


if __name__ == "__main__":
    app.run(debug=True)