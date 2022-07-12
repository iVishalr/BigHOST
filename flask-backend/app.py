from urllib import response
from flask import Flask, request, jsonify
import py_compile
import sys

app = Flask(__name__)

@app.route('/sanity-check', methods=["POST"])
def sanity_check():
    files = request.files
    print(files.getlist('File'), file=sys.stderr)

    return jsonify("received")


if __name__ == "__main__":
    app.run(debug=True)