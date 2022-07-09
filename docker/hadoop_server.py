from flask import Flask, jsonify, request
import json
import os
import subprocess
import time
import stat
import os
import signal

app = Flask(__name__)

HOST = "0.0.0.0"
PORT = 10000
PATH_TO_STREAMING = "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar"

@app.route("/run_job", methods=["POST"])
def run_job():
    TEAM_ID = request.form["team_id"]
    ASSIGNMENT_ID = request.form["assignment_id"]
    TIMEOUT = float(request.form["timeout"])
    submissions_dir = "submissions"
    
    path = os.path.join(submissions_dir, TEAM_ID)
    if not os.path.exists(path):
        os.mkdir(path)
    
    MAPPER = request.form["mapper"]
    REDUCER = request.form["reducer"]

    with open(os.path.join(path,"mapper.py"), "w") as f:
        f.write(MAPPER)

    with open(os.path.join(path,"reducer.py"), "w") as f:
        f.write(REDUCER)

    st = os.stat(os.path.join(path,"mapper.py"))
    os.chmod(os.path.join(path,"mapper.py"), st.st_mode | stat.S_IEXEC)

    st = os.stat(os.path.join(path,"reducer.py"))
    os.chmod(os.path.join(path,"reducer.py"), st.st_mode | stat.S_IEXEC)

    mapred_process = subprocess.Popen([
        f'''/opt/hadoop/bin/hadoop jar {PATH_TO_STREAMING} -mapper "/{os.path.join(path,'mapper.py')}" -reducer "'/{os.path.join(path,'reducer.py')}' '/{os.path.join(path,'v')}'" -input /{ASSIGNMENT_ID}/input/dataset_1percent.txt -output /{TEAM_ID}/{ASSIGNMENT_ID}/output'''
    ], shell=True, text=True, preexec_fn=os.setsid)

    
    try:
        process_exit_code = job_timer(mapred_process, TIMEOUT) # timeout is in seconds
        if process_exit_code == 0:
            msg = "Job Completed Successfully!"
        else:
            msg = f"Job Failed due to : \n{mapred_process.stderr}"
    except RuntimeError:
        process_exit_code = -1
        msg = "Submission has taken more time than the alloted time. Submission has been killed!"
        print(msg)
        # make updates to database here
        # ....
        # ....
    res = {"text": msg, "status_code": 200, "process_exit_code": process_exit_code}
    return jsonify(res)

@app.route("/", methods=["GET"])
def init():
    return jsonify({"response": "Server is Running on "})

def job_timer(proc: subprocess.Popen, timeout: int) -> int:
    start = time.time()
    end = start + timeout

    interval = min(timeout/1000.0, .25)

    while True:
        process_result = proc.poll()
        if process_result is not None:
            return process_result
        if time.time() >= end:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            raise RuntimeError("Process Timed Out")
        time.sleep(interval)
    

if __name__ == "__main__":
    hadoop_initializer_process = subprocess.Popen(["bash /start-hadoop.sh"], shell=True, text=True)
    res = hadoop_initializer_process.wait()

    if res == 0:
        print("Hadoop Environment has been setup successfully!")
        print("Starting hadoop server.")
        os.mkdir("submissions")
        app.run(host=HOST, port=PORT)
    else:
        print("Hadoop Environment could not be setup.")
        print(f"Error Log :\n{hadoop_initializer_process.stderr}")
        exit(1)