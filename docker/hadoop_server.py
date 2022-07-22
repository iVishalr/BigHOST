import requests
import json
import os
import subprocess
import time
import stat
import os
import signal

from datetime import datetime
from typing import List, Tuple
from flask import Flask, jsonify, request

app = Flask(__name__)

HOST = "0.0.0.0"
PORT = 10000

HDFS = "/opt/hadoop/bin/hdfs"
HADOOP = "/opt/hadoop/bin/hadoop"

PATH_TO_STREAMING = "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar"
SUBMISSIONS = "submissions"

JOBHISTORY_URL = "http://localhost:19888/ws/v1/history/mapreduce/jobs"

TASK_OUTPUT_PATH = {"task1":"task-1-output", "task2":"task-2-output"}

class Logger:
    def __init__(self) -> None:
        self.logs = []
    
    def mark(self, message: str) -> None:
        now = datetime.now()
        timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
        msg = f"[{timestamp}]   {message}"
        self.logs.append(msg)

    def send_logs():
        pass
    
    def display(self) -> None:
        print("\n".join(self.logs))

    def get_logs(self, as_str=False):
        if not as_str:
            return self.logs
        else:
            return "\n".join(self.logs)

logger = Logger()

@app.route("/", methods=["GET"])
def init():
    return jsonify({"response": "Server is Running on "})

@app.route("/run_job", methods=["POST"])
def run_job():
    
    logger.mark("Received Hadoop Job Request.")

    TEAM_ID = request.form["team_id"]
    ASSIGNMENT_ID = request.form["assignment_id"]
    TIMEOUT = float(request.form["timeout"])
    TASK = request.form["task"]
    
    MAPPER = request.form["mapper"]
    REDUCER = request.form["reducer"] 

    logger.mark(f"Starting Hadoop Job. Team ID: {TEAM_ID} Assignment ID: {ASSIGNMENT_ID} Task: {TASK}")

    job_result = run_hadoop_job(TEAM_ID, ASSIGNMENT_ID, TASK, TIMEOUT, MAPPER, REDUCER)

    logger.mark(f"Hadoop Job Completed. Team ID: {TEAM_ID} Assignment ID: {ASSIGNMENT_ID} Task: {TASK}\n")

    return jsonify(job_result)

def create_hdfs_directory(dirname: str) -> int:
    process = subprocess.Popen([f"{HDFS} dfs -mkdir {dirname}"], shell=True, text=True)
    res = process.wait()
    logger.mark(f"Created Directory - hdfs:{dirname}")
    return res

def delete_hdfs_directories(dirname: str) -> int:
    process = subprocess.Popen([f"{HDFS} dfs -rm -r {dirname}"], shell=True, text=True)
    res = process.wait()
    logger.mark(f"Deleted Directory - hdfs:{dirname}")
    return res

def run_hadoop_job(team_id, assignment_id, task, timeout, mapper: str, reducer: str):
    """
    Arguments
    ---------

    team_id : Team ID of the submission
    assignment_id : Assignment ID of the submission
    timeout : Timeout in seconds 
    mapper : List of mapper codes. Each element represents mapper for ith task (Update needed)
    reducer : List of reducer codes. Each element represents reducer for ith task (Update needed)
    """
    
    path = os.path.join(SUBMISSIONS, team_id)
    if not os.path.exists(path):
        os.mkdir(path)
    
    logger.mark(f"Created Directory - {path}")

    task_path = os.path.join(path, task)
    if not os.path.exists(task_path):
        os.mkdir(task_path)

    logger.mark(f"Created Directory - {task_path}")

    with open(os.path.join(task_path, "mapper.py"), "w+") as f:
        f.write(mapper)
    
    logger.mark(f"Created mapper.py at {os.path.join(task_path, 'mapper.py')}")

    with open(os.path.join(task_path, "reducer.py"), "w+") as f:
        f.write(reducer)

    logger.mark(f"Created reducer.py at {os.path.join(task_path, 'reducer.py')}")

    st = os.stat(os.path.join(task_path,"mapper.py"))
    os.chmod(os.path.join(task_path,"mapper.py"), st.st_mode | stat.S_IEXEC)

    st = os.stat(os.path.join(task_path,"reducer.py"))
    os.chmod(os.path.join(task_path,"reducer.py"), st.st_mode | stat.S_IEXEC)
    
    res = create_hdfs_directory(f"/{team_id}")
    if res != 0:
        print(f"Failed to create HDFS Directory : hdfs:/{team_id}")

    directory = f"/{team_id}/{assignment_id}"
    res = create_hdfs_directory(directory)
    if res != 0:
        print(f"Failed to create HDFS Directory : hdfs:{directory}")

    # directory = f"/{team_id}/{assignment_id}/{TASK_OUTPUT_PATH[task]}"
    # res = create_hdfs_directory(directory)
    # if res != 0:
    #     print(f"Failed to create HDFS Directory : hdfs:{directory}")

    task_path = os.path.join(path, task)

    timestamp = str(time.time())
    job_name = team_id + "_" + assignment_id + "_" + timestamp
    
    mapred_job = f'''{HADOOP} jar {PATH_TO_STREAMING} -D mapreduce.job.name="{job_name}" -mapper "/{os.path.join(task_path,'mapper.py')}" -reducer "'/{os.path.join(task_path,'reducer.py')}' '/{os.path.join(task_path,'v')}'" -input /{assignment_id}/input/dataset_1percent.txt -output /{team_id}/{assignment_id}/{TASK_OUTPUT_PATH[task]}'''
    
    logger.mark(f"Spawning Hadoop Process")

    mapred_process = subprocess.Popen([
        mapred_job
    ], shell=True, text=True, preexec_fn=os.setsid)

    try:
        process_exit_code = job_timer(mapred_process, timeout) # timeout is in seconds
        r = requests.get(JOBHISTORY_URL)
        data = json.loads(r.text)
        jobs = data["jobs"]["job"]
        
        current_job = jobs[-1]
        job_output = None
        status = None
        if current_job["name"] == job_name:
            if current_job["state"] == "SUCCEEDED":
                logger.mark(f"Team ID:{team_id} Assignment ID:{assignment_id} Hadoop Job Completed Successfully")
                msg = f"Team ID:{team_id} Assignment ID:{assignment_id} Hadoop Job Completed Successfully!"
                job_output = "Good Job!"
                status = current_job["state"]
            elif current_job["state"] == "FAILED":
                logger.mark(f"Team ID:{team_id} Assignment ID:{assignment_id} Hadoop Job Failed")
                msg = f"Team ID:{team_id} Assignment ID:{assignment_id} Hadoop Job Failed."
                # job_output = mapred_process.communicate()[0]
                # job_err = mapred_process.communicate()[1]
                status = current_job["state"]
                job_output = "Something is wrong in input files."
            else:
                job_output = "God knows what you are doing!"
                status = "KILLED"

    except RuntimeError:
        logger.mark(f"Team ID:{team_id} Assignment ID:{assignment_id} Hadoop Job Exceeded time limits")
        msg = f"Team ID:{team_id} Assignment ID:{assignment_id} Submission has taken more time than the alloted time. Submission has been killed!"
        status = "KILLED"

    res = cleanup(team_id, assignment_id, task)

    logs = logger.get_logs(as_str=True)

    res = {"text": msg, "status_code": 200, "job_output": job_output, "status": status}
    return res

def job_timer(proc: subprocess.Popen, timeout: int) -> int:
    """
    Times a subprocess instance and terminates it if process exceeds timeout. 
    """
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

def cleanup(team_id, assign_id, task) -> int:
    """
    Safely Removes all the elements that were created for and during evaluation of the code.

    Assuming Directory Structure to be :
    
    /submissions
    |_BD_XXX_XXX_XXX/
      |_Task{x}/
        |_mapper.py
        |_reducer.py

    """

    logger.mark("Deleting user files")

    path = os.path.join(SUBMISSIONS, team_id)

    task_path = os.path.join(path, task)
    
    logger.mark(f"Entering {task_path} directory")
    
    for files in os.listdir(task_path):
        filename = os.path.join(task_path, files)
        os.remove(filename)
        logger.mark(f"Removed file {filename}")
    
    logger.mark(f"Leaving {task_path} directory")
    os.rmdir(task_path)
    logger.mark(f"Deleted {task_path} directory")

    logger.mark(f"Leaving {path} directory")
    os.rmdir(path)
    logger.mark(f"Deleted {path} directory")

    logger.mark("Deleting data in HDFS")

    task_path = TASK_OUTPUT_PATH[task]

    _ = delete_hdfs_directories(f"/{team_id}/{assign_id}/{task_path}")
    _ = delete_hdfs_directories(f"/{team_id}/{assign_id}")
    _ = delete_hdfs_directories(f"/{team_id}")

    return 0

def initialize_environment() -> Tuple:

    error_logs = ""

    hadoop_initializer_process = subprocess.Popen(["bash /start-hadoop.sh"], shell=True, text=True)
    process_code = hadoop_initializer_process.wait()

    if process_code != 0:
        error_logs += "Hadoop Initializer Process : \n" + hadoop_initializer_process.stderr + "\n\n"
        return process_code, error_logs
    
    jps_process = subprocess.Popen(["jps"], shell=True, text=True, stdout=subprocess.PIPE)
    process_code = jps_process.wait()

    if process_code != 0:
        error_logs += "jps Process : \n" + jps_process.stderr + "\n\n"
        return process_code, error_logs

    process_stdout = jps_process.communicate()[0]
    print(process_stdout)
    res = process_stdout.strip()
    res = len(res.split("\n"))

    if res < 6:
        error_logs += "jps Process : Not all Hadoop Processes have been started."
        return 1, error_logs

    os.mkdir(SUBMISSIONS)

    # following stuff is temporary. Only for testing purposes.
    _ = create_hdfs_directory("/A1")
    _ = create_hdfs_directory("/A1/input")

    p = subprocess.Popen([f"{HDFS} dfs -put /Assign2/datasets/dataset_1percent.txt /A1/input"], shell=True, text=True)
    _ = p.wait()
    
    return 0, error_logs 

if __name__ == "__main__":
    
    return_code, error_logs = initialize_environment()
    
    if return_code == 0:
        print("Hadoop Environment has been setup successfully!")
        print("Starting hadoop server.")
        app.run(host=HOST, port=PORT)
    else:
        print("Hadoop Environment could not be setup.")
        print(f"Error Log :\n{error_logs}")
        exit(1)
        # hadoop job -list | egrep '^job' | awk '{print $1}' | xargs -n 1 -I {} sh -c "hadoop job -status {} | egrep '^tracking' | awk '{print \$3}'" | xargs -n 1 -I{} sh -c "echo -n {} | sed 's/.*jobid=//'; echo -n ' ';curl -s -XGET {} | grep 'Job Name' | sed 's/.* //' | sed 's/<br>//'"