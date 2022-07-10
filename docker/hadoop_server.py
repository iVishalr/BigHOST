from typing import List, Tuple
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

HDFS = "/opt/hadoop/bin/hdfs"
HADOOP = "/opt/hadoop/bin/hadoop"

PATH_TO_STREAMING = "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar"
SUBMISSIONS = "submissions"

TASK_OUTPUT_PATH = {"task1":"task-1-output", "task2":"task-2-output"}

@app.route("/", methods=["GET"])
def init():
    return jsonify({"response": "Server is Running on "})

@app.route("/run_job", methods=["POST"])
def run_job():
    
    TEAM_ID = request.form["team_id"]
    ASSIGNMENT_ID = request.form["assignment_id"]
    TIMEOUT = float(request.form["timeout"])
    TASK = request.form["task"]
    
    MAPPER = request.form["mapper"]
    REDUCER = request.form["reducer"]    

    job_result = run_hadoop_job(TEAM_ID, ASSIGNMENT_ID, TASK, TIMEOUT, MAPPER, REDUCER)
    return jsonify(job_result)

def create_hdfs_directory(dirname: str) -> int:
    process = subprocess.Popen([f"{HDFS} dfs -mkdir {dirname}"], shell=True, text=True)
    res = process.wait()
    return res

def delete_hdfs_directories(dirname: str) -> int:
    process = subprocess.Popen([f"{HDFS} dfs -rm -r {dirname}"], shell=True, text=True)
    return process.wait()


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

    task_path = os.path.join(path, task)
    if not os.path.exists(task_path):
        os.mkdir(task_path)

    with open(os.path.join(task_path, "mapper.py"), "w+") as f:
        f.write(mapper)
    
    with open(os.path.join(task_path, "reducer.py"), "w+") as f:
        f.write(reducer)

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
    
    mapred_job = f'''{HADOOP} jar {PATH_TO_STREAMING} -mapper "/{os.path.join(task_path,'mapper.py')}" -reducer "'/{os.path.join(task_path,'reducer.py')}' '/{os.path.join(task_path,'v')}'" -input /{assignment_id}/input/dataset_1percent.txt -output /{team_id}/{assignment_id}/{TASK_OUTPUT_PATH[task]}'''
    mapred_process = subprocess.Popen([
        mapred_job
    ], shell=True, text=True, preexec_fn=os.setsid)

    try:
        process_exit_code = job_timer(mapred_process, timeout) # timeout is in seconds
        if process_exit_code == 0:
            msg = f"{task} Completed Successfully!"
        else:
            msg = f"{task} Failed due to : \n{mapred_process.stderr}"
    except RuntimeError:
        process_exit_code = -1
        msg = f"{task} Submission has taken more time than the alloted time. Submission has been killed!"

    res, logs = cleanup(team_id, assignment_id, task)
    res = {"text": msg, "status_code": 200, "logs": logs}
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

def cleanup(team_id, assign_id, task) -> Tuple:
    """
    Safely Removes all the elements that were created for and during evaluation of the code.

    Assuming Directory Structure to be :
    
    /submissions
    |_BD_XXX_XXX_XXX/
      |_Task{x}/
        |_mapper.py
        |_reducer.py

    """

    logs = "Deleting user files\n"

    path = os.path.join(SUBMISSIONS, team_id)

    task_path = os.path.join(path, task)
    logs += f"Entering {task_path} directory\n"
    for files in os.listdir(task_path):
        filename = os.path.join(task_path, files)
        os.remove(filename)
        logs += f"Removed file {filename}\n"
    logs += f"Leaving {task_path} directory\n"
    os.rmdir(task_path)
    logs += f"Deleted {task_path} directory\n"

    logs += f"Leaving {path} directory\n"
    os.rmdir(path)
    logs += f"Deleted {path} directory\n"

    logs += "Deleting data in HDFS\n"

    task_path = TASK_OUTPUT_PATH[task]

    _ = delete_hdfs_directories(f"/{team_id}/{assign_id}/{task_path}")
    logs += f"Removed file /{team_id}/{assign_id}/{task_path}/\n"

    _ = delete_hdfs_directories(f"/{team_id}/{assign_id}")
    logs += f"Removed file /{team_id}/{assign_id}/\n"

    _ = delete_hdfs_directories(f"/{team_id}")
    logs += f"Removed file /{team_id}/\n"

    return 0, logs

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