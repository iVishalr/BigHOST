from crypt import methods
import shutil
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

os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()

app = Flask(__name__)

HOST = "0.0.0.0"
PORT = 10000

HDFS = "/opt/hadoop/bin/hdfs"
HADOOP = "/opt/hadoop/bin/hadoop"
YARN = "/opt/hadoop/bin/yarn"
HADOOP_LOGS = f'/opt/hadoop/logs/userlogs/'

PATH_TO_STREAMING = "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar"
SUBMISSIONS = "submissions"

JOBHISTORY_URL = "http://localhost:19888/ws/v1/history/mapreduce/jobs"

TASK_OUTPUT_PATH = {"A2T1":"task-1-output", "A2T2":"task-2-output"}

FILEPATH = os.path.join(os.getcwd(), 'output')

os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()

def get_datetime() -> str:
    now = datetime.now()
    timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
    return timestamp

class Logger:
    def __init__(self) -> None:
        self.logs = []
    
    def mark(self, message: str) -> None:
        now = datetime.now()
        timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
        msg = f"[{get_datetime()}]   {message}"
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
    SUBMISSION_ID = request.form["submission_id"]
    
    MAPPER = request.form["mapper"]
    REDUCER = request.form["reducer"] 

    logger.mark(f"Starting Hadoop Job. Team ID : {TEAM_ID} Assignment ID : {ASSIGNMENT_ID} Submission ID : {SUBMISSION_ID}")

    job_result = run_hadoop_job(TEAM_ID, ASSIGNMENT_ID, SUBMISSION_ID, TIMEOUT, MAPPER, REDUCER)

    logger.mark(f"Hadoop Job Completed. Team ID : {TEAM_ID} Assignment ID : {ASSIGNMENT_ID} Submission ID : {SUBMISSION_ID}\n")

    return jsonify(job_result)

@app.route("/kill_job", methods=["GET"])
def kill_hadoop_job():
    """
    Kills the currently running job
    """
    r = requests.get("http://localhost:8088/ws/v1/cluster/apps")
    data = json.loads(r.text)
    print(data)
    data = data["apps"]["app"]
    
    current_job = data[0]
    current_job_state = current_job["state"]
    application_id = current_job["id"]

    print(f"Killing Application ID : {application_id}")
    if current_job_state == "RUNNING":
        kill_process = subprocess.Popen([f"{YARN} application -kill {application_id}"], shell=True, text=True)
        _ = kill_process.wait()
        res = {"job": application_id, "Message": "Killed"}
        print(f"Job : {application_id} - Killed!")
    else:
        res = {"job": application_id, "Message": "Job not running. Hence not killed."}
    return res

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

def check_convergence(w_file_path, w1_file_path, iteration_log_path, convergence_limit, iter):
    """
    Checks convergence of file w to w1
    """
    total_nodes = 0
    converged_nodes = 0

    w_file = open(w_file_path, "r")
    w1_file = open(w1_file_path, "r")
    log_file = open(iteration_log_path, "a+")

    for w, w1 in zip(w_file, w1_file):
        total_nodes += 1
        old_pagerank = float(w.split(',')[1])
        new_pagerank = float(w1.split(',')[1])

        if abs(old_pagerank - new_pagerank) < convergence_limit:
            converged_nodes += 1
    
    if iter == 1:
        log_file.write(
            f"Begining Convergence at {get_datetime()}\n"
        )
    
    log_file.write(
        f"Iteration - {iter} : {converged_nodes}/{total_nodes}\n"
    )
    flag = False
    if converged_nodes == total_nodes:
        log_file.write(
            f"Convergence occured at {get_datetime()}\n"
        )
        flag = True
    
    os.remove(w_file_path)
    root_path = w_file_path.split("/")[:-1]
    root_path = "/".join(root_path)
    os.rename(os.path.join(root_path, "w1"), os.path.join(root_path, "w"))
    return flag

def run_hadoop_job(team_id, assignment_id, submission_id, timeout, mapper: str, reducer: str):
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

    task_path = os.path.join(path, submission_id)
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

    task_path = os.path.join(path, submission_id)

    timestamp = str(time.time())
    job_name = team_id + "_" + assignment_id + "_" + timestamp
    
    job_output = None
    status = None
    
    msg = ""

    if assignment_id == "A2T1":
        logger.mark(f"Spawning Hadoop Process")
        mapred_job = f'''{HADOOP} jar {PATH_TO_STREAMING} -D mapreduce.map.maxattempts=1 -D mapreduce.reduce.maxattempts=1 -D mapreduce.job.name="{job_name}" -D mapreduce.task.timeout={int(timeout*1000)} -mapper "/{os.path.join(task_path,'mapper.py')}" -reducer "'/{os.path.join(task_path,'reducer.py')}' '/{os.path.join(task_path,'w')}'" -input /A2/input/graph.txt -output /{team_id}/{assignment_id}/{TASK_OUTPUT_PATH[assignment_id]}'''
        mapred_process = subprocess.Popen([
            mapred_job
        ], shell=True, text=True, preexec_fn=os.setsid)
        process_exit_code = mapred_process.wait()

        r = requests.get(JOBHISTORY_URL)
        data = json.loads(r.text)
        jobs = data["jobs"]
        flag = True
        if len(jobs) == 0:
            flag = False
        else:
            jobs = jobs["job"]
            current_job = jobs[-1]
        
        if not os.path.exists(os.path.join(FILEPATH, team_id, assignment_id)):
            os.makedirs(os.path.join(FILEPATH, team_id, assignment_id))

        if flag and current_job["state"] == "SUCCEEDED":
            logger.mark(f"Team ID : {team_id} Assignment ID : {assignment_id} Hadoop Job Completed Successfully")
            msg = f"Team ID : {team_id} Assignment ID : {assignment_id} Hadoop Job Completed Successfully!"
            job_output = "Good Job!"
            status = current_job["state"]

            if os.path.exists(os.path.join(FILEPATH, team_id, assignment_id, "part-00000")):
                os.remove(os.path.join(FILEPATH, team_id, assignment_id, "part-00000"))
                # os.remove(os.path.join(FILEPATH, team_id, assignment_id, "_SUCCESS"))
                
            process = subprocess.Popen([f"{HDFS} dfs -get /{team_id}/{assignment_id}/{TASK_OUTPUT_PATH[assignment_id]}/part-00000 {os.path.join(FILEPATH, team_id, assignment_id)}"], shell=True, text=True)
            process_code = process.wait()

        elif flag and current_job["state"] == "FAILED":
            application_id = current_job["id"]
            application_id = application_id.split("_")[1:]
            application_id = ["application"] + application_id
            application_id = "_".join(application_id)

            log_path = os.path.join(HADOOP_LOGS, application_id)
            containers_logs = []
            for folders in os.listdir(log_path):
                containers_logs.append(os.path.join(log_path, folders, "stderr"))

            error_logs = [
                f"Submission ID : {submission_id}",
                f"Team ID : {team_id}",
                f"Assignment ID : {assignment_id}",
                "Note : If you do not see any error with respect to your code and you only see : \n\nlog4j:WARN\n\nThen that means your code had infinite loop and submission was killed.\n\nLOGS :- \n\n",
            ]

            for stderr_logs in containers_logs:
                with open(stderr_logs, "r") as f:
                    title = stderr_logs.split("/")[-2:]
                    title = "/".join(title)
                    error_logs.append(title+f"\n{'-' * len(title)}\n\n")
                    error_logs.append(f.read()+"\n")
            
            error_logs = "\n".join(error_logs)
            
            with open(os.path.join(FILEPATH, team_id, assignment_id, "error.txt"), "w+") as f:
                f.write(error_logs)

            logger.mark(f"Team ID : {team_id} Assignment ID : {assignment_id} Hadoop Job Failed")
            msg = f"Team ID : {team_id} Assignment ID : {assignment_id} Hadoop Job Failed."
            status = current_job["state"]
            job_output = "Failed! Your submission might have thrown an error or has exceeded time limits. Logs have been mailed to you."
        
    elif assignment_id == "A2T2":
        it = 1
        CONVERGED = False

        convergence_limit = 0.05

        # copy the files from A2 directory to current directory
        copy_process = subprocess.Popen([f"cp -r /A2/w /{os.path.join(task_path)}"], shell=True, text=True)
        _ = copy_process.wait()

        print("copied files!")

        while not CONVERGED:
            logger.mark(f"Spawning Hadoop Process It - {it}")

            _ = delete_hdfs_directories(f"/{team_id}/{assignment_id}/{TASK_OUTPUT_PATH[assignment_id]}")

            mapred_job = f'''{HADOOP} jar {PATH_TO_STREAMING} -D mapreduce.map.maxattempts=1 -D mapreduce.reduce.maxattempts=1 -D mapreduce.job.name="{job_name}" -D mapreduce.task.timeout={int(timeout*1000)} -mapper "'/{os.path.join(task_path,'mapper.py')}' '/{os.path.join(task_path,'w')}' '/A2/page_embeddings.json'" -reducer "/{os.path.join(task_path,'reducer.py')}" -input /A2/input/adjacency_list.txt -output /{team_id}/{assignment_id}/{TASK_OUTPUT_PATH[assignment_id]}'''
            mapred_process = subprocess.Popen([
                mapred_job
            ], shell=True, text=True, preexec_fn=os.setsid)
            process_exit_code = mapred_process.wait()
        
            r = requests.get(JOBHISTORY_URL)
            data = json.loads(r.text)
            jobs = data["jobs"]
            flag = True
            if len(jobs) == 0:
                flag = False
            else:
                jobs = jobs["job"]
                current_job = jobs[-1]
            
            if flag and current_job["state"] == "SUCCEEDED":

                if not os.path.exists(os.path.join(FILEPATH, team_id, assignment_id)):
                    os.makedirs(os.path.join(FILEPATH, team_id, assignment_id))
                    # permission_process = subprocess.Popen([f"chmod -R 777 {os.path.join(FILEPATH, team_id, assignment_id)}"], shell=True, text=True)
                    # _ = permission_process.wait()
                
                if os.path.exists(os.path.join(FILEPATH, team_id, assignment_id, "part-00000")):
                    os.remove(os.path.join(FILEPATH, team_id, assignment_id, "part-00000"))
                    # os.remove(os.path.join(FILEPATH, team_id, assignment_id, "_SUCCESS"))
                
                process = subprocess.Popen([f"{HDFS} dfs -get /{team_id}/{assignment_id}/{TASK_OUTPUT_PATH[assignment_id]}/part-00000 {os.path.join(FILEPATH, team_id, assignment_id)}"], shell=True, text=True)
                process_code = process.wait()

                process = subprocess.Popen([f"touch /{os.path.join(task_path,'w1')}"], shell=True, text=True)
                process_code = process.wait()

                process = subprocess.Popen([f"cp -r {os.path.join(FILEPATH, team_id, assignment_id, 'part-00000')} /{os.path.join(task_path,'w1')}"], shell=True, text=True)
                process_code = process.wait()

                CONVERGED = check_convergence(
                    w_file_path = os.path.join(task_path,'w'),
                    w1_file_path = os.path.join(task_path,'w1'),
                    iteration_log_path = os.path.join(FILEPATH, team_id, assignment_id, 'log.txt'),
                    convergence_limit = convergence_limit,
                    iter = it
                ) # this renames W1 to W and deletes W1
                status = current_job["state"]
                job_output = "Good Job!"
            
            elif flag and current_job["state"] == "FAILED":
                application_id = current_job["id"]
                application_id = application_id.split("_")[1:]
                application_id = ["application"] + application_id
                application_id = "_".join(application_id)

                log_path = os.path.join(HADOOP_LOGS, application_id)
                containers_logs = []
                for folders in os.listdir(log_path):
                    containers_logs.append(os.path.join(log_path, folders, "stderr"))

                error_logs = [
                    f"Submission ID : {submission_id}",
                    f"Team ID : {team_id}",
                    f"Assignment ID : {assignment_id}"
                    "Note : If you do not see any error with respect to your code and you only see : \n\nlog4j:WARN\n\nThen that means your code had infinite loop and submission was killed.\n\nLOGS :- \n\n"
                ]

                for stderr_logs in containers_logs:
                    with open(stderr_logs, "r") as f:
                        title = stderr_logs.split("/")[-2:]
                        title = "/".join(title)
                        error_logs.append(title+f"\n{'-' * len(title)}\n\n")
                        error_logs.append(f.read()+"\n")
                
                error_logs = "\n".join(error_logs)
                
                with open(os.path.join(FILEPATH, team_id, assignment_id, "error.txt"), "w+") as f:
                    f.write(error_logs)

                logger.mark(f"Team ID : {team_id} Assignment ID : {assignment_id} Hadoop Job Failed")
                msg = f"Team ID : {team_id} Assignment ID : {assignment_id} Hadoop Job Failed."
                status = current_job["state"]
                job_output = "Failed! Your submission might have thrown an error or has exceeded time limits. Logs have been mailed to you."
                break # break out of iterative hadoop job
            elif not flag:
                break

            it += 1

    res = cleanup(team_id, assignment_id, submission_id)

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
            os.killpg(os.getpgid(proc.pid), signal.SIGINT)
            break
            # raise RuntimeError("Process Timed Out")
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

    task_path = TASK_OUTPUT_PATH[assign_id]

    _ = delete_hdfs_directories(f"/{team_id}/{assign_id}/{task_path}")
    _ = delete_hdfs_directories(f"/{team_id}/{assign_id}")
    _ = delete_hdfs_directories(f"/{team_id}")

    return 0

def restart_hadoop_environment():
    
    hadoop_restart_process = subprocess.Popen([
        "$HADOOP_HOME/sbin/stop-all.sh"
    ], shell=True, text=True)
    process_code = hadoop_restart_process.wait()

    hadoop_restart_process = subprocess.Popen([
        "$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver"
    ], shell=True, text=True)
    process_code = hadoop_restart_process.wait()

    hadoop_initializer_process = subprocess.Popen(["bash /restart-hadoop.sh"], shell=True, text=True)
    process_code = hadoop_initializer_process.wait()

    jps_process = subprocess.Popen(["jps"], shell=True, text=True, stdout=subprocess.PIPE)
    process_code = jps_process.wait()

    if process_code != 0:
        error_logs += "jps Process : \n" + jps_process.stderr + "\n\n"
        return process_code, error_logs

    process_stdout = jps_process.communicate()[0]
    print(process_stdout)
    res = process_stdout.strip()
    res = len(res.split("\n"))

def initialize_environment(add_dataset=True) -> Tuple:

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

    if add_dataset:
        os.mkdir(SUBMISSIONS)

        # following stuff is temporary. Only for testing purposes.
        _ = create_hdfs_directory("/A2")
        _ = create_hdfs_directory("/A2/input")

        p = subprocess.Popen([f"{HDFS} dfs -put /A2/graph.txt /A2/input"], shell=True, text=True)
        _ = p.wait()

        p = subprocess.Popen([f"{HDFS} dfs -put /A2/adjacency_list.txt /A2/input"], shell=True, text=True)
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
        # hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar -D mapreduce.map.maxattempts=1 -D mapreduce.reduce.maxattempts=1 -D mapreduce.job.name="bev" -D mapreduce.task.timeout=20000 -mapper "/submissions/BD_019_536_571_001/task1/mapper.py" -reducer "'/submissions/BD_019_536_571_001/task1/reducer.py' '/submissions/BD_019_536_571_001/task1/v'" -input "/A1/input/dataset_1percent.txt" -output "BD_019_536_571_001/output5"
