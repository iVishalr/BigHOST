import os
import sys
import ast
import json
import time
import pickle
import requests
import subprocess

from common import mail_queue
from common.db import DataBase
from common.utils import Tee, get_datetime, Logger

from dotenv import load_dotenv
from flask_backend import queue, executor_table
from flask_cors import cross_origin
from flask import Flask, request, jsonify
from signal import signal, SIGPIPE, SIG_DFL
from flask_backend.parser import SanityCheckerASTVisitor

signal(SIGPIPE, SIG_DFL) 

def createApp():

    load_dotenv(os.path.join(os.getcwd(), '.env'))
    os.environ['TZ'] = 'Asia/Kolkata'
    time.tzset()

    config_path = os.path.join(os.getcwd(),"config", "evaluator.json")

    configs = None
    with open(config_path, "r") as f:
        configs = json.loads(f.read())

    app = Flask(__name__)  
    db = DataBase()

    configs = configs["backend"]
    backend_name = configs["name"]
    backend_logdir = configs["log_dir"]
    backend_config_dir = configs["config_dir"]
    
    log_timestamp = "-".join(('-'.join(get_datetime().split(' '))).split('/'))
    backend_config_dir = os.path.join(backend_config_dir, log_timestamp)
    log_path = os.path.join(backend_logdir, backend_name, log_timestamp)
    executor_log_dir = os.path.join(backend_logdir, "executor", log_timestamp)

    if not os.path.exists(log_path):
        os.makedirs(log_path)
    
    if not os.path.exists(executor_log_dir):
        os.makedirs(executor_log_dir)

    # f = open(os.path.join(log_path, f'sanity_checker_logs.txt'), 'a+')
    # backup = sys.stdout
    # sys.stdout = Tee(sys.stdout, f)

    sys.stdout = Logger(os.path.join(log_path, f'sanity_checker_logs.txt'), 'a+')

    def is_registered(executor_ip) -> bool:
        return executor_ip in executor_table
    
    def delete_files(path):
        for file in os.listdir(path):
            if file.endswith('.py'):
                os.remove(os.path.join(path, file))
        os.rmdir(path)

    def get_timeouts(assignment_id):
        timeout = None
        if "A1" in assignment_id:
            if "T1" in assignment_id:
                timeout = 30
            elif "T2" in assignment_id:
                timeout = 30
        elif "A2" in assignment_id:
            if "T1" in assignment_id:
                timeout = 60
            elif "T2" in assignment_id:
                timeout = 480
        elif "A3" in assignment_id:
            if "T1" in assignment_id:
                timeout = 60
            elif "T2" in assignment_id:
                timeout = 60
        else:
            timeout = 30

        return timeout
        
    def update_submission(marks, message, data, send_mail=False):
        doc = db.try_find("submissions", data['teamId'])
        timestamp = int(str(time.time_ns())[:10])
        teamId = data['teamId']
        assignmentId = data['assignmentId']
        submissionId = str(data['submissionId'])
        if not doc:
            db.insert("submissions", teamId, None, assignmentId, submissionId, marks, message, timestamp)
        else:
            db.update("submissions", teamId, None, assignmentId, submissionId, marks, message, timestamp)
        
        if send_mail:
            mail_data = {
                'teamId': teamId,
                'submissionId': submissionId,
                'submissionStatus': message,
                'attachment': ""
            }
            mail_data = pickle.dumps(mail_data)
            mail_queue.enqueue(mail_data)

    @app.route('/sanity-check', methods=["POST"])
    @cross_origin()
    def sanity_check():
        '''
        Currently assuming the assignment to be a MR Job
        '''
        jobs = json.loads(request.data)
        data = jobs
        
        update_submission(marks=-1, message='Sanity Checking', data=data)

        compile_path = f"{os.path.join(os.getcwd(),'compile-test', str(data['submissionId']))}"

        if not os.path.exists(compile_path):
            os.makedirs(compile_path)

        _ = open(os.path.join(compile_path, "__init__.py"), "w+").close() # required for pylint to work

        if "A3" not in data["assignmentId"]:
            mapper_data = data["mapper"]
            reducer_data = data['reducer']
            mapper_name = f"{data['teamId']}-{data['assignmentId']}-mapper.py"
            reducer_name = f"{data['teamId']}-{data['assignmentId']}-reducer.py"

            if mapper_data.strip().split("\n")[0] != '#!/usr/bin/env python3':
                print(f"[{get_datetime()}] [sanity_checker]\tTeam : {data['teamId']} Assignment ID : {data['assignmentId']} Message : Mapper Shebang Not Present. Check if your file is in LF format.")
                update_submission(marks=-1, message='Mapper Shebang Not Present. Make sure shebang is present as the first line and check if file is in LF format.', data=data, send_mail=True)
                res = {"msg": "Mapper shebang not present", "len": len(queue)}
                return jsonify(res)

            if reducer_data.strip().split("\n")[0] != '#!/usr/bin/env python3':
                print(f"[{get_datetime()}] [sanity_checker]\tTeam : {data['teamId']} Assignment ID : {data['assignmentId']} Message : Reducer Shebang Not Present. Check if your file is in LF format.")
                update_submission(marks=-1, message='Reducer Shebang Not Present. Make sure shebang is present as the first line and check if file is in LF format.', data=data, send_mail=True)
                res = {"msg": "Reducer shebang not present", "len": len(queue)}
                return jsonify(res)

            mapper = open(os.path.join(compile_path, mapper_name), 'w')
            mapper.write(mapper_data)
            mapper.close()

            reducer = open(os.path.join(compile_path, reducer_name), 'w')
            reducer.write(reducer_data)
            reducer.close()

        elif "A3" in data["assignmentId"]:
            if "T1" in data["assignmentId"]:
                
                spark_file_data = data["spark"]
                spark_file_name = f"{data['teamId']}-{data['assignmentId']}-spark.py"

                if "\r\n" in spark_file_data:
                    print(f"[{get_datetime()}] [sanity_checker]\tTeam : {data['teamId']} Assignment ID : {data['assignmentId']} Message : Check if your file is in LF format.")
                    update_submission(marks=-1, message='Check if file is in LF format.', data=data, send_mail=True)
                    res = {"msg": "Check if file is in LF format.", "len": len(queue)}
                    return jsonify(res)
                
                ast_parser = SanityCheckerASTVisitor(["for", "while", "open"])
                parsed_code = ast.parse(spark_file_data)
                ast_parser.visit(parsed_code)
                parser_report = None
                if ast_parser.num_errors != 0:
                    # we have errors in the submitted code
                    parser_report = ast_parser.report()
                    print(f"[{get_datetime()}] [sanity_checker]\tTeam : {data['teamId']} Assignment ID : {data['assignmentId']} Message : Failed Sanity Check. It appears that you have used an illegal python construct. Error Log : \n{parser_report}")
                    update_submission(marks=-1, message="Failed Sanity Check. It appears that you have used illegal python construct(s). Error Log : \n"+parser_report, data=data, send_mail=True)
                    res = {"msg": "Error", "len": len(queue)}
                    return jsonify(res)
                
                spark_file = open(os.path.join(compile_path, spark_file_name), "w")
                spark_file.write(spark_file_data)
                spark_file.close()
            
            elif "T2" in data["assignmentId"]:
                producer_data = data["producer"]
                consumer_data = data["consumer"]
                producer_name = f"{data['teamId']}-{data['assignmentId']}-producer.py"
                consumer_name = f"{data['teamId']}-{data['assignmentId']}-consumer.py"
                
                if "\r\n" in producer_data:
                    print(f"[{get_datetime()}] [sanity_checker]\tTeam : {data['teamId']} Assignment ID : {data['assignmentId']} Message : Check if your Producer file is in LF format.")
                    update_submission(marks=-1, message='Check if Producer file is in LF format.', data=data, send_mail=True)
                    res = {"msg": "Check if Producer file is in LF format.", "len": len(queue)}
                    return jsonify(res)

                if "\r\n" in consumer_data:
                    print(f"[{get_datetime()}] [sanity_checker]\tTeam : {data['teamId']} Assignment ID : {data['assignmentId']} Message : Check if your Consumer file is in LF format.")
                    update_submission(marks=-1, message='Check if Consumer file is in LF format.', data=data, send_mail=True)
                    res = {"msg": "Check if Consumer file is in LF format.", "len": len(queue)}
                    return jsonify(res)

                ast_parser = SanityCheckerASTVisitor(["open"])
                producer_code = ast.parse(producer_data)
                ast_parser.visit(producer_code)
                parser_report = None
                
                if ast_parser.num_errors != 0:
                    # we have errors in the submitted code
                    parser_report = ast_parser.report()
                    print(f"[{get_datetime()}] [sanity_checker]\tTeam : {data['teamId']} Assignment ID : {data['assignmentId']} Message : Failed Sanity Check. It appears that you have used an illegal python construct(s) in Producer. Error Log : \n{parser_report}")
                    update_submission(marks=-1, message="Failed Sanity Check. It appears that you have used illegal python construct(s) in Producer. Error Log : \n"+parser_report, data=data, send_mail=True)
                    res = {"msg": "Error", "len": len(queue)}
                    return jsonify(res)  

                ast_parser.reset()

                consumer_code = ast.parse(consumer_data) 
                ast_parser.visit(consumer_code)
                parser_report = None
                if ast_parser.num_errors != 0:
                    # we have errors in the submitted code
                    parser_report = ast_parser.report()
                    print(f"[{get_datetime()}] [sanity_checker]\tTeam : {data['teamId']} Assignment ID : {data['assignmentId']} Message : Failed Sanity Check. It appears that you have used an illegal python construct(s) in Consumer. Error Log : \n{parser_report}")
                    update_submission(marks=-1, message="Failed Sanity Check. It appears that you have used illegal python construct(s) in Consumer. Error Log : \n"+parser_report, data=data, send_mail=True)
                    res = {"msg": "Error", "len": len(queue)}
                    return jsonify(res)

                producer = open(os.path.join(compile_path, producer_name), 'w')
                producer.write(producer_data)
                producer.close()

                consumer = open(os.path.join(compile_path, consumer_name), 'w')
                consumer.write(consumer_data)
                consumer.close()

        process = subprocess.Popen([f'pylint --disable=R,C,W,import-error {compile_path}/'], shell=True, stdout=subprocess.PIPE, text=True)
        exit_code = process.wait()

        output = process.communicate()[0]
        delete_files(compile_path)

        if exit_code != 0:
            print(f"[{get_datetime()}] [sanity_checker]\tTeam : {data['teamId']} Assignment ID : {data['assignmentId']} Message : Failed Sanity Check. Check your files for syntax errors or illegal module imports. Error Log : {output}")
            update_submission(marks=-1, message="Failed Sanity Check. Check your files for syntax errors or illegal module imports. Error Log : "+output, data=data, send_mail=True)
            res = {"msg": "Error", "len": len(queue)}
            return jsonify(res)

        elif exit_code == 0:
            print(f"[{get_datetime()}] [sanity_checker]\tTeam : {data['teamId']} Assignment ID : {data['assignmentId']} Message : Passed Sanity Check.")
            update_submission(marks=-1, message='Sanity Check Passed', data=data)

        data["timeout"] = get_timeouts(assignment_id=data['assignmentId'])

        update_submission(marks=-1, message='Queued for Execution', data=data)

        data = pickle.dumps(data)
        queue.enqueue(data)
        
        res = {"msg": "Queued", "len": len(queue)}
        return jsonify(res)

    @app.route('/get-jobs', methods=['GET'])
    @cross_origin()
    def get_jobs():
        client_addr = None
        if request.environ.get('HTTP_X_FORWARDED_FOR') is None:
            client_addr = request.environ['REMOTE_ADDR']
        else:
            client_addr = request.environ['HTTP_X_FORWARDED_FOR'] # if behind a proxy

        if not is_registered(str(client_addr)):
            res = {"status": 401, "msg": f"This executor has not been registered with backend server. Please register before making request"}
            return jsonify(res)
        
        prefetch_factor = int(request.args.get("prefetch_factor"))

        if prefetch_factor is None: prefetch_factor = 1

        if len(queue) == 0:
            res = {"msg": "Submission Queue is currently empty.", "len": len(queue), "num_submissions": 0, "status": 200}
            return jsonify(res)

        data = []
        i = 0
        while i < prefetch_factor:
            queue_data = queue.dequeue()

            if queue_data is None:
                break
            
            _, serialized_job = queue_data
            job = pickle.loads(serialized_job)
            data.append(job)
            i += 1  

        length = len(data)
        data = json.dumps(data)

        res = {
            "msg": f"Dequeued {length} submissions from queue.", 
            "num_submissions": length, 
            "len": len(queue), 
            "status": 200,
            "jobs": data
        }
        return jsonify(res)

    @app.route("/register_executor", methods=["POST"])
    def register_executor():
        executor_addr = None
        if request.environ.get('HTTP_X_FORWARDED_FOR') is None:
            executor_addr = request.environ['REMOTE_ADDR']
        else:
            executor_addr = request.environ['HTTP_X_FORWARDED_FOR'] # if behind a proxy

        executor_addr = str(executor_addr)

        data = json.loads(request.data)

        executor_metadata = {
            'executor_name': data["executor_name"],
            'executor_uuid': data["executor_uuid"],
            'executor_log_dir': data["executor_log_dir"],
            'num_backends': data["num_backends"],
            'num_workers': data["num_workers"],
            'num_threads': data["num_threads"],
            'num_prefetch_threads': data["num_prefetch_threads"],
            'prefetch_factor': data["prefetch_factor"],
            'threshold': data["threshold"],
            'timeout': data["timeout"],
            'ipaddr': executor_addr,
            'cpu_limit': data["cpu_limit"],
            'mem_limit': data["mem_limit"],
            'sys_info': data["sys_info"]
        }
        executor_table[executor_addr] = executor_metadata

        if not os.path.exists(backend_config_dir):
            os.makedirs(backend_config_dir)

        with open(os.path.join(backend_config_dir, f'{data["executor_name"]}-{data["executor_uuid"]}.json'), 'w', encoding='utf-8') as _f:
            json.dump(executor_metadata, _f, ensure_ascii=False, indent=4)
        
        msg = {"status": 200, "message": "Registered!"}
        return jsonify(msg)

    @app.route("/executors", methods=["GET"])
    def get_executors():
        files = list(os.listdir(backend_config_dir))
        executors = {}
        for filename in files:
            if ".json" not in filename:
                continue
            path = os.path.join(backend_config_dir, filename)
            e = open(path,"r")
            executor = json.loads(e.read())
            e.close()
            executors[executor['ipaddr']] = executor
        
        return jsonify(executors)

    @app.route("/logs", methods=["GET"])
    def get_logs():
        logs = {}
        for executor_addr in executor_table:
            executor = executor_table[executor_addr]
            executor_name = executor["executor_name"]
            executor_uuid = executor["executor_uuid"]

            executor_log_path = os.path.join(executor_log_dir, executor_name, executor_uuid)

            if not os.path.exists(executor_log_path):
                logs[executor_addr] = None
                continue

            logfile = {}
            for logname in os.listdir(executor_log_path):
                f = open(os.path.join(executor_log_path, logname), "r")
                logfile[logname] = f.read()
                f.close()
            logs[executor_addr] = logfile
        
        res = {"logs": logs}
        return jsonify(res)

    @app.route("/executor-log", methods=["POST"])
    def executor_log():
        if request.environ.get('HTTP_X_FORWARDED_FOR') is None:
            executor_addr = request.environ['REMOTE_ADDR']
        else:
            executor_addr = request.environ['HTTP_X_FORWARDED_FOR'] # if behind a proxy

        executor_addr = str(executor_addr)

        data = json.loads(request.data)
        executor_name = data["executor_name"]
        executor_uuid = data["executor_uuid"]

        if executor_addr not in executor_table:
            res = {"status": 401, "message": "Executor not registered with backend server."}
            return jsonify(res)
        
        executor_log_path = os.path.join(executor_log_dir, executor_name, executor_uuid)

        if not os.path.exists(executor_log_path):
            os.makedirs(executor_log_path)
        
        logs = json.loads(data["logs"])
        for logname in logs:
            f = open(os.path.join(executor_log_path, logname), "w")
            f.write(logs[logname])
            f.close()

        logs = json.loads(data["syslogs"])
        for logname in logs:
            f = open(os.path.join(executor_log_path, logname), "w")
            f.write(logs[logname])
            f.close()
        
        res = {"status": 200, "message": "Received!"}
        return jsonify(res)

    @app.route("/queue-length", methods=["GET"])
    def queue_length():
        msg = {"length": len(queue)}
        return jsonify(msg)

    @app.route("/empty-queue", methods=["GET"])
    def empty_queue():
        queue.empty_queue()
        res = {"msg": "Queue Emptied"}
        return jsonify(res)

    return app

if __name__ == "__main__":
    createApp()
    # app.run(port=9000, debug=False)