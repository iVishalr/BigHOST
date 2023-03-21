import os
import sys
import ast
import json
import time
import pickle
import requests
import subprocess

from common.db import DataBase
from common import mail_queue
from datetime import datetime
from dotenv import load_dotenv
from flask_backend import queue
from flask_cors import cross_origin
from flask import Flask, request, jsonify
from signal import signal, SIGPIPE, SIG_DFL
from flask_backend.parser import SanityCheckerASTVisitor

signal(SIGPIPE, SIG_DFL) 

def createApp():

    load_dotenv(os.path.join(os.getcwd(), '.env'))
    os.environ['TZ'] = 'Asia/Kolkata'
    time.tzset()

    app = Flask(__name__)  
    db = DataBase()

    class Tee(object):
        def __init__(self, *files):
            self.files = files
        def write(self, obj):
            for f in self.files:
                f.write(obj)
        def flush(self):
            pass

    f = open(f'./sanity_checker_logs.txt', 'a+')
    backup = sys.stdout
    sys.stdout = Tee(sys.stdout, f)

    def get_datetime() -> str:
        now = datetime.now()
        timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
        return timestamp
    
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
            mail_data = {}
            mail_data['teamId'] = teamId
            mail_data['submissionId'] = submissionId
            mail_data['submissionStatus'] = message
            mail_data['attachment'] = ""
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
        
        prefetch_factor = int(request.args.get("prefetch_factor"))

        if prefetch_factor is None: prefetch_factor = 1

        if len(queue) == 0:
            res = {"msg": "Submission Queue is currently empty.", "len": len(queue), "num_submissions": 0}
            return jsonify(res)

        data = []
        i = 0
        while i < prefetch_factor:
            queue_data = queue.dequeue()

            if queue_data is None:
                break
            
            queue_name, serialized_job = queue_data
            job = pickle.loads(serialized_job)
            data.append(job)
            i += 1  

        length = len(data)
        data = json.dumps(data)

        request_url = f"http://{client_addr}:10001/submit-job"

        r = requests.post(request_url, data=data)

        res = {"msg": f"Dequeued {length} submissions from queue.", "num_submissions": length, "len": len(queue)}
        # res = {"msg": "dequeued from submission queue", "len": len(queue), "server_response": res}
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