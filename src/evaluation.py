import os
# DynamicClassAttributes are those attributes of an object that are defined at runtime
from types import DynamicClassAttribute
import pytz
import base64
import datetime
from rq.timeouts import JobTimeoutException
from pymongo import MongoClient

mongodb_client = MongoClient("mongodb://root:root@mongodb:27017")
system_username = os.environ.get('USER')
IST = pytz.timezone("Asia/Kolkata")


def executeCommand(command):
    '''
    Function used to execute commands on the command line
    '''
    output = os.popen(command).read().strip()
    return output


def cleanup(hdfs_output_path):
    '''
    Function that cleans up HDFS
    '''
    executeCommand(f"hdfs dfs -rm -r -f {hdfs_output_path}")


def runHadoopJob(mapper_path, reducer_path, hdfs_input_path, hdfs_output_path, log_path, mapper_args, reducer_args):
    '''
    Function used to run hadoop job
    '''
    command = f'''hadoop jar /home/{system_username}/hadoop-3.2.2/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar \
    -mapper "{mapper_path} {mapper_args}" \
    -reducer "{reducer_path} {reducer_args}" \
    -input {hdfs_input_path} \
    -output {hdfs_output_path} 2> {log_path}'''
    # print(command)
    executeCommand(command)


def runSparkJob(spark_script_path, spark_output_path, log_path, args=""):
    '''
    Function used to run spark job
    '''
    command = f'''$SPARK_HOME/bin/spark-submit {spark_script_path} {args} 2> {log_path} > {spark_output_path}'''
    executeCommand(command)


def compareOutput(output_path, solution_path):
    '''
    Compares outputs of a submission and the solution
    '''
    try:
        check_output = int(executeCommand(
            # -E ignores tab expansion, -w ignores white space, -b ignores changes in the white space, -B ignores blank lines
            f"diff -E -w -Z -b -B {output_path} {solution_path} && echo 1 || echo 0"))
    except ValueError:
        check_output = 0
    return check_output


def compareOutputAssignment2(output_path, solution_path):
    '''
    Compares the outputs of a submission and the solution for Assignment 2 (Page Rank)
    '''
    flag = True
    with open(output_path) as file1, open(solution_path) as file2:
        for line1, line2 in zip(file1, file2):
            # Extract page and associated ranks
            page1, rank1 = line1.strip().split(',')
            page2, rank2 = line2.strip().split(',')
            rank1 = float(rank1)
            rank2 = float(rank2)
            if page1 == page2:
                if not abs(rank1 - rank2) <= 0.05:
                    flag = False
                    break
            else:
                flag = False
                break
    return flag


def checkCompilationHadoop(hdfs_output_path):
    '''
    Used to check compilation in Hadoop
    '''
    check_compilation = int(executeCommand(
        f'if $(hdfs dfs -test -e {hdfs_output_path}/part-00000) ; then echo 1;else echo 0; fi'))
    # print("Compilation check:", check_compilation)
    return check_compilation


def checkCompilationSpark(spark_output_path):
    '''
    Used to check compilation in Spark
    '''
    with open(spark_output_path) as spark_output_file:
        return not "Traceback" in spark_output_file.read()


def writeToOutputPath(status_path, check_output, ASSIGNMENT_ID, TESTCASE_ID):
    '''
    Writes the status of an Assignment and Testcase to a file
    '''
    with open(status_path, 'w') as status_file:
        if check_output:
            message = f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Correct Answer"
        else:
            message = f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Wrong Answer"
        status_file.write(message)
    return message


def checkConvergence(v_file_path, v1_file_path, iteration_log_path, convergence_limit, iter):
    '''
    Checks convergence of file v to v1
    '''
    total_nodes = 0
    converged_nodes = 0
    with open(v_file_path) as v_file, open(v1_file_path) as v1_file, open(iteration_log_path, 'a') as iteration_log_file:
        # Counts the number of total nodes and the number of converged nodes
        for v_line, v1_line in zip(v_file, v1_file):
            total_nodes += 1
            old_pagerank = float(v_line.split(',')[1])
            new_pagerank = float(v1_line.split(',')[1])
            if abs(old_pagerank - new_pagerank) < convergence_limit:
                converged_nodes += 1

        current_time = str(datetime.datetime.now(IST))
        if iter == 1:
            iteration_log_file.write(
                f"BEGINNING CONVERGENCE AT {current_time}\n")
        # Writes percentage of nodes that have converged at a particular iteration
        iteration_log_file.write(
            f"Iteration: {iter}: {converged_nodes}/{total_nodes}\n")
        # If all the nodes have converged
        if converged_nodes == total_nodes:
            iteration_log_file.write(f"CONVERGENCE OCCURRED AT {current_time}\n")
            return True
        else:
            # Removes original v_file
            executeCommand(f"rm {v_file_path}")
            # Renames v1 to v
            executeCommand(f"mv {v1_file_path} {v_file_path}")
            return False


def iterateAssignment2(data, testcase_id, v_file_path, v1_file_path, iteration_log_path, convergence_limit, solution_path, mapper_path, reducer_path, hdfs_input_path, mapper_args="", reducer_args=""):
    '''
    Function that iterates the page rank assignment
    '''
    TEAM_ID = data['team_id']
    ASSIGNMENT_ID = data['ass_id']
    TESTCASE_ID = testcase_id

    hdfs_output_path = f"/{ASSIGNMENT_ID}/{TESTCASE_ID}/{TEAM_ID}"
    log_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/log.txt"
    output_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/output.txt"
    status_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/status.txt"

    CONVERGE = False
    executeCommand(f"rm {v1_file_path} {iteration_log_path}")
    executeCommand(f"hdfs dfs -rm -r -f {hdfs_output_path}")
    
    iter = 1
    while not CONVERGE:
        cleanup(hdfs_output_path)
        runHadoopJob(mapper_path, reducer_path, hdfs_input_path,
                     hdfs_output_path, log_path, mapper_args, reducer_args)
        executeCommand(f"touch {v1_file_path}")
        executeCommand(
            f"hdfs dfs -cat {hdfs_output_path}/part-00000 > {v1_file_path}")
        CONVERGE = checkConvergence(
            v_file_path, v1_file_path, iteration_log_path, convergence_limit, iter)
        iter += 1

    marks = 0
    message = None
    check_compilation = checkCompilationHadoop(hdfs_output_path)
    # If submission compiles
    if check_compilation:
        executeCommand(
            f"hdfs dfs -cat {hdfs_output_path}/part-00000 > {output_path}")
        # Obtains the difference between submission and solution
        check_output = compareOutputAssignment2(output_path, solution_path) # compareOutput(output_path, solution_path)
        # If the output is the same as desired output
        if check_output:
            marks += 1
        message = writeToOutputPath(
            status_path, check_output, ASSIGNMENT_ID, TESTCASE_ID)

    else:
        message = "Compile Error"
        with open(status_path, 'w') as status_file:
            status_file.write(message)

        with open(log_path) as logfile:
            lines = logfile.readlines()
            for line in lines:
                if line.startswith("Error:") or "Error" in line:
                    message = f"{message}: {line}"
                    break

    return marks, message


def runSparkAssignment(data, solution_path, testcase_id, spark_script_path, args=""):
    '''
    Function used to run spark assignments
    '''
    TEAM_ID = data['team_id']
    ASSIGNMENT_ID = data['ass_id']
    TESTCASE_ID = testcase_id

    log_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/log.txt"
    output_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/output.txt"
    status_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/status.txt"

    runSparkJob(spark_script_path, output_path, log_path, args)

    marks = 0 
    message = None
    check_compilation = checkCompilationSpark(output_path)
    # If submission compiled
    if check_compilation:
        check_output = compareOutput(output_path, solution_path)
        # If submission gives desired output
        if check_output:
            marks += 1

        message = writeToOutputPath(
            status_path, check_output, ASSIGNMENT_ID, TESTCASE_ID)
    else:
        message = "Compile Error"
        with open(status_path, 'w') as status_file:
            status_file.write(message)

        with open(output_path) as logfile:
            error_content = logfile.read().strip()
            message = f"{message}: {error_content}"

    return marks, message


def runHadoopAssignment(data, solution_path, hdfs_input_path, testcase_id, mapper_path, reducer_path, mapper_args="", reducer_args=""):
    '''
    Function used to run hadoop assignments
    '''
    TEAM_ID = data['team_id']
    ASSIGNMENT_ID = data['ass_id']
    TESTCASE_ID = testcase_id

    hdfs_output_path = f"/{ASSIGNMENT_ID}/{TESTCASE_ID}/{TEAM_ID}"
    cleanup(hdfs_output_path)

    log_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/log.txt"
    output_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/output.txt"
    status_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/status.txt"
    runHadoopJob(mapper_path, reducer_path, hdfs_input_path,
                 hdfs_output_path, log_path, mapper_args, reducer_args)

    check_compilation = checkCompilationHadoop(hdfs_output_path)
    marks = 0
    message = None
    if check_compilation:
        if "A1" in ASSIGNMENT_ID:
            executeCommand(
                f"hdfs dfs -cat {hdfs_output_path}/part-00000 > {output_path}")

        elif "A2" in ASSIGNMENT_ID:
            if "T1" in ASSIGNMENT_ID:
                output_path = reducer_args
            else:
                output_path = None  # T2

        check_output = compareOutput(output_path, solution_path)
        if check_output:
            marks += 1
        message = writeToOutputPath(
            status_path, check_output, ASSIGNMENT_ID, TESTCASE_ID)

    else:
        message = "Compile Error"
        with open(status_path, 'w') as status_file:
            status_file.write(message)

        with open(log_path) as logfile:
            lines = logfile.readlines()
            for line in lines:
                if line.startswith("Error:") or "Error" in line:
                    message = f"{message}: {line}"
                    break

    return marks, message


def evaluation(data):
    '''
    Function used for evaluation
    '''
    TEAM_ID = data['team_id']
    ASSIGNMENT_ID = data['ass_id']

    if not os.path.isdir(f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}"):
        os.makedirs(f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}")
    # For assignments other than A3
    if ASSIGNMENT_ID[:2] != "A3":
        mapper_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/mapper.py"
        with open(mapper_path, 'w') as mapper_file:
            mapper_code = base64.b64decode(data['source_files'][0]['file'])
            mapper_file.write(mapper_code.decode('ascii'))

        reducer_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/reducer.py"
        with open(reducer_path, 'w') as reducer_file:
            reducer_code = base64.b64decode(data['source_files'][1]['file'])
            reducer_file.write(reducer_code.decode('ascii'))

        executeCommand(f"chmod +x {mapper_path} {reducer_path}")
        executeCommand(f"dos2unix {mapper_path} {reducer_path}")

    else:
        spark_script_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/script.py"
        with open(spark_script_path, 'w') as spark_script_file:
            spark_script_code = base64.b64decode(data['source_files'][0]['file'])
            spark_script_file.write(spark_script_code.decode('ascii'))

    if ASSIGNMENT_ID == "A1T1":

        total_marks = 0
        total_message = str()

        TESTCASE_ID = "C1"
        if not os.path.isdir(f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}"):
            os.makedirs(
                f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}")

        try:
            marks, message = runHadoopAssignment(
                data,
                "../solutions/A1T1C1.txt",
                "/A1/US_ACCIDENT_DATA_5PERCENT.json",
                TESTCASE_ID,
                mapper_path,
                reducer_path,
            )
        except JobTimeoutException:
            marks = 0
            message = f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Timeout Error"

        total_marks += marks
        total_message += f'{message}\n'

        TESTCASE_ID = "C2"
        if not os.path.isdir(f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}"):
            os.makedirs(
                f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}")

        try:
            marks, message = runHadoopAssignment(
                data,
                "../solutions/A1T1C2.txt",
                "/A1/US_ACCIDENT_DATA_25PERCENT.json",
                TESTCASE_ID,
                mapper_path,
                reducer_path,
            )
        except JobTimeoutException:
            marks = 0
            message = f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Timeout Error"

        total_marks += marks
        total_message += f'{message}\n'

        record = {
            '_id': data['id'],
            'ass_id': data['ass_id']
        }

        team_db = mongodb_client[f'{ASSIGNMENT_ID[:2]}']
        results_collection = team_db['results']

        newValues = {"$set": {"marks": total_marks,
                              "message": total_message.strip()}}
        _ = results_collection.update_one(record, newValues)

    if ASSIGNMENT_ID == "A1T2":

        total_marks = 0
        total_message = str()

        TESTCASE_ID = "C1"
        if not os.path.isdir(f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}"):
            os.makedirs(
                f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}")

        try:
            marks, message = runHadoopAssignment(
                data,
                "../solutions/A1T2C1.txt",
                "/A1/US_ACCIDENT_DATA_20PERCENT.json",
                TESTCASE_ID,
                mapper_path,
                reducer_path,
                mapper_args='26 -80 2'
            )
        except JobTimeoutException:
            marks = 0
            message = f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Timeout Error"

        total_marks += marks
        total_message += f'{message}\n'

        TESTCASE_ID = "C2"
        if not os.path.isdir(f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}"):
            os.makedirs(
                f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}")

        try:
            marks, message = runHadoopAssignment(
                data,
                "../solutions/A1T2C2.txt",
                "/A1/US_ACCIDENT_DATA_20PERCENT.json",
                TESTCASE_ID,
                mapper_path,
                reducer_path,
                mapper_args='40 -66 7.5'
            )
        except JobTimeoutException:
            marks = 0
            message = f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Timeout Error"

        total_marks += marks
        total_message += f'{message}\n'

        record = {
            '_id': data['id'],
            'ass_id': data['ass_id']
        }

        team_db = mongodb_client[f'{ASSIGNMENT_ID[:2]}']
        results_collection = team_db['results']

        newValues = {"$set": {"marks": total_marks,
                              "message": total_message.strip()}}
        _ = results_collection.update_one(record, newValues)

    if ASSIGNMENT_ID == "A2T1":

        total_marks = 0
        total_message = str()

        TESTCASE_ID = "C1"
        if not os.path.isdir(f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}"):
            os.makedirs(
                f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}")

        v_file_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/v.txt"

        try:
            marks, message = runHadoopAssignment(
                data,
                "../solutions/A2T1C1.txt",
                "/A2/dataset-3.0.txt",
                TESTCASE_ID,
                mapper_path,
                reducer_path,
                reducer_args=f'{v_file_path}'
            )
        except JobTimeoutException:
            marks = 0
            message = f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Timeout Error"

        total_marks += marks
        total_message += f'{message}\n'

        TESTCASE_ID = "C2"
        if not os.path.isdir(f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}"):
            os.makedirs(
                f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}")

        v_file_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/v.txt"

        try:
            marks, message = runHadoopAssignment(
                data,
                "../solutions/A2T1C2.txt",
                "/A2/dataset-15.0.txt",
                TESTCASE_ID,
                mapper_path,
                reducer_path,
                reducer_args=f'{v_file_path}'
            )
        except JobTimeoutException:
            marks = 0
            message = f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Timeout Error"

        total_marks += marks
        total_message += f'{message}\n'

        record = {
            '_id': data['id'],
            'ass_id': data['ass_id']
        }

        team_db = mongodb_client[f'{ASSIGNMENT_ID[:2]}']
        results_collection = team_db['results']

        newValues = {"$set": {"marks": total_marks,
                              "message": total_message.strip()}}
        _ = results_collection.update_one(record, newValues)

    if ASSIGNMENT_ID == "A2T2":

        total_marks = 0
        total_message = str()

        TESTCASE_ID = "C1"
        if not os.path.isdir(f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}"):
            os.makedirs(
                f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}")

        try:
            status_file_exist = True
            with open(f"/home/{system_username}/{TEAM_ID}/A2T1/{TESTCASE_ID}/status.txt") as status_check_file:
                status_content = status_check_file.read().strip()
        except FileNotFoundError:
            status_file_exist = False

        if status_file_exist and "Correct" in status_content:
            executeCommand(
                f"cp /home/{system_username}/{TEAM_ID}/A2T1/{TESTCASE_ID}/v.txt /home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/v.txt")
            solution_path = f"/home/{system_username}/backend/solutions/A2T2C1.txt"
            v_file_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/v.txt"
            v1_file_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/v1.txt"
            iteration_log_file_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/iterlog.txt"
            page_embedding_path = f"/home/{system_username}/backend/data/node-map-3-5.json"
            hdfs_input_path = f"/A2T1/{TESTCASE_ID}/{TEAM_ID}/part-00000"

            try:
                marks, message = iterateAssignment2(
                    data,
                    TESTCASE_ID,
                    v_file_path,
                    v1_file_path,
                    iteration_log_file_path,
                    3.0,
                    solution_path,
                    mapper_path,
                    reducer_path,
                    hdfs_input_path,
                    mapper_args=f"{v_file_path} {page_embedding_path}"
                )
            except JobTimeoutException:
                marks = 0
                message = f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Timeout Error"

            total_marks += marks
            total_message += f'{message}\n'

        else:
            if not status_file_exist:
                total_message+= f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Submission for A2T1 does not exist.\n"
            else:
                total_message+= f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Submission for A2T1 is INCORRECT. Cannot proceed with A2T2.\n"


        TESTCASE_ID = "C2"
        if not os.path.isdir(f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}"):
            os.makedirs(
                f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}")

        try:
            status_file_exist = True
            with open(f"/home/{system_username}/{TEAM_ID}/A2T1/{TESTCASE_ID}/status.txt") as status_check_file:
                status_content = status_check_file.read().strip()
        except FileNotFoundError:
            status_file_exist = False

        if status_file_exist and "Correct" in status_content:
            executeCommand(
                f"cp /home/{system_username}/{TEAM_ID}/A2T1/{TESTCASE_ID}/v.txt /home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/v.txt")
            solution_path = f"/home/{system_username}/backend/solutions/A2T2C2.txt"
            v_file_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/v.txt"
            v1_file_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/v1.txt"
            iteration_log_file_path = f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}/iterlog.txt"
            page_embedding_path = f"/home/{system_username}/backend/data/node-map-15-5.json"
            hdfs_input_path = f"/A2T1/{TESTCASE_ID}/{TEAM_ID}/part-00000"

            try:
                marks, message = iterateAssignment2(
                    data,
                    TESTCASE_ID,
                    v_file_path,
                    v1_file_path,
                    iteration_log_file_path,
                    10.0,
                    solution_path,
                    mapper_path,
                    reducer_path,
                    hdfs_input_path,
                    mapper_args=f"{v_file_path} {page_embedding_path}"
                )
            except JobTimeoutException:
                marks = 0
                message = f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Timeout Error"

            total_marks += marks
            total_message += f'{message}\n'

        else:
            if not status_file_exist:
                total_message+= f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Submission for A2T1 does not exist.\n"
            else:
                total_message+= f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Submission for A2T1 is INCORRECT. Cannot proceed with A2T2.\n"

        record = {
            '_id': data['id'],
            'ass_id': data['ass_id']
        }

        team_db = mongodb_client[f'{ASSIGNMENT_ID[:2]}']
        results_collection = team_db['results']

        newValues = {"$set": {"marks": total_marks,
                              "message": total_message.strip()}}
        _ = results_collection.update_one(record, newValues)


    if ASSIGNMENT_ID == "A3T1":

        total_marks = 0
        total_message = str()

        TESTCASE_ID = "C1"
        if not os.path.isdir(f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}"):
            os.makedirs(
                f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}")

        try:
            marks, message = runSparkAssignment(
                data,
                "../solutions/A3T1C1.txt",
                TESTCASE_ID,
                spark_script_path,
                'Germany ../data/city_sample_25percent.csv',
            )
        except JobTimeoutException:
            marks = 0
            message = f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Timeout Error"

        total_marks += marks
        total_message += f'{message}\n'

        TESTCASE_ID = "C2"
        if not os.path.isdir(f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}"):
            os.makedirs(
                f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}")

        try:
            marks, message = runSparkAssignment(
                data,
                "../solutions/A3T1C2.txt",
                TESTCASE_ID,
                spark_script_path,
                'India ../data/city.csv',
            )
        except JobTimeoutException:
            marks = 0
            message = f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Timeout Error"

        total_marks += marks
        total_message += f'{message}\n'

        record = {
            '_id': data['id'],
            'ass_id': data['ass_id']
        }

        team_db = mongodb_client[f'{ASSIGNMENT_ID[:2]}']
        results_collection = team_db['results']

        newValues = {"$set": {"marks": total_marks,
                              "message": total_message.strip()}}
        _ = results_collection.update_one(record, newValues)


    if ASSIGNMENT_ID == "A3T2":

        total_marks = 0
        total_message = str()

        TESTCASE_ID = "C1"
        if not os.path.isdir(f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}"):
            os.makedirs(
                f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}")

        try:
            marks, message = runSparkAssignment(
                data,
                "../solutions/A3T2C1.txt",
                TESTCASE_ID,
                spark_script_path,
                '../data/city_sample_25percent.csv ../data/global.csv',
            )
        except JobTimeoutException:
            marks = 0
            message = f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Timeout Error"

        total_marks += marks
        total_message += f'{message}\n'

        TESTCASE_ID = "C2"
        if not os.path.isdir(f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}"):
            os.makedirs(
                f"/home/{system_username}/{TEAM_ID}/{ASSIGNMENT_ID}/{TESTCASE_ID}")

        try:
            marks, message = runSparkAssignment(
                data,
                "../solutions/A3T2C2.txt",
                TESTCASE_ID,
                spark_script_path,
                '../data/city.csv ../data/global.csv',
            )
        except JobTimeoutException:
            marks = 0
            message = f"{ASSIGNMENT_ID} - {TESTCASE_ID}: Timeout Error"

        total_marks += marks
        total_message += f'{message}\n'

        record = {
            '_id': data['id'],
            'ass_id': data['ass_id']
        }

        team_db = mongodb_client[f'{ASSIGNMENT_ID[:2]}']
        results_collection = team_db['results']

        newValues = {"$set": {"marks": total_marks,
                              "message": total_message.strip()}}
        _ = results_collection.update_one(record, newValues)
