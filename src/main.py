import os
import pytz
import json
import redis
import datetime
from rq import Queue
from rq.job import Job
from rq.command import send_stop_job_command
from rq.registry import StartedJobRegistry
from pymongo import MongoClient
from flask_cors import CORS
from flask import Flask, request, Response
from evaluation import evaluation
from moss import getPlagiarismResult, calculatePlagiarismResult

IST = pytz.timezone('Asia/Kolkata')

app = Flask(__name__)
CORS(app)

# URL at which MongoDB Database is located
MONGODB_URL = "mongodb://root:root@mongodb:27017"
# Connects flask app to the db via a URI
app.config["MONGO URI"] = MONGODB_URL
# Client object of the MongoDB client via which the DB can accesse
mongodb_client = MongoClient(MONGODB_URL)

# URL for Redis server
REDIS_URL = 'redis://redis:6379'
# Redis connection is set up
redis_connection = redis.from_url(REDIS_URL)
# Initializes a queue from the redis connection
redis_queue = Queue(connection=redis_connection)

# Contains blacklisted teams and which assignments they are blacklisted for
BLACKLISTED_TEAMS = dict()


def loadBlacklistedTeams():
    '''
    Loads all the blacklisted teams into the above dictionary
    '''
    global BLACKLISTED_TEAMS
    BLACKLISTED_TEAMS = dict()
    # Checks if file is there in the current working directory
    if "BLACKLISTED_TEAMS.txt" in os.listdir():
        # Opens the file
        with open("BLACKLISTED_TEAMS.txt") as blacklisted_team_file:
            # Reads the contents of the file and splits into a list
            lines = blacklisted_team_file.read().strip().split('\n')
            # Iterates over each line
            for line in lines:
                # Extracts team ID and assignment ID
                team_id, assignment_id = line.split(',')
                # Removes trailing and leading whitespaces
                team_id = team_id.strip()
                assignment_id = assignment_id.strip()
                # If the team was not blacklisted before
                if team_id not in BLACKLISTED_TEAMS:
                    # Add team to blacklisted teams
                    BLACKLISTED_TEAMS[team_id] = list()
                # Add assignment ID to team blacklist
                BLACKLISTED_TEAMS[team_id].append(assignment_id)


def updateBlacklistedTeamsFile():
    '''
    Updates blacklisted teams
    '''
    global BLACKLISTED_TEAMS
    # Create a new file to contain updated list of blacklisted teams
    with open("BLACKLISTED_TEAMS_NEW.txt", 'w') as new_blacklisted_teams_file:
        # Below loop copies content from dictionary in memory to the txt file
        for team_id in BLACKLISTED_TEAMS:
            for assignment_id in BLACKLISTED_TEAMS[team_id]:
                new_blacklisted_teams_file.write(f"{team_id},{assignment_id}\n")
    # Delete old file
    os.remove("BLACKLISTED_TEAMS.txt")
    # Rename new file to the same name as old file
    os.rename("BLACKLISTED_TEAMS_NEW.txt", "BLACKLISTED_TEAMS.txt")


def checkTeamIsBlacklistedFromAssignment(team_id, assignment_id):
    '''
    Checks if a team is blacklisted from a particular assignment
    '''
    global BLACKLISTED_TEAMS
    return team_id in BLACKLISTED_TEAMS and assignment_id in BLACKLISTED_TEAMS[team_id]


@app.route("/hello")
def hello():
    # If user is not authenticated
    if request.headers['x-access-token'] != 'BINOD':
        return Response(), 203
    # If user is authenticated
    return "<h1>Hello World!</h1>", 200


@app.route("/input", methods=['POST'])
def assignmentSubmission():
    '''
    Submits assignment to the Redis Queue
    '''
    data = request.get_json()
    # Assignment data
    '''
    data = {
    id: 1234,
    team_id: BD_1_2_3_4,
    ass_id: A1T1,
    source_files: [
        {
        "file": "for _ in range(10): ..."
        },
        {
        "file": "for _ in range(10)..."
        }
    ]
    '''

    # print("Data received: ", data)

    if(request.headers['x-access-token'] != 'BINOD'):
        return Response(), 203
    # Extract relevant data to insert into DB
    record = {
        "_id": data['id'],
        "ass_id": data['ass_id'],
        "marks": -1,
        "message": "Processing"
    }

    ASSIGNMENT_ID = data["ass_id"]
    TEAM_ID = data["team_id"]
    SUBMISSION_ID = data["id"]

    if checkTeamIsBlacklistedFromAssignment(TEAM_ID, ASSIGNMENT_ID):
        blacklist_flag = True
        record["message"] = "Team is blacklisted until further notice. Submission will not be evaluated."
    else:
        blacklist_flag = False
    # Get team data from the DB
    team_db = mongodb_client[f'{ASSIGNMENT_ID[:2]}']
    # Get results of that particular team's submissions
    results_collection = team_db['results']
    # Insert temporary results of the team
    _ = results_collection.insert_one(record)
    # Assign relevant job timeout based on assignment ID
    if not blacklist_flag:
        try:
            if ASSIGNMENT_ID == "A1T1":
                job_timeout = 150
            elif ASSIGNMENT_ID == "A1T2":
                job_timeout = 900
            elif ASSIGNMENT_ID == "A2T1":
                job_timeout = 300
            elif ASSIGNMENT_ID == "A2T2":
                job_timeout = 900
            elif ASSIGNMENT_ID == "A3T1":
                job_timeout = 120
            elif ASSIGNMENT_ID == "A3T2":
                job_timeout = 120
            else:
                job_timeout = 60
            # Add job to the queue
            job = redis_queue.enqueue(evaluation, data, job_timeout=job_timeout, job_id=f"{TEAM_ID}-{ASSIGNMENT_ID}-{SUBMISSION_ID}")

        except Exception as error:
            print(f"Error occured: {error}")

    return Response(), 200


@app.route("/check_submission", methods=['POST'])
@app.route("/submission", methods=['POST'])
def checkSubmissionStatus():
    '''
    Checking submission status
    '''
    # Getting relevant data from the frontend
    data = request.get_json()

    '''
    data = {
        submission_id: [1234, 1235, ...],
        ass_id: A1T1
        x-access-token: BINOD
    }
    '''

    # if request.headers['x-access-token'] != 'BINOD':
    #     return Response(), 203

    # print("Data received: ", data)
    result = list()
    ASSIGNMENT_ID = data["ass_id"]
    if len(ASSIGNMENT_ID) > 2:
        ASSIGNMENT_ID = ASSIGNMENT_ID[:2]
    # For each submission ID to be checked
    for key in data['submission_id']:
        query_team = {"_id": key}
        # Get team responsible for submission
        team_db = mongodb_client[f'{ASSIGNMENT_ID}']
        # Get results of that particular team
        collection = team_db['results']
        # If the team has submission results ready
        if collection.find_one(query_team):
            marks = collection.find_one(query_team)['marks']
            message = collection.find_one(query_team)['message']
            assignment_id = collection.find_one(query_team)['ass_id']
        else:
            marks = -1
            message = "ID not found"
            assignment_id = -1

        result.append({
            "submission_id": key,
            "marks": marks,
            "message": message,
            "ass_id": assignment_id
        })

    return json.dumps(result), 200


@app.route("/plagiarism-check", methods=["POST"])
async def plagiarismChecker():
    if request.headers['x-access-token'] != 'BINOD':
        return Response(), 203

    data = request.get_json()
    assignment_id = data["ass_id"]
    await calculatePlagiarismResult(assignment_id)
    result = "Plagiarism content will be calculated and stored"
    return json.dumps(result), 200


@app.route("/plagiarism-get", methods=["POST"])
def plagiarismGetter():
    if request.headers['x-access-token'] != 'BINOD':
        return Response(), 203

    data = request.get_json()
    assignment_id = data["ass_id"]
    result = getPlagiarismResult(assignment_id)
    return result, 200


@app.route("/wait", methods=["GET", "POST"])
def getWaitingTime():
    team_db = mongodb_client["A3"]  # should we make this a parameter?
    # Getting jobs that are processing (have marks == -1)
    processing_jobs = list(team_db.results.find({"marks": -1}))
    
    if request.method == "POST":
        data = request.get_json()
        last_id = data["_id"]
    else:
        if processing_jobs:
            last_id = processing_jobs[-1]["_id"]
        else:
            result = f"Job Queue is currently empty :)"
            return result, 200
    # Started JobRegistry holds the list of currently executing jobs
    registry = StartedJobRegistry('default', connection=redis_connection)
    running_job_id = registry.get_job_ids()
    if not running_job_id:
            result = f"No jobs under execution. Queue may or may not be empty. Please try again after a few minutes :)"
            return result, 200

    running_job_id = registry.get_job_ids()[0]
    start_id = int(running_job_id.split('-')[-1])

    wait_times = {
        "A1T1": 1,
        "A1T2": 10,
        "A2T1": 5,
        "A2T2": 13,
        "A3T1": 1,
        "A3T2": 2
    }
    # Wait time calculation
    total_time = 0
    for job in processing_jobs:
        if job["_id"] > start_id and job["_id"] <= last_id:
            total_time += wait_times[job["ass_id"]]

    current_time = datetime.datetime.now(IST)
    end_time = current_time + datetime.timedelta(minutes=total_time)
    # Calculate the result
    result = f"Currently processing: {start_id}\nEstimated time remaining: {total_time} minutes\nJob Queue will complete at {end_time}"
    return result, 200


@app.route("/cancel", methods=["GET", "POST"])
def cancelJob():
    '''
    Cancels a job, prevents it from running
    '''
    # Gets relevant data
    data = request.get_json()
    submission_id = data["id"]
    team_id = data["team_id"]
    assignment_id = data["ass_id"]
    # Returns an empty string if the reason is not present
    reason = data.get("reason", "")
    # Get the Job ID
    job_id = f"{team_id}-{assignment_id}-{submission_id}"

    try:
        # Fetch job details from Redis Queue
        job = Job.fetch(job_id, connection=redis_connection)
        job_details = {
            "args": job.args,
            "start_time": job.started_at,
            "enqueue_time": str(job.enqueued_at + datetime.timedelta(hours=5, minutes=30))[:-4]
        }
        job.cancel()

        record = {
            '_id': data['id'],
            'ass_id': data['ass_id']
        }
        # Update DB entry
        team_db = mongodb_client[f'{assignment_id[:2]}']
        results_collection = team_db['results']
        cancel_message = "Job cancelled by TA Team"
        if reason:
            cancel_message = f"{cancel_message}: {reason}"

        newValues = {"$set": {"marks": -1,
                              "message": cancel_message}}
        _ = results_collection.update_one(record, newValues)

        result = f"Job cancelled successfully.\bJob details: {job_details}"
    except Exception as error_message:
        result = f"Job cancellation failed: {error_message}"

    return result, 200


@app.route("/stop", methods=["GET", "POST"])
def stopJob():
    '''
    Stops a currently running
    '''
    # Get relevant data
    data = request.get_json()
    submission_id = data["id"]
    team_id = data["team_id"]
    assignment_id = data["ass_id"]
    reason = data.get("reason", "")
    job_id = f"{team_id}-{assignment_id}-{submission_id}"

    try:
        job = Job.fetch(job_id, connection=redis_connection)
        job_details = {
            "args": job.args,
            "start_time": job.started_at,
            "enqueue_time": str(job.enqueued_at + datetime.timedelta(hours=5, minutes=30))[:-4]
        }
        # Sends a command to stop the job
        send_stop_job_command(redis_connection, job_id)

        record = {
            '_id': data['id'],
            'ass_id': data['ass_id']
        }
        # Updated in the db
        team_db = mongodb_client[f'{assignment_id[:2]}']
        results_collection = team_db['results']
        stop_message = "Job stopped during execution by TA Team"
        if reason:
            stop_message = f"{stop_message}: {reason}"

        newValues = {"$set": {"marks": -1,
                              "message": stop_message}}
        _ = results_collection.update_one(record, newValues)

        result = f"Job stopped successfully.\bJob details: {job_details}"
    except Exception as error_message:
        result = f"Job stop failed: {error_message}"

    return result, 200


@app.route("/queue", methods=["GET", "POST"])
def getQueue():
    '''
    Gets the currently queued jobs
    '''
    queued_jobs = redis_queue.job_ids
    job_data = dict()
    # Gets data of all the queued jobs
    for job_id in queued_jobs:
        team_id, assignment_id, submission_id = job_id.split('-')
        job = Job.fetch(job_id, connection=redis_connection)
        job_details = {
                # "args": job.args,
                "job_id": job_id,
                "submission_id": submission_id,
                # "team_id": team_id,
                # "assignment_id": assignment_id,
                "start_time": job.started_at,
                "enqueue_time": job.enqueued_at + datetime.timedelta(hours=5, minutes=30)
        }
        job_data_key = f"{team_id}-{assignment_id}"
        if job_data_key not in job_data:
            job_data[job_data_key] = list()
        # Team already has another submission for same assignment
        if len(job_data[job_data_key]) > 0:
            # If same assignment has been submitted within 10 mins of last submission
            if job_details['enqueue_time'] - job_data[job_data_key][-1]['enqueue_time'] <= datetime.timedelta(minutes=10):
                # Mark job as spam
                job_details['spam'] = True

        job_data[job_data_key].append(job_details)
    
    return job_data, 200


@app.route("/blacklist", methods=["POST"])
def blacklistTeam():
    '''
    Blacklists a team
    '''
    global BLACKLISTED_TEAMS
    # Gets team ID and assignment ID
    data = request.get_json()
    team_id = data["team_id"]
    assignment_id = data["ass_id"]
    # If team is not blacklisted for any assignment yet
    if team_id not in BLACKLISTED_TEAMS:
        BLACKLISTED_TEAMS[team_id] = list()
    # Add assignment to list of assignments the team has been blacklisted for
    BLACKLISTED_TEAMS[team_id].append(assignment_id)
    BLACKLISTED_TEAMS[team_id] = list(set(BLACKLISTED_TEAMS[team_id]))
    # Update blacklisted teams
    updateBlacklistedTeamsFile()
    return f"Team ID {team_id} has been blacklisted from {assignment_id}", 200
    

@app.route("/unblacklist", methods=["POST"])
def removeBlacklistTeam():
    '''
    Remove a team from the blacklist
    '''
    global BLACKLISTED_TEAMS
    # Get team ID and assignment ID
    data = request.get_json()
    team_id = data["team_id"]
    assignment_id = data["ass_id"]
    # If the team has been blacklisted
    if team_id in BLACKLISTED_TEAMS:
        # If the team has been blacklisted for that assignment
        if assignment_id in BLACKLISTED_TEAMS[team_id]:
            # Remove the assignment from the list of blacklisted assignments for that team
            BLACKLISTED_TEAMS[team_id].remove(assignment_id)
            BLACKLISTED_TEAMS[team_id] = list(set(BLACKLISTED_TEAMS[team_id]))
            # If the team is not blacklisted for any other assignments
            if len(BLACKLISTED_TEAMS[team_id]) == 0:
                # Remove team from list of blacklisted teams
                del BLACKLISTED_TEAMS[team_id]
    # Update the blacklist files
    updateBlacklistedTeamsFile()
    return f"Team ID {team_id} has been removed from the blacklist for {assignment_id}", 200


@app.route("/blacklisted", methods=["GET", "POST"])
def viewBlacklistTeam():
    '''
    Returns the list of blacklisted teams
    '''
    global BLACKLISTED_TEAMS
    return BLACKLISTED_TEAMS, 200


if __name__ == '__main__':
    # Load blacklisted teams
    loadBlacklistedTeams()
    app.run(host="0.0.0.0", port="5000")
