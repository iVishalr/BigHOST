import os
import sys
import json
import time
import pickle
import requests
import subprocess

from pprint import pprint
from smtp import mail_queue
from datetime import datetime
from dotenv import load_dotenv
from flask_backend import queue
from pymongo import MongoClient
from flask_cors import cross_origin
from flask import Flask, request, jsonify
from signal import signal, SIGPIPE, SIG_DFL

load_dotenv(os.path.join(os.getcwd(), '.env'))

client_rr = MongoClient(os.getenv('MONGO_URI_RR'), connect=False)
db_rr = client_rr['bd']
assignment_rr = db_rr['assignmentQuestion']

client_ec = MongoClient(os.getenv('MONGO_URI_EC'), connect=False)
db_ec = client_ec['bd']
assignment_ec = db_ec['assignmentQuestion']

evaluator_internal_ip = os.getenv('EVALUATOR_INTERNAL_IP')
evaluator_external_ip = os.getenv('EVALUATOR_EXTERNAL_IP')

os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()

CURRENT_ASSIGNMENT = 'A2'
ASSIGNMENT_OPEN = False
ASSIGNMENT_CLOSE_MESSAGE = "A2 Completed"

sleep_until = 'Sat Oct 01 23:59:10 2022' # String format might be locale dependent.

def get_datetime() -> str:
    now = datetime.now()
    timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
    return timestamp

class Tee(object):
    def __init__(self, *files):
        self.files = files
    def write(self, obj):
        for f in self.files:
            f.write(obj)
    def flush(self):
        pass

f = open(f'./close_assignment_logs.txt', 'a+')
backup = sys.stdout
sys.stdout = Tee(sys.stdout, f)

print("Sleeping until {}...".format(sleep_until))
print(time.mktime(time.strptime(sleep_until)) - time.time())

if time.mktime(time.strptime(sleep_until)) - time.time() >= 0:
    time.sleep(time.mktime(time.strptime(sleep_until)) - time.time())

doc = assignment_rr.find_one({'assignmentId' : CURRENT_ASSIGNMENT})
doc['assignmentOpen'] = ASSIGNMENT_OPEN
doc['assignmentClosedMessage'] = ASSIGNMENT_CLOSE_MESSAGE

doc = assignment_rr.find_one_and_update({'assignmentId': CURRENT_ASSIGNMENT}, {'$set': {'assignmentOpen': doc['assignmentOpen'], 'assignmentClosedMessage': doc['assignmentClosedMessage']}})

doc = assignment_ec.find_one({'assignmentId' : CURRENT_ASSIGNMENT})
doc['assignmentOpen'] = ASSIGNMENT_OPEN
doc['assignmentClosedMessage'] = ASSIGNMENT_CLOSE_MESSAGE

doc = assignment_ec.find_one_and_update({'assignmentId': CURRENT_ASSIGNMENT}, {'$set': {'assignmentOpen': doc['assignmentOpen'], 'assignmentClosedMessage': doc['assignmentClosedMessage']}})

print(f"[{get_datetime()}]\tAssignment : {CURRENT_ASSIGNMENT} Closed.")