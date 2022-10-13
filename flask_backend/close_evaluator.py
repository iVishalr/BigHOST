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
ASSIGNMENT_CLOSE_MESSAGE = "Portal will open at 10 AM IST"

sleep_until = 'Sat Oct 01 09:03:00 2022' # String format might be locale dependent.

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

f = open(f'./close_portal_logs.txt', 'a+')
backup = sys.stdout
sys.stdout = Tee(sys.stdout, f)

PORTAL_SHUTDOWN_TIME = 'Sun Oct 02 00:30:00 2022'

print("Sleeping until {}...".format(PORTAL_SHUTDOWN_TIME))
print(time.mktime(time.strptime(PORTAL_SHUTDOWN_TIME)) - time.time())

if time.mktime(time.strptime(PORTAL_SHUTDOWN_TIME)) - time.time() >= 0:
    time.sleep(time.mktime(time.strptime(PORTAL_SHUTDOWN_TIME)) - time.time())

print(f"[{get_datetime()}]\tPortal Shutting Down")

os.system("sudo shutdown")