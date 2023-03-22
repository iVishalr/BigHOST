import os
import sys
import time

from dotenv import load_dotenv
from common.db import DataBase
from common.utils import Tee, get_datetime

load_dotenv(os.path.join(os.getcwd(), '.env'))
os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()

CURRENT_ASSIGNMENT = 'A3'
ASSIGNMENT_OPEN = False
ASSIGNMENT_CLOSE_MESSAGE = "Portal will open on 10th Nov, 2022."

sleep_until = 'Sat Nov 05 23:59:10 2022' # String format might be locale dependent.

f = open(f'./close_assignment_logs.txt', 'a+')
backup = sys.stdout
sys.stdout = Tee(sys.stdout, f)
db = DataBase()
print("Sleeping until {}...".format(sleep_until))
print(time.mktime(time.strptime(sleep_until)) - time.time())

if time.mktime(time.strptime(sleep_until)) - time.time() >= 0:
    time.sleep(time.mktime(time.strptime(sleep_until)) - time.time())

db.close_assignment(CURRENT_ASSIGNMENT, ASSIGNMENT_CLOSE_MESSAGE)
print(f"[{get_datetime()}]\tAssignment : {CURRENT_ASSIGNMENT} Closed.")

PORTAL_SHUTDOWN_TIME = 'Sun Nov 06 00:30:00 2022'

print("Sleeping until {}...".format(PORTAL_SHUTDOWN_TIME))
print(time.mktime(time.strptime(PORTAL_SHUTDOWN_TIME)) - time.time())

if time.mktime(time.strptime(PORTAL_SHUTDOWN_TIME)) - time.time() >= 0:
    time.sleep(time.mktime(time.strptime(PORTAL_SHUTDOWN_TIME)) - time.time())

print(f"[{get_datetime()}]\tPortal Shutting Down")

os.system("sudo shutdown")