import os
import sys
import time

from dotenv import load_dotenv
from common.utils import Tee, get_datetime

load_dotenv(os.path.join(os.getcwd(), '.env'))
os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()

f = open(f'./close_portal_logs.txt', 'a+')
backup = sys.stdout
sys.stdout = Tee(sys.stdout, f)

PORTAL_SHUTDOWN_TIME = 'Sun Nov 06 00:30:00 2022'

print("Sleeping until {}...".format(PORTAL_SHUTDOWN_TIME))
print(time.mktime(time.strptime(PORTAL_SHUTDOWN_TIME)) - time.time())

if time.mktime(time.strptime(PORTAL_SHUTDOWN_TIME)) - time.time() >= 0:
    time.sleep(time.mktime(time.strptime(PORTAL_SHUTDOWN_TIME)) - time.time())

print(f"[{get_datetime()}]\tPortal Shutting Down")
os.system("sudo shutdown")