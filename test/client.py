import os
import json
import requests
import time
from time import sleep
from pprint import pprint
import random
import argparse

parser = argparse.ArgumentParser(
    prog="client.py",
    description="BigHOST Testing Client",
    formatter_class=argparse.MetavarTypeHelpFormatter,
)

parser.add_argument("--type", type=str, default="valid", help="Type of request to send. valid | syntax | infinite | import")
parser.add_argument("--nreqs", type=int, default=64, help="Number of requests to send.")

args = parser.parse_args()

url = "http://35.213.133.27:9000/sanity-check"

data = {'teamId': 'BD1_ADMIN_09', 
        'assignmentId': 'A2T1', 
        'mapper': None, 
        'reducer': None,
        'timeout': 300, 
        "submissionId": None
    }

if args.type == "syntax":
    with open("./docker/A2/task1/mapper.py", "r") as f:
        data['mapper'] = f.read()

    with open("./test/r_syntax_error.py", "r") as f:
        data['reducer'] = f.read()

elif args.type == "infinite":
    with open("./test/m_infinite_loop.py", "r") as f:
        data['mapper'] = f.read()

    with open("./docker/A2/task1/reducer.py", "r") as f:
        data['reducer'] = f.read()

elif args.type == "import":
    with open("./test/m_import.py", "r") as f:
        data['mapper'] = f.read()

    with open("./docker/A2/task1/reducer.py", "r") as f:
        data['reducer'] = f.read()

elif args.type == "valid":
    with open("./docker/A2/task1/mapper.py", "r") as f:
        data['mapper'] = f.read()

    with open("./docker/A2/task1/reducer.py", "r") as f:
        data['reducer'] = f.read()

else:
    print("Error: No such request type.")
    exit(1)

# data['submissionId'] = int(str(time.time_ns())[:13])
req_num = 0
for _ in range(args.nreqs//8):
    for i in range(0, 8):
        # if i < 10:
        #     data['teamId'] = f'BD1_ADMIN_0{i}'
        # else:
        #     data['teamId'] = f'BD2_ADMIN_0{i}'
        data['submissionId'] = int(str(time.time_ns())[:10]) + random.randint(10,10000)
        payload = json.dumps(data)
        r = requests.post(url, data=payload)
        print(f"{i+1} {r.status_code}")
        res = json.loads(r.text)
        print(res)
        req_num += 1    
    # sleep(5)

for i in range(req_num, args.nreqs):
    data['submissionId'] = int(str(time.time_ns())[:10]) + random.randint(10,10000)
    payload = json.dumps(data)
    r = requests.post(url, data=payload)
    print(f"{i+1} {r.status_code}")
    res = json.loads(r.text)
    print(res)