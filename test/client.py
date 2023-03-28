import os
import json
import requests
import time
from time import sleep
from pprint import pprint
import random
# import ast

# r = requests.get("http://127.0.0.1:9000/empty-queue")
# res = json.loads(r.text)
# print(res)
# print(r.status_code)

# url = "http://35.213.133.27:9000/sanity-check"
url = "http://localhost:9000/sanity-check"

# data = {'teamId': 'BD1_ADMIN_09', 
# 'assignmentId': 'A1T1', 
# 'mapper': '#!/usr/bin/env python3\nimport sys\n\ndef mapper():\n    for line in sys.stdin:\n        data=line.strip().split()\n        print(f"{data[0]},{data[1]}")\n\nif __name__ == \'__main__\':\n    mapper()\n', 
# 'reducer': '#!/usr/bin/env python3\nimport sys\n\ndef reducer():\n    path = sys.argv[1]\n    f = open(path,"w+")\n    current_key = None\n    s = \'\'\n    for line in sys.stdin:\n        key,val = line.strip().split(\',\')\n        if current_key == None:\n            current_key = key\n            s = f"{key} [{val}"\n            f.write(f"{key},1\\n")\n            continue\n        if key == current_key:\n            s += f",{val}"\n        else:\n            print(s+"]")\n            current_key = key\n            s = f"{key} [{val}"\n            f.write(f"{key},1\\n")\n    print(s+"]")\n    f.close()\n\nif __name__ == \'__main__\':\n    reducer()\n', 
# 'submissionId': time.time()}

# data = {'teamId': 'BD1_ADMIN_09', 
# 'assignmentId': 'A1T2', 
# 'mapper': f'{open("./docker/A1/task2/mapper.py").read()}', 
# 'reducer': f'{open("./docker/A1/task2/reducer.py").read()}', 
# 'submissionId': time.time()}

data = {'teamId': 'BD1_ADMIN_09', 
        'assignmentId': 'A2T1', 
        'mapper': None, 
        'reducer': None,
        'timeout': 300, 
        "submissionId": None
    }

with open("./docker/A2/task1/mapper.py", "r") as f:
    data['mapper'] = f.read()

with open("./docker/A2/task1/reducer.py", "r") as f:
    data['reducer'] = f.read()

# data['submissionId'] = int(str(time.time_ns())[:13])

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
    # sleep(0.5)

# data = {'teamId': 'BD1_ADMIN_09', 
# 'assignmentId': 'A1T1', 
# 'mapper': f'{open("./docker/A1/task1/mapper.py").read()}', 
# 'reducer': f'{open("./docker/A1/task1/reducer.py").read()}', 
# 'submissionId': time.time()}



# for i in range(2):
#     data['submissionId'] = int(str(time.time_ns())[:13])
#     payload = json.dumps(data)
#     r = requests.post(url, data=payload)
#     print(f"{i+1} {r.status_code}")
#     res = json.loads(r.text)
#     print(res)

# with open("./test/m.py", "r") as f:
#     data['mapper'] = f.read()

# with open("./test/r.py", "r") as f:
#     data['reducer'] = f.read()

# for i in range(4):
#     teamId = data["teamId"]
#     teamId = teamId.split("_")
#     last_srn = int(teamId[-1])
#     last_srn += 1
#     last_srn = str(last_srn)
#     last_srn = "0" * (3-len(last_srn)) + last_srn
#     teamId[-1] = last_srn
#     data["teamId"] = "_".join(teamId)
#     print(f"Team ID : {data['teamId']}")
#     payload = [data]
#     payload = json.dumps(payload)
#     r = requests.post(url, data=payload)
#     print(f"{i+1} {r.status_code}")
#     res = json.loads(r.text)
#     print(res)
#     sleep(0.1)

# data = {'teamId': 'BD_019_536_571_004', 
#         'assignmentId': 'A1T1', 
#         'mapper': None, 
#         'reducer': None, 
#         'timeout': 30, 
#         "submissionId": "submissionId1"
#     }

# with open("./test/m.py", "r") as f:
#     data['mapper'] = f.read()

# with open("./test/r_infinite_loop.py", "r") as f:
#     data['reducer'] = f.read()

# for i in range(4):
#     teamId = data["teamId"]
#     teamId = teamId.split("_")
#     last_srn = int(teamId[-1])
#     last_srn += 1
#     last_srn = str(last_srn)
#     last_srn = "0" * (3-len(last_srn)) + last_srn
#     teamId[-1] = last_srn
#     data["teamId"] = "_".join(teamId)
#     print(f"Team ID : {data['teamId']}")
#     payload = [data]
#     payload = json.dumps(payload)
#     r = requests.post(url, data=payload)
#     print(f"{i+1} {r.status_code}")
#     res = json.loads(r.text)
#     print(res)
#     sleep(0.1)

# data = {'teamId': 'BD_019_536_571_008', 
#         'assignmentId': 'A1T1', 
#         'mapper': None, 
#         'reducer': None, 
#         'timeout': 30, 
#         "submissionId": "submissionId1"
#     }

# with open("./test/m_invalid.py", "r") as f:
#     data['mapper'] = f.read()

# with open("./test/r_invalid.py", "r") as f:
#     data['reducer'] = f.read()

# for i in range(10):
#     teamId = data["teamId"]
#     teamId = teamId.split("_")
#     last_srn = int(teamId[-1])
#     last_srn += 1
#     last_srn = str(last_srn)
#     last_srn = "0" * (3-len(last_srn)) + last_srn
#     teamId[-1] = last_srn
#     data["teamId"] = "_".join(teamId)
#     print(f"Team ID : {data['teamId']}")
#     payload = [data]
#     payload = json.dumps(payload)
#     r = requests.post(url, data=payload)
#     print(f"{i+1} {r.status_code}")
#     res = json.loads(r.text)
#     print(res)
#     sleep(0.1)

# data = {'teamId': 'BD_ADMIN_09', 
#         'assignmentId': 'A2T2', 
#         'mapper': None, 
#         'reducer': None,
#         'timeout': 30, 
#         "submissionId": None
#     }

# with open("./docker/A2/task2/mapper.py", "r") as f:
#     data['mapper'] = f.read()

# with open("./docker/A2/task2/reducer.py", "r") as f:
#     data['reducer'] = f.read()

# data['submissionId'] = int(str(time.time_ns())[:13])

# for i in range(5):
#     teamId = data["teamId"]
#     teamId = teamId.split("_")
#     last_srn = int(teamId[-1])
#     last_srn += 1
#     last_srn = str(last_srn)
#     last_srn = "0" * (3-len(last_srn)) + last_srn
#     teamId[-1] = last_srn
#     data["teamId"] = "_".join(teamId)
#     print(f"Team ID : {data['teamId']}")
#     payload = [data]
#     payload = json.dumps(payload)
#     r = requests.post(url, data=payload)
#     print(f"{i+1} {r.status_code}")
#     res = json.loads(r.text)
#     print(res)
#     sleep(0.1)

# data = {'teamId': 'BD_019_536_571_023', 
#         'assignmentId': 'A1T1', 
#         'mapper': None, 
#         'reducer': None, 
#         'timeout': 30, 
#         "submissionId": "submissionId1"
#     }

# with open("./test/m_infinite_loop.py", "r") as f:
#     data['mapper'] = f.read()

# with open("./test/r_syntax_error.py", "r") as f:
#     data['reducer'] = f.read()

# for i in range(2):
#     teamId = data["teamId"]
#     teamId = teamId.split("_")
#     last_srn = int(teamId[-1])
#     last_srn += 1
#     last_srn = str(last_srn)
#     last_srn = "0" * (3-len(last_srn)) + last_srn
#     teamId[-1] = last_srn
#     data["teamId"] = "_".join(teamId)
#     print(f"Team ID : {data['teamId']}")
#     payload = [data]
#     payload = json.dumps(payload)
#     r = requests.post(url, data=payload)
#     print(f"{i+1} {r.status_code}")
#     res = json.loads(r.text)
#     print(res)
#     sleep(0.1)

# r = requests.get("http://127.0.0.1:9000/get-submissions", params={"prefetch_factor": 1})
# res = json.loads(r.text)
# print(res)
# print(r.status_code)

# r = requests.get("http://127.0.0.1:10001/queue-length")
# res = json.loads(r.text)
# print(res)
# print(r.status_code)

# r = requests.get("http://localhost:8088/ws/v1/cluster/apps")
# r = requests.get("http://localhost:33155/ws/v1/history/mapreduce/jobs")
# data = json.loads(r.text)
# print(data)
# pprint(data["apps"]["app"], indent=3)

# print(apps)