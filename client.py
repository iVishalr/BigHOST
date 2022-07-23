import os
import json
import requests
from time import sleep

r = requests.get("http://127.0.0.1:9000/empty-queue")
res = json.loads(r.text)
print(res)
print(r.status_code)

url = "http://127.0.0.1:9000/sanity-check"

data = {'teamId': 'BD_019_536_571_000', 
        'assignmentId': 'A1', 
        'mapper': None, 
        'reducer': None, 
        'timeout':30, 
        "submissionId": "submissionId1"
    }

with open("./test/m.py", "r") as f:
    data['mapper'] = f.read()

with open("./test/r.py", "r") as f:
    data['reducer'] = f.read()

for i in range(4):
    teamId = data["teamId"]
    teamId = teamId.split("_")
    last_srn = int(teamId[-1])
    last_srn += 1
    last_srn = str(last_srn)
    last_srn = "0" * (3-len(last_srn)) + last_srn
    teamId[-1] = last_srn
    data["teamId"] = "_".join(teamId)
    print(f"Team ID : {data['teamId']}")
    payload = [data]
    payload = json.dumps(payload)
    r = requests.post(url, data=payload)
    print(f"{i+1} {r.status_code}")
    res = json.loads(r.text)
    print(res)
    sleep(0.1)

data = {'teamId': 'BD_019_536_571_004', 
        'assignmentId': 'A1', 
        'mapper': None, 
        'reducer': None, 
        'timeout': 30, 
        "submissionId": "submissionId1"
    }

with open("./test/m.py", "r") as f:
    data['mapper'] = f.read()

with open("./test/r_infinite_loop.py", "r") as f:
    data['reducer'] = f.read()

for i in range(4):
    teamId = data["teamId"]
    teamId = teamId.split("_")
    last_srn = int(teamId[-1])
    last_srn += 1
    last_srn = str(last_srn)
    last_srn = "0" * (3-len(last_srn)) + last_srn
    teamId[-1] = last_srn
    data["teamId"] = "_".join(teamId)
    print(f"Team ID : {data['teamId']}")
    payload = [data]
    payload = json.dumps(payload)
    r = requests.post(url, data=payload)
    print(f"{i+1} {r.status_code}")
    res = json.loads(r.text)
    print(res)
    sleep(0.1)

data = {'teamId': 'BD_019_536_571_008', 
        'assignmentId': 'A1', 
        'mapper': None, 
        'reducer': None, 
        'timeout': 30, 
        "submissionId": "submissionId1"
    }

with open("./test/m_invalid.py", "r") as f:
    data['mapper'] = f.read()

with open("./test/r_invalid.py", "r") as f:
    data['reducer'] = f.read()

for i in range(10):
    teamId = data["teamId"]
    teamId = teamId.split("_")
    last_srn = int(teamId[-1])
    last_srn += 1
    last_srn = str(last_srn)
    last_srn = "0" * (3-len(last_srn)) + last_srn
    teamId[-1] = last_srn
    data["teamId"] = "_".join(teamId)
    print(f"Team ID : {data['teamId']}")
    payload = [data]
    payload = json.dumps(payload)
    r = requests.post(url, data=payload)
    print(f"{i+1} {r.status_code}")
    res = json.loads(r.text)
    print(res)
    sleep(0.1)

data = {'teamId': 'BD_019_536_571_018', 
        'assignmentId': 'A1', 
        'mapper': None, 
        'reducer': None, 
        'timeout': 30, 
        "submissionId": "submissionId1"
    }

with open("./test/m.py", "r") as f:
    data['mapper'] = f.read()

with open("./test/r_syntax_error.py", "r") as f:
    data['reducer'] = f.read()

for i in range(5):
    teamId = data["teamId"]
    teamId = teamId.split("_")
    last_srn = int(teamId[-1])
    last_srn += 1
    last_srn = str(last_srn)
    last_srn = "0" * (3-len(last_srn)) + last_srn
    teamId[-1] = last_srn
    data["teamId"] = "_".join(teamId)
    print(f"Team ID : {data['teamId']}")
    payload = [data]
    payload = json.dumps(payload)
    r = requests.post(url, data=payload)
    print(f"{i+1} {r.status_code}")
    res = json.loads(r.text)
    print(res)
    sleep(0.1)

data = {'teamId': 'BD_019_536_571_023', 
        'assignmentId': 'A1', 
        'mapper': None, 
        'reducer': None, 
        'timeout': 30, 
        "submissionId": "submissionId1"
    }

with open("./test/m_infinite_loop.py", "r") as f:
    data['mapper'] = f.read()

with open("./test/r_syntax_error.py", "r") as f:
    data['reducer'] = f.read()

for i in range(2):
    teamId = data["teamId"]
    teamId = teamId.split("_")
    last_srn = int(teamId[-1])
    last_srn += 1
    last_srn = str(last_srn)
    last_srn = "0" * (3-len(last_srn)) + last_srn
    teamId[-1] = last_srn
    data["teamId"] = "_".join(teamId)
    print(f"Team ID : {data['teamId']}")
    payload = [data]
    payload = json.dumps(payload)
    r = requests.post(url, data=payload)
    print(f"{i+1} {r.status_code}")
    res = json.loads(r.text)
    print(res)
    sleep(0.1)

# r = requests.get("http://127.0.0.1:9000/get-submissions", params={"prefetch_factor": 1})
# res = json.loads(r.text)
# print(res)
# print(r.status_code)

r = requests.get("http://127.0.0.1:10001/queue-length")
res = json.loads(r.text)
print(res)
print(r.status_code)