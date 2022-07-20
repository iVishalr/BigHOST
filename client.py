import os
import json
import requests
from time import sleep

r = requests.get("http://127.0.0.1:9000/empty-queue")
res = json.loads(r.text)
print(res)
print(r.status_code)

url = "http://127.0.0.1:9000/submit-job"

data = {'team_id': 'BD_019_536_571_000', 
        'assignment_id': 'A1', 
        'mapper': None, 
        'reducer': None, 
        'timeout':120, 
        "task": "task1"
    }

with open("./test/m.py", "r") as f:
    data['mapper'] = f.read()

with open("./test/r.py", "r") as f:
    data['reducer'] = f.read()

for i in range(5):
    team_id = data["team_id"]
    team_id = team_id.split("_")
    last_srn = int(team_id[-1])
    last_srn += 1
    last_srn = str(last_srn)
    last_srn = "0" * (3-len(last_srn)) + last_srn
    team_id[-1] = last_srn
    data["team_id"] = "_".join(team_id)
    print(f"Team ID : {data['team_id']}")
    payload = [data]
    payload = json.dumps(payload)
    r = requests.post(url, data=payload)
    print(f"{i+1} {r.status_code}")
    res = json.loads(r.text)
    print(res)
    sleep(0.1)

data = {'team_id': 'BD_019_536_571_0015', 
        'assignment_id': 'A1', 
        'mapper': None, 
        'reducer': None, 
        'timeout':120, 
        "task": "task1"
    }

with open("./test/m_invalid.py", "r") as f:
    data['mapper'] = f.read()

with open("./test/r_invalid.py", "r") as f:
    data['reducer'] = f.read()

for i in range(10):
    team_id = data["team_id"]
    team_id = team_id.split("_")
    last_srn = int(team_id[-1])
    last_srn += 1
    last_srn = str(last_srn)
    last_srn = "0" * (3-len(last_srn)) + last_srn
    team_id[-1] = last_srn
    data["team_id"] = "_".join(team_id)
    print(f"Team ID : {data['team_id']}")
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