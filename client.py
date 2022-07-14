import os
import json
import requests
from time import sleep

r = requests.get("http://127.0.0.1:9000/empty-queue")
res = json.loads(r.text)
print(res)
print(r.status_code)

url = "http://127.0.0.1:9000/submit-job"

data = {'team_id': 'BD_019_536_571_589', 
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

data = [data]
data = json.dumps(data)

for i in range(5):
    # data["task"] = data["task"][:-1] + str(i)
    r = requests.post(url, data=data)
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