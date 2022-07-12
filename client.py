import os
import requests
import json

r = requests.get("http://127.0.0.1:10001/empty-queue")
res = json.loads(r.text)
print(res)
print(r.status_code)

url = "http://127.0.0.1:10001/submit-job"

data = {'team_id': 'BD_019_536_571_589', 
        'assignment_id': 'A1', 
        'mapper': None, 
        'reducer': None, 
        'timeout':120, 
        "task": "task1"
    }

with open("./test/m.py", "rb") as f:
    data['mapper'] = f.read()

with open("./test/r.py", "rb") as f:
    data['reducer'] = f.read()

for i in range(10000):
    data["task"] = data["task"][:-1] + str(i)
    r = requests.post(url, data=data)
    print(f"{i+1} {r.status_code}")
    res = json.loads(r.text)
    print(res)

r = requests.get("http://127.0.0.1:10001/get-job")
res = json.loads(r.text)
print(res)
print(r.status_code)