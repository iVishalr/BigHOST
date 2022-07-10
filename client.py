import os
import requests
import json

url = "http://localhost:10000/run_job"

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

r = requests.post(url, data=data)

res = json.loads(r.text)
print(res["logs"])
print(r.status_code)