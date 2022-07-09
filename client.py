import os
import requests
import json

url = "http://localhost:10000/run_job"

data = {'team_id': 'BD_019_536_571_589', 'assignment_id': 'A1', 'mapper': None, 'reducer': None, 'timeout':120}

with open("./m.py", "rb") as f:
    data['mapper'] = f.read()

with open("./r.py", "rb") as f:
    data['reducer'] = f.read()

r = requests.post(url, data=data)

print(r.text)
print(r.status_code)