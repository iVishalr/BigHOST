# just testing for now

import json
import requests
from pprint import pprint

url = "http://localhost:19888/ws/v1/history/mapreduce/jobs"

r = requests.get(url)
data = json.loads(r.text)
jobs = data["jobs"]["job"]

for job in jobs:
    print(job)
    print(type(job))

