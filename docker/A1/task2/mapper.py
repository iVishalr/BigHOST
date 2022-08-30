#!/usr/bin/env python3
import sys
import json

queried_distances = float(sys.argv[1])
latitudes = float(sys.argv[2])
longitudes = float(sys.argv[3])

for line in sys.stdin:
    record = json.loads(line.lower().strip().replace("nan","NaN"))
    value1 = float(record["lat"])
    value2 = float(record["lon"])
    distance = pow(pow((value1-latitudes),2) + pow((value2-longitudes),2) ,0.5)
    if(distance <= queried_distances and value1!=None and value2!=None and 48.00 < float(record["humidity"])< 54.00 and 20.00 < float(record["temperature"]) < 24.00):
        print(record['timestamp'], 1)