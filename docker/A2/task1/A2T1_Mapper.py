#!/usr/bin/env python3
import sys

l = []
for line in sys.stdin:
    line = line.strip()
    l.append(line)

for line in l:
    try:
        from_node, to_node = line.split()
        from_node = from_node.strip()
        to_node = to_node.strip()
    except:
        continue
    print(f"{from_node}\t{to_node}")
