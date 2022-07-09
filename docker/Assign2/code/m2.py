#!/usr/bin/env python3
import sys
import json
import math

if __name__ == "__main__":
    with open(sys.argv[2],) as f:
        s = json.load(f)
    v = {}
    with open(sys.argv[1], 'r') as f:
        lines = f.readlines()
        lines.pop()
        for line in lines:
            v[line.strip().split(',')[0]] = int(line.strip().split(',')[1])
    for record in sys.stdin:
        p_node, q_nodes = record.strip().split('$')
        p_node = int(p_node)
        # print(q_nodes)
        q_nodes = q_nodes.lstrip('[').rstrip(']').split(', ')
        no_q_nodes = len(q_nodes)
        # s = {i: list(s1[str(i)]) for i in (q_nodes + [p_node])}
        norm_p_node = math.sqrt(sum([float(i) ** 2 for i in s[p_node]]))
        for q_node in q_nodes:
            print(f"{int(q_node)} {float((v[str(p_node)] * sum([s[p_node][i] * s[q_node][i] for i in range(len(s[q_node]))]) / (norm_p_node * math.sqrt(sum([float(i) ** 2 for i in s[q_node]])))) / no_q_nodes)}")
    for i in s:
        print(f"{int(i)} {float(0)}")

    