#!/usr/bin/env python3
import sys

if __name__ == "__main__":
    adj_nodes = []
    inp = (a.strip().split(',') for a in sys.stdin)
    prev_key, val = inp.__next__()
    adj_nodes.append(int(val))
    try:
        while True:
            key, val = inp.__next__()
            if(prev_key != key):
                print(f"{int(prev_key)}${adj_nodes}")
                with open(sys.argv[1], 'a') as f:
                    f.write(f"{prev_key},1\n")
                prev_key = key
                adj_nodes = []
            adj_nodes.append(int(val))
    except StopIteration:
        print(f"{int(prev_key)}${list(adj_nodes)}")
        with open(sys.argv[1], 'a') as f:
            f.write(f"{prev_key},1\n")