#!/usr/bin/env python3
import sys

def reducer():
    path = sys.argv[1]
    f = open(path,"w+")
    prev_key = None
    current_key = None
    s = ''
    for line in sys.stdin:
        key,val = line.strip().split(',')
        if current_key == None:
            current_key = key
            s = f"{key}\t[{val}"
            f.write(f"{key},1\n")
            continue
        if key == current_key:
            s += f", {val}"
        else:
            print(s+"]")
            current_key = key
            s = f"{key}\t[{val}"
            f.write(f"{key},1\n")
    print(s+"]")
    f.close()

if __name__ == '__main__':
    reducer()