#!/usr/bin/env python3
import sys

def mapper():

    f = open("./m.py", "r").read()

    while(True):
        pass

    for line in sys.stdin:
        data=line.strip().split()
        print(f"{data[0]},{data[1]}")

    while(True):
        pass

if __name__ == '__main__':
    mapper()
