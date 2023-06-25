#!/usr/bin/env python3
import sys
import hadoop

def mapper():

    f = open("./m.py", "r").read()

    for line in sys.stdin:
        data=line.strip().split()
        print(f"{data[0]},{data[1]}")

if __name__ == '__main__':
    mapper()
