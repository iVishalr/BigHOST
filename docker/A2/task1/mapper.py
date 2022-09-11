#!/usr/bin/env python3
import sys

def mapper():
    for line in sys.stdin:
        if line[0]!='#':
            data=line.strip().split()
            print(f"{data[0]},{data[1]}")

if __name__ == '__main__':
    mapper()