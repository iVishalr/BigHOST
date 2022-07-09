#!/usr/bin/env python3
import sys

if __name__ == "__main__":
    for record in sys.stdin:
        record = record.strip().split('\t')
        print(f"{int(record[0])},{ int(record[1])}")