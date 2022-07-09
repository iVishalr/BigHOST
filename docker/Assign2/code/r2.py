#!/usr/bin/env python3
import sys

if __name__ == "__main__":
    inp = (a.strip().split() for a in sys.stdin)
    prev_key, contribution = inp.__next__()
    rank = float(contribution)
    try:
        while True:
            key, contribution = inp.__next__()
            if(prev_key != key):
                rank *= 0.85
                print(f"{int(prev_key)},{rank + 0.15}")
                prev_key = key
                rank = float(contribution)
            else : rank += float(contribution)
    except StopIteration:
        rank = 0.15 + 0.85 * rank
        print(f"{int(prev_key)},{rank}")