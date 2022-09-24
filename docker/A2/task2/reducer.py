#!/usr/bin/env python3
import sys

def reducer():
    current_node_id = None
    sum_ = 0.0
    for line in sys.stdin:
        node_id,ind_contrib = line.strip().split(",")
        ind_contrib = float(ind_contrib)

        if current_node_id == None:
            current_node_id = node_id
            sum_ = ind_contrib
            continue

        if node_id == current_node_id:
            sum_ += ind_contrib
        
        else:
            print(f"{current_node_id},{0.34+0.57*(sum_):.2f}")
            current_node_id = node_id
            sum_ = ind_contrib

    print(f"{current_node_id},{0.34+0.57*(sum_):.2f}")
    

if __name__ == '__main__':
    reducer()  