#!/usr/bin/env python3

import sys
import heapq

output_dir = sys.argv[1].strip()

prev_node = None
prev_node_outlink = list()
#unique = []
minHeap = []
to_nodes = []
hash_set = set()
with open(output_dir, "w") as w_file:
    for line in sys.stdin:
        line = line.strip()
        try:
            from_node, to_node = line.split()
            from_node = from_node.strip()
            to_node = to_node.strip()
        except:
            continue
        if from_node not in hash_set:
        	hash_set.add(from_node)
        	heapq.heappush(minHeap, from_node)
        if to_node not in hash_set:
        	hash_set.add(to_node)
        	heapq.heappush(minHeap, to_node)
        if prev_node is None:
            prev_node = from_node
            to_nodes.append(from_node)
            prev_node_outlink.append(int(to_node))

        elif prev_node == from_node:
            prev_node_outlink.append(int(to_node))

        else:
            to_nodes.append(from_node)
            while len(minHeap)>0 and prev_node > minHeap[0]:
            	if minHeap[0] not in to_nodes:
            		print(f"{minHeap[0]}\t{[]}")
            		w_file.write(f"{minHeap[0]}, 1\n")
            		to_nodes.append(minHeap[0])
            	poppped = heapq.heappop(minHeap) 
            	hash_set.remove(poppped)
            if len(minHeap)>0:  	
            	heapq.heappop(minHeap)    	
            print(f"{prev_node}\t{prev_node_outlink}")
            w_file.write(f"{prev_node}, 1\n")
            prev_node = from_node
            prev_node_outlink.clear()
            prev_node_outlink.append(int(to_node))
    if prev_node != None:
	    print(f"{prev_node}\t{prev_node_outlink}")
	    w_file.write(f"{prev_node}, 1\n")
	    while len(minHeap)>0:
		    if minHeap[0] not in to_nodes:
		    	print(f"{minHeap[0]}\t{[]}")
		    	w_file.write(f"{minHeap[0]}, 1\n")
		    	to_nodes.append(minHeap[0])
		    poppped = heapq.heappop(minHeap) 
		    hash_set.remove(poppped)
