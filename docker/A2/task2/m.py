#!/usr/bin/python3

import sys
import json
pagerank = dict()


def similarity(A, B):
    sum = 0
    a = 0
    b = 0
    for i in range(len(A)):
        a += A[i]**2
        b += B[i]**2
        sum += A[i]*B[i]
    return sum/(a + b - (sum))


with open(sys.argv[1].strip()) as w_file:
    lines = w_file.read().strip().split("\n")
    for line in lines:
        try:
            page, rank = line.split(",")
        except:
            continue
        pagerank[int(page.strip())] = float(rank.strip())

pe = {}
with open(sys.argv[2].strip()) as page_embeddings:
    pe = json.load(page_embeddings)

for line in sys.stdin:
    line = line.strip()
    try:
        from_node, to_nodes = line.split("\t")
        from_node = int(from_node.strip())
        to_nodes = json.loads(to_nodes.strip())
    except:
        continue
    num_outgoing_links = len(to_nodes)
    from_node_pagerank = pagerank[from_node]
    from_node_contribution = from_node_pagerank * (1 / num_outgoing_links)
    print(f"{from_node},0")

    for node in to_nodes:
        if node in pagerank.keys():
            page_emb = similarity(pe[str(from_node)], pe[str(node)])
            print(f"{node},{from_node_contribution*page_emb}")