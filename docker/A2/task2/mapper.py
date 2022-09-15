#!/usr/bin/env python3
import sys
import json
import math

def similarity(p,q,cache):
    norm_p = 0
    norm_q = 0
    dot = 0
    bound = len(p)-4+1
    i = 0
    if cache == '':
        while i<bound:
            norm_p += p[i]**2 + p[i+1]**2 + p[i+2]**2 + p[i+3]**2
            norm_q += q[i]**2 + q[i+1]**2 + q[i+2]**2 + q[i+3]**2
            dot += p[i]*q[i] + p[i+1]*q[i+1] + p[i+2]*q[i+2] + p[i+3]*q[i+3]
            i+=4
        while i<len(p):
            norm_p+=p[i]**2
            norm_q+=q[i]**2
            dot += p[i]*q[i]
            i+=1
        cache = norm_p
    else:
        while i<bound:
            norm_q += q[i]**2 + q[i+1]**2 + q[i+2]**2 + q[i+3]**2
            dot += p[i]*q[i] + p[i+1]*q[i+1] + p[i+2]*q[i+2] + p[i+3]*q[i+3]
            i+=4
        while i<len(p):
            norm_q+=q[i]**2
            dot += p[i]*q[i]
            i+=1
    
    return cache, dot/(cache + norm_q - dot) 

def mapper():
    w_path = sys.argv[1]
    emb_path = sys.argv[2]
    f_w = open(w_path,'r')
    
    w_ = f_w.read().split("\n")
    w = dict()
    for item in w_[:-1]:
        key,val = item.split(",")
        w[key] = val

    # print(v)
    w_ = []
    f_w.close()
    f_emb_json = open(emb_path,'r')
    emb_data = json.loads(f_emb_json.read())
    f_emb_json.close()

    for line_adj in sys.stdin:
        node_id,adj_list = line_adj.strip().split("\t")
        adj_list = adj_list[1:-1].split(", ")
        
        m = float(w[node_id])/len(adj_list)
        contrib = 0
        print(f"{node_id},0")
        cache = ''
        for i in adj_list:
            cache,contrib = similarity(emb_data[str(node_id)],emb_data[i],cache)
            print(f"{i},{m*contrib}")

if __name__ == '__main__':
    mapper()