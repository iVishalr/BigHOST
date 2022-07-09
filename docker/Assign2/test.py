import numpy as np
import json as json
with open('./datasets/embedding_1percent.json',) as f:
    s = json.load(f)
print(s[:10])
a = {
    "1": [
        0.032086015,
        0.108658746,
        0.34455177,
        0.54974973,
        -0.89134425
    ],
    "3": [
        -0.09123115,
        0.2637072,
        0.47960255,
        0.3426702,
        -0.9511681
    ],
    "4": [
        0.18350898,
        0.09125652,
        0.19741566,
        0.54505724,
        -0.91121626
    ],
    "2": [
        -6.809274e-05,
        0.19512779,
        0.31758854,
        0.24154831,
        -1.027901
    ]
}
# print(a['3'])
import math
print( sum([a['1'][i] * a['2'][i] for i in range(len(a['2']))]) / (math.sqrt(sum([i ** 2 for i in a['1']])) * math.sqrt(sum([i ** 2 for i in a['1']]))))
print(np.dot(np.array(a['1']).T,a['2']))
