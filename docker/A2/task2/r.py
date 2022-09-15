#!/usr/bin/python3

import sys

rank = lambda x: 0.34 + 0.57 * x

prev_page = None

for line in sys.stdin:
    line = line.strip()
    try:
        from_page, page_contribution = line.split(",")
        from_page = int(from_page.strip())
        page_contribution = float(page_contribution.strip())
    except:
        continue

    if prev_page is None:
        prev_page = from_page
        prev_pagerank = rank(page_contribution)

    elif prev_page == from_page:
        #print(page_contribution)
        prev_pagerank += 0.57 * page_contribution

    else:
        print("{}, {:.2f}".format(prev_page, prev_pagerank))
        prev_page = from_page
        prev_pagerank = rank(page_contribution)

print("{}, {:.2f}".format(prev_page, prev_pagerank))