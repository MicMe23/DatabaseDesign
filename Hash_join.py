
from collections import defaultdict
from typing import List, Dict, Iterable

def hash_join_inner(left, right, key):
    # Requirements :
    #  - left_rows / right_rows: lists of dicts
    #  - key exists in both sides; None keys don't match
    #  - duplicate keys produce the cross product of matches for now.
    #  returns: list of merged dicts
    if len(left) > len(right):
        build, probe = (left, right)
    else:
        build, probe = (right, left)

    # assign to know which side is which when merging
    build_is_left = build is left  

     # Build: key -> list of rows
    H = defaultdict(list)
    for r in build:
        H[r[key]].append(r)
    
    # Probe
    out = []
    for r in probe:
        k = r[key]
        if k in H:
            for b in H[k]:
                if build_is_left:
                    joined = {**b, **r}     # left fields then right fields
                else:
                    joined = {**r, **b}     # right fields then left fields
                out.append(joined)
    return out

L = [{"id":1,"gender":"M"}, {"id":1,"gender":"F"}, {"id":2,"gender":"M"}]
R = [{"id":1,"age":10}, {"id":2,"age":20}, {"id":3,"age":30}]
print(hash_join_inner(L, R, "id"))