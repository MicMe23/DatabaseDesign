
from collections import defaultdict
from typing import List, Dict, Iterable
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

def hash_join_inner(left, right, key):
    # Requirements :
    #  - left_rows / right_rows: lists of dicts
    #  - key exists in both sides; None keys don't match
    #  - duplicate keys produce the cross product of matches for now.
    #  returns: list of merged dicts
    if len(left) <= len(right):
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

# Load only needed cols
yellow = pq.read_table(Path("yellow_tripdata_2025-01.parquet"),
                       columns=["PULocationID","fare_amount"]).to_pandas()
green  = pq.read_table(Path("green_tripdata_2025-01.parquet"),
                       columns=["PULocationID","trip_distance"]).to_pandas()

# Clean + align types
yellow = yellow.dropna(subset=["PULocationID"]).astype({"PULocationID":"int64"})
green  = green.dropna(subset=["PULocationID"]).astype({"PULocationID":"int64"})

# (Optional) sample down while testing so it’s fast
yellow = yellow.sample(min(len(yellow), 50_000), random_state=0)
green  = green.sample(min(len(green), 50_000), random_state=1)

# Convert to records (list of dicts)
left_rows  = yellow.to_dict(orient="records")
right_rows = green.to_dict(orient="records")

joins = hash_join_inner(left_rows, right_rows, "PULocationID")
print("rows joined:", len(joins))
print(joins[:3])