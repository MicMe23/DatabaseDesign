# from collections import defaultdict
# from typing import List, Dict, Iterable
# from pathlib import Path
# import pandas as pd
# import pyarrow.parquet as pq
# import time

from collections import defaultdict
from typing import List, Dict, Iterable
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import time
import os
import json
import tempfile
import psutil
import gc
from collections import defaultdict

# def hash_join_inner(left, right, key):
#     # Requirements :
#     #  - left_rows / right_rows: lists of dicts
#     #  - duplicate keys produce the cross product of matches for now.
#     #  returns: list of merged dicts
#     if len(left) <= len(right):
#         build, probe = (left, right)
#     else:
#         build, probe = (right, left)

#     # assign to know which side is which when merging
#     build_is_left = build is left

#      # Build: key -> list of rows
#     H = defaultdict(list)
#     for r in build:
#         H[r[key]].append(r)
    
#     # Probe
#     out = []
#     for r in probe:
#         k = r[key]
#         if k in H:
#             for b in H[k]:
#                 if build_is_left:
#                     joined = {**b, **r}     # left fields then right fields
#                 else:
#                     joined = {**r, **b}     # right fields then left fields
#                 out.append(joined)
#     return out

#     # modify probe for differnt joins


start = time.time()

def hash_join_inner(left, right, key, mem_limit_bytes=4 * 1024**3, spill_buckets=64, batch_flush=10000):
    '''
        Build-phase monitor and then spill hash join. Returns path to NDJSON output file (in tmp folder)
        --- build_rows, probe_rows: coming in as list of dict values
        --- mem_limit_bytes: threshold (RSS) to trigger spill event
        --- spill_buckets: number of spill files (hash partitioning)
        --- batch_flush: how many joined rows to buffer before writing to disk
    '''
    
    # Spin this up as a separate process - a process self-contained and standalone in itself
    # largely to keep track of memory
    proc = psutil.Process()
    spill_dir = tempfile.mkdtemp(prefix="hash_spill_")
    spill_paths = {}            # bucket ---> file path for spilled build rows

    if len(left) <= len(right):
        build_rows, probe_rows = (left, right)
    else:
        build_rows, probe_rows = (right, left)

    in_memory_H = defaultdict(list) # key ---> list[rows] (kept in memory)
    # Build phase with spill
    for r in build_rows:
        in_memory_H[r[key]].append(r)
        
        if proc.memory_info().rss > mem_limit_bytes:
            # k ---> key and b ---> bucket_number
            for k, rows in in_memory_H.items():
                b = (abs(hash(k)) % spill_buckets)
                # print(b)
                p = spill_paths.setdefault(b, os.path.join(spill_dir, f"bucket_{b}.ndjson"))
                with open(p, "a", encoding="utf-8") as f:
                    for row in rows:
                        f.write(json.dumps(row, ensure_ascii=False) + "\n")
                
            for k in list(in_memory_H.keys()):
                rows = in_memory_H.pop(k)
                if not rows:
                    continue
            rows.clear()
            gc.collect()
            #in_memory_H.clear()

    end = time.time()

    # after building we may have some spill files and some rows in the in_memory_H
    # out_path is the path where the joined rows are written - one large file for now with disk spillage rows
    out_path = os.path.join(spill_dir, "join_out.ndjson")
    out_buf = []
    
    # function to write to the out_path file and clear out the out_buffer
    def flush_out_buf():
        nonlocal out_buf
        if not out_buf:
            return
        with open(out_path, "a", encoding="utf-8") as f:
            for row in out_buf:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        out_buf = []
        
    # probe phase for each probe row, first check in-memory H, otherwise check spill file for that bucket
    probe_bucket_paths = {}  # small helper to optionally bucket probes on-disk - here we stream directly
    joined_count = 0

    def flush_if_needed():
        # flush by buffer size or memory pressure
        if len(out_buf) >= batch_flush or proc.memory_info().rss > mem_limit_bytes:
            flush_out_buf()

    # Stream probe rows directly - first memory and then disk
    for r in probe_rows:
        k = r[key]
        # matched = False
        
        if k in in_memory_H:
            for bld in in_memory_H[k]:
                out_buf.append({**bld, **r})
                joined_count += 1
            # matched = True
            flush_if_needed()
            
        # check the spill files
        b = (abs(hash(k)) % spill_buckets)
        build_spill_p = spill_paths.get(b)
        if build_spill_p and os.path.exists(build_spill_p):
            with open(build_spill_p, "r", encoding="utf-8") as f:
                for line in f:
                    bld = json.loads(line)
                    if bld.get(key) == k:
                        out_buf.append({**bld, **r})
                        joined_count += 1
                        flush_if_needed()
                        
    flush_out_buf()
    print("joined rows:", joined_count)
    print("Join completed successfully:", out_path)
    return out_path, spill_dir
