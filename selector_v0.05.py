from collections import defaultdict
from typing import List, Dict, Iterable
from pathlib import Path
import numpy as np
import pandas as pd
import time
import pyarrow.parquet as pq


def skew_summary(sample_df, key_col="PULocationID"):
    """
    Returns dictionary with:
      --- total_rows: int
      --- distinct_keys: int
      --- skew_max_med: float (max / median of per-key counts)
      --- top1_share: float (fraction of rows in hottest key)
      --- per_key_counts: pandas.Series (counts sorted desc)
    """
    print(type(sample_df))
    total_rows = len(sample_df)
    
    # pandas.Series.value_counts() (or DataFrame column .value_counts()) returns a Series mapping each unique value to the number of times it appears
    counts = sample_df[key_col].value_counts().sort_values(ascending=False)
    distinct_keys = counts.size
    # Covert NumPy Scalar to float
    vals = counts.values.astype(float)

    max_count = vals[0]    # Picks the top
    median_count = float(np.median(vals)) if distinct_keys > 0 else 0.0    # Find the median of the array
    skew_max_med = max_count / max(1.0, median_count)
    top1_share = max_count / total_rows

    return {
        "total_rows": int(total_rows),
        "distinct_keys": int(distinct_keys),
        "skew_max_med": float(skew_max_med),
        "top1_share": float(top1_share),
        "per_key_counts": counts
    }


'''
CONSERVATVE
'''
def is_high_skew(skew_summary_result,
                 skew_threshold=10.0,   # skew_max_med threshold
                 top1_threshold=0.20):  # top1_share threshold
    """
    Decide high skew (True) or low skew (False) using the output of skew_summary()
    Rules (conservative):
      --- High skew if skew_max_med >= skew_threshold OR top1_share >= top1_threshold
      --- Otherwise low skew
    Returns boolean.
    """
    if skew_summary_result.get("total_rows") == 0:
        return False

    skew_max_med = skew_summary_result.get("skew_max_med")
    top1_share = skew_summary_result.get("top1_share")

    return (skew_max_med >= skew_threshold) or (top1_share >= top1_threshold)



"""
DataFrame processing of the tables --- testing instances
"""
time_start = time.time()
# (Optional) Load only needed columns to save memory
# This dataset is massive
yellow = pq.read_table(Path("yellow_tripdata_2025-01.parquet"),
                       columns=["PULocationID","fare_amount"]).to_pandas()    # Changed it to test with skews
green  = pq.read_table(Path("green_tripdata_2025-01.parquet"),
                       columns=["PULocationID","fare_amount"]).to_pandas()    # Changed it to test with non-skews - made 2nd columns same to just test

# (Optional) Clean + align types
yellow = yellow.dropna(subset=["PULocationID"]).astype({"PULocationID":"int64"})
green  = green.dropna(subset=["PULocationID"]).astype({"PULocationID":"int64"})

# (Optional) making this quicker for solo testing
yellow = yellow.sample(min(len(yellow), 50_000), random_state=0)
green  = green.sample(min(len(green), 50_000), random_state=1)

# Convert to records (list of dicts) for consistency
left_rows  = yellow.to_dict(orient="records")
right_rows = green.to_dict(orient="records")



"""
Calculate skew summary and pass it to the boolean function to determined if it skewed enough
Test: available mem = False
"""

def choose_join(available_memory=True):
    """
      * If data is too skewed ---> sort-merge
      * If skewed AND needs a lot of memory ---> sort-merge
      * If low skew ---> hash join
      * If low skew but too much memory (i.e., not enough memory) ---> sort-merge
    """
    memory_threshold = 2 * 1024**3      # 2 GiB

    print(len(yellow))
    skew_sum = skew_summary(green)
    is_skewed = is_high_skew(skew_sum)
    enough_memory = available_memory >= memory_threshold

    # Changed for testing
    enough_memory = True

    if is_skewed:
        print("It is in fact skewed according to metrics")
        return 'sort-merge'
    else:
        if enough_memory:
            return 'hash'
        else:
            return 'sort-merge'

print(choose_join())
time_end = time.time()
print("Time taken (s):", time_end - time_start)
