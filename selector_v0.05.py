from collections import defaultdict
from typing import List, Dict, Iterable
from pathlib import Path
import numpy as np
import pandas as pd
import time
import pyarrow.parquet as pq
import sort_merge
#import Hash_join
import Hash_join2


def skew_summary(sample_df, key_col="PULocationID"):    # Default - intial tables' "PULocationID" as join key
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
    
    # --- Returns a Series mapping each unique value to the number of times it appears and sorts in descending order
    counts = sample_df[key_col].value_counts().sort_values(ascending=False)
    distinct_keys = counts.size
    # Covert NumPy Scalar to float
    vals = counts.values.astype(float)

    max_count = vals[0]    # Picks the top - this is the hottest key's frequency count - the highest occurence key
    median_count = np.median(vals)    # Find the median of the array
    skew_max_med = max_count / median_count    # looks at the how maximum-freq key is positoned with respect to the median-count key
    top1_share = max_count / total_rows    # what percent of the hottest key

    return {
        "total_rows": int(total_rows),
        "distinct_keys": int(distinct_keys),
        "skew_max_med": float(skew_max_med),
        "top1_share": float(top1_share),
        "per_key_counts": counts    # for testing I have printed this for visually checking for a long, asymmetric tail
    }

def skew_summary2(df, key_col="PULocationID"):
    counts = df[key_col].value_counts(dropna=False).sort_values(ascending=False)
    vals = counts.to_numpy(dtype=float)
    total = vals.sum()
    if len(vals) == 0 or total == 0:
        return {
            "total_rows": 0,
            "distinct_keys": 0,
            "skew_max_med": 1.0,
            "top1_share": 0.0,
            "per_key_counts": counts
        }
    max_count = vals[0]
    median_count = np.median(vals)
    skew_max_med = float(max_count / median_count) if median_count > 0 else float('inf')
    top1_share = float(max_count / total)
    return {
        "total_rows": int(total),
        "distinct_keys": int(counts.size),
        "skew_max_med": float(skew_max_med),
        "top1_share": float(top1_share),
        "per_key_counts": counts
    }

def join_hotkey_share(left_counts: pd.Series, right_counts: pd.Series) -> float:
    """
    Estimate the JOIN output is on one key
    hotkey_share = max_k (L[k]*R[k]) / sum_k (L[k]*R[k])
    """
    # align on common keys only
    common = left_counts.index.intersection(right_counts.index)
    if common.empty:
        return 0.0
    lc = left_counts.loc[common].to_numpy(dtype=float)
    rc = right_counts.loc[common].to_numpy(dtype=float)
    pairwise = lc * rc
    total_pairs = pairwise.sum()
    if total_pairs <= 0:
        return 0.0
    return float(pairwise.max() / total_pairs)

def is_high_skew_joint(left_sum, right_sum, hot_share,
                       hot_hi: float = 0.60,
                       hot_lo: float = 0.30,
                       use_gray_guard: bool = False):
    """
    Decide 'high skew' based on JOIN-AWARE concentration only (temporarily)
      hot_share = max_k L[k]*R[k] / sum_k L[k]*R[k}
    Rules:
      - hot_share >= hot_hi  -> True  (skewed: prefer sort-merge)
      - hot_share <= hot_lo  -> False (not skewed: prefer hash)
    """
    print({"hot_share": hot_share, "hot_hi": hot_hi, "hot_lo": hot_lo})
    if hot_share >= hot_hi:
        return True
    if hot_share <= hot_lo:
        return False

    if not use_gray_guard:
        return False

    #(rarely fires)
    per_side_extreme = (
        (left_sum["top1_share"] >= 0.75 or left_sum["skew_max_med"] >= 80) and
        (right_sum["top1_share"] >= 0.75 or right_sum["skew_max_med"] >= 80)
    )
    return per_side_extreme

def choose_join2(yellow_df: pd.DataFrame, green_df: pd.DataFrame, key_col="PULocationID",
                available_memory_bytes=None, memory_threshold_bytes=2 * 1024**3):
    # summarize both sides
    y_sum = skew_summary(yellow_df, key_col)
    g_sum = skew_summary(green_df, key_col)

    # compute hot-key share of the *join output*
    hot_share = join_hotkey_share(y_sum["per_key_counts"], g_sum["per_key_counts"])

    # make decision based off of hot key
    high_skew = is_high_skew_joint(y_sum, g_sum, hot_share, hot_hi=0.60, hot_lo=0.30)

    # memory guard (optional for now until fleshed out)
    #enough_mem = True if available_memory_bytes is None else (available_memory_bytes >= memory_threshold_bytes)

    if high_skew:
        return "sort-merge"
    else:
        return "hash"
    
'''
CONSERVATIVE
--- We run sample tests on several tables as we go - to determine those thresholds (kind of averaged out) - we will eventually have more
tested numbers which is closer to the real-world thresholds
--- This function is to be tested with to avoid changing more of the gorund rules for the actual skew estimator above

'''
def is_high_skew(skew_summary_result,
                 skew_threshold=10.0,   # skew_max_med threshold
                 top1_threshold=0.20):  # top1_share threshold i.e. the first is almost 20% of the unique keys - change it to 50% - eventually will test
                                                      # through several quantiles (like 25%, 50%, 75%) and also for the top-k (each)
    """
    Decide high skew (True) or low skew (False) using the output of skew_summary()
    Rules (conservative):
      --- High skew if skew_max_med >= skew_threshold OR top1_share >= top1_threshold
      --- Otherwise low skew
    Returns boolean.
    """
    print(skew_summary_result.get("per_key_counts"))
    skew_max_med = skew_summary_result.get("skew_max_med")
    top1_share = skew_summary_result.get("top1_share")

    return (skew_max_med >= skew_threshold) or (top1_share >= top1_threshold)



"""
DataFrame processing of the tables --- testing instances
"""
# (Optional) Load only needed columns to save memory
# This dataset is massive
yellow = pq.read_table(Path("./test_files/2skewed_150k_rows_L.parquet"),
                       columns=["PULocationID","fare_amount"]).to_pandas()    # Changed it to test with skews
green  = pq.read_table(Path("./test_files/2skewed_150k_rows_R.parquet"),
                       columns=["PULocationID","fare_amount"]).to_pandas()    # Changed it to test with non-skews - made 2nd columns same to just test

# (Optional) Clean + align types
#yellow = yellow.dropna(subset=["PULocationID"]).astype({"PULocationID":"int64"})
#green  = green.dropna(subset=["PULocationID"]).astype({"PULocationID":"int64"})

# (Optional) making this quicker for solo testing
yellow = yellow.sample(min(len(yellow), 20000), random_state=0)
green  = green.sample(min(len(green), 20000), random_state=0)

#yellow = yellow.sort_values("PULocationID")
#green  = green.sort_values("PULocationID")

time_start = time.time()


"""
Calculate skew summary and pass it to the boolean function to determined if it skewed enough
Test: available mem = True
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
    # enough_memory = available_memory >= memory_threshold

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
        
#choice = choose_join2(yellow, green)

choice = 'hash'   # For testing purposes
print(len(yellow))
print(len(green))

if choice == 'hash':
    # print(type(yellow))       # comes in as Dataframe
    left_rows  = yellow.to_dict(orient="records")
    right_rows = green.to_dict(orient="records")
    # print(type(left_rows))        # it's a list of dictionaries 
    joins = Hash_join2.hash_join_inner(left_rows, right_rows, "PULocationID")
else:
    left_rows  = yellow.to_dict(orient="records")
    right_rows = green.to_dict(orient="records")
    joins = sort_merge.sort_merge_inner3(left_rows, right_rows, "PULocationID")

time_end = time.time()
print(choice)
print("Time taken (s): (main)", time_end - time_start)
print("rows joined: (main)", len(joins))
print(joins[:3])
