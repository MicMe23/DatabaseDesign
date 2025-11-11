from collections import defaultdict
from typing import List, Dict, Iterable
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import time

def sort_merge_inner (left, right, key):
    # Requirements :
    #  - left_rows / right_rows: lists of dicts
    #  - key exists in both sides; None keys don't match
    #  - duplicate keys produce the cross product of matches for now.
    #  returns: list of merged dicts

    if (not left) or (not right):
        return

    # make the dictionaries into lists with orders instead of sets. 
    list_left = list(left)
    list_right = list(right)

    # Sorting step
    #sorted_left = merge_sort(list_left, key)
    #sorted_right = merge_sort(list_right, key)

    sorted_left = sorted(list_left, key=lambda x: x.get(key))
    sorted_right = sorted(list_right, key=lambda x: x.get(key))
    output = []

    # Left Index, right index
    li = 0
    ri = 0
    
    # Infinite loop to be exited when something bad happens
    outer_loop = True
    while outer_loop:
        # While left key and right key aren't equal, skip them
        # skipping is done on the smaller value, assuming both must be sorted
        while not (sorted_left[li].get(key) == sorted_right[ri].get(key)):
            if sorted_left[li].get(key) < sorted_right[ri].get(key):
                if (li+1) < len(sorted_left):
                    li += 1
                else:
                    outer_loop = False
                    break
            else:
                if (ri+1) < len(sorted_right):
                    ri += 1
                else: 
                    outer_loop = False
                    break
                    
        if not outer_loop:
            break
        
        # Store the current left row in case it gets tied to multiple rows on the right
        mi = li
        curr_l = sorted_left[li]
        
        # Second forever loop from the algorithm
        while True:
            # While the keys are equal, merge rows and add to the output
            while sorted_left[li].get(key) == sorted_right[ri].get(key):
                combined = sorted_left[li].copy()

                for k, v in sorted_right[ri].items():
                    if not (k == key):
                        combined[k+".right"] = sorted_right[ri][k]
                
                output.append(combined)

                # Go to next left row, to check if it's also equal to right row
                li += 1
                
                # Only continue while left row actually exists
                if not(li < len(sorted_left)):
                    break
            
            # Index up the right row once if possible
            if (ri+1) < len(sorted_right):
                ri += 1
            else: 
                outer_loop = False
                break

            # If the old left row we saved matched the new right row, we need to go back and consider
            # the old left row and the new right row before moving onwards
            if curr_l.get(key) == sorted_right[ri].get(key):
                li = mi
            else:
                # If li isn't in the list then something broke big time
                if not (li < len(sorted_left)):
                    outer_loop = False
                    break
                else:
                    break

        if not outer_loop:
            break
    
    #output = {index: value for index, value in enumerate(output)}

    return output

def sort_merge_inner2(left, right, key):
    # Requirements :
    #  - left_rows / right_rows: lists of dicts
    #  - key exists in both sides; None keys don't match
    #  - duplicate keys produce the cross product of matches for now.
    #  returns: list of merged dicts

    # 2nd version

    if (not left) or (not right):
        return
 
    list_left = list(left)
    list_right = list(right)


    sorted_left = sorted(list_left, key=lambda x: x.get(key))
    sorted_right = sorted(list_right, key=lambda x: x.get(key))

    output = []
    li = 0
    ri = 0

    # logic is same for this phase
    while li < len(sorted_left) and ri < len(sorted_right):

        left_key = sorted_left[li][key]
        right_key = sorted_right[ri][key]

        if left_key < right_key:
            li += 1
        elif left_key > right_key:
            ri += 1
        else:
            # keys match → cross product block
            curr_key = left_key

            # collect all matching on left
            li_start = li
            while li < len(sorted_left) and sorted_left[li][key] == curr_key:
                li += 1

            # collect all matching on right
            ri_start = ri
            while ri < len(sorted_right) and sorted_right[ri][key] == curr_key:
                ri += 1

            # cross product (same as before)
            for l in range(li_start, li):
                for r in range(ri_start, ri):
                    combined = sorted_left[l].copy()
                    for k, v in sorted_right[r].items():
                        if k != key:
                            combined[k + ".right"] = v
                    output.append(combined)

    return output

def merge_sort (unsorted_list, key):
    if len(unsorted_list) <= 1:
        return unsorted_list
    
    unsorted_left = []
    unsorted_right = []
    for index, value in enumerate(unsorted_list):
        if index < (len(unsorted_list)/2):
            unsorted_left.append(value)
        else:
            unsorted_right.append(value)

    unsorted_left = merge_sort(unsorted_left, key)
    unsorted_right = merge_sort(unsorted_right, key)

    return merge(unsorted_left, unsorted_right, key)

def merge(lhs, rhs, key):
    result = []

    while lhs and rhs:
        if lhs[0].get(key) < rhs[0].get(key):
            result.append(lhs[0])
            lhs = lhs[1:]
        else:
            result.append(rhs[0])
            rhs = rhs[1:]

    while lhs:
        result.append(lhs[0])
        lhs = lhs[1:]
    while rhs:
        result.append(rhs[0])
        rhs = rhs[1:]
        
    return result
