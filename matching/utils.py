# -*- coding: utf-8 -*- 

import random
import numpy as np
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt
from collections import defaultdict

'''
    -------------------------------------------------
    ---------- SUMMARIES AND VISUALIZATION ----------
    ------------------------------------------------- 
'''

def split_list(lst, nparts):
    '''
        Stack Overflow link:
        https://stackoverflow.com/questions/2130016/splitting-a-list-into-n-parts-of-approximately-equal-length/37414115#37414115
    '''
    n = len(lst)
    return [lst[i * (n // nparts) + min(i, n % nparts):(i+1) * (n // nparts) + min(i+1, n % nparts)] for i in range(nparts)]

'''
    -------------------------------------------------
    ----------------- OPERATIONAL -------------------
    ------------------------------------------------- 
'''

def format_annotation(list_of_pairs, left_df, right_df, agg_df, left_cols, right_cols, batchsize=5000):
    '''
    
    '''
    count = 0
    object_list = []

    # -- for memory efficiency purposes, process the list of pairs by batches
    splitted_list_of_pairs = [ list_of_pairs[x:x+batchsize] for x in range(0, len(list_of_pairs), batchsize) ]
    for current_list_of_pairs in splitted_list_of_pairs:
        
        # -- separate the left and right indexes 
        left_indexes = [ pair[0] for pair in current_list_of_pairs ]
        right_indexes = [ pair[1] for pair in current_list_of_pairs ]
        #classification = [ pair[2] for pair in current_list_of_pairs ] # maybe not needed
        
        # -- signal for linkage
        if right_df is not None:
            left_records, right_records = left_df[left_cols].loc[left_indexes].to_dict(orient='records'), right_df[right_cols].loc[right_indexes].to_dict(orient='records')
        # -- signal for deduplication
        else:
            left_records, right_records = left_df[left_cols].loc[left_indexes].to_dict(orient='records'), left_df[left_cols].loc[right_indexes].to_dict(orient='records')

        agg = None 
        if agg_df is not None:
            agg = agg_df.loc[current_list_of_pairs].to_dict(orient='records')

        for subindex in range(len(current_list_of_pairs)):
            count+=1
            pair_element = {"cod": count,
                            "classification": '',
                            "a": left_records[subindex], "b": right_records[subindex], 
                            "identifiers": {"a": left_indexes[subindex], "b": right_indexes[subindex]},
                            "agg": agg[subindex] }
            
            object_list.append(pair_element)
    return object_list


def find_root(index, ptr):
    dummy = index
    while ptr[dummy]>=0:
        dummy = ptr[dummy]
    return dummy

# --> Deduplication
def deduple_grouping(pairs):
    '''
        Perform grouping of matched records into a final schema file, identifying unique individuals.

        After deduplication, we use 'pairs'(a list of tuples corresponding to each pair of
        matched records), to create a hash/dictionary structure associating a given record to all its matched
        records (same person). Dictionary contains a list of matched records.

        Args:
        -----
            pairs:
                pandas.DataFrame. A dataframe containing at least two columns representing
                the matched pairs of unique records: "left" and "right".  
        Return:
        -------
            matched_records:
                collections.defaultdict. 
    '''
    left_nots = pairs["left_id"].tolist()
    right_nots = pairs["right_id"].tolist()

    # --> Define data structure of trees to aggregate several matched files through transitive relations. 
    # ----> Unique records in 'pairs'
    unique_nots = np.unique(left_nots+right_nots) 
    # ----> Tree positions of each unique record of 'pairs' (based on the union/find algorithms)
    ptr = np.zeros(unique_nots.shape[0], int) - 1
    # ----> Associate each record to its position in 'ptr' (hash)
    ptr_index = dict( zip(unique_nots, np.arange(0, unique_nots.shape[0], 1)) )

    # --> Aggregate matched records associating each unique person to a root index. 
    for index in tqdm(range(len(left_nots))):
        left = left_nots[index]
        right = right_nots[index]
        left_index = ptr_index[left]
        right_index = ptr_index[right]
    
        left_root = find_root(left_index, ptr)
        right_root = find_root(right_index, ptr)
    
        if left_root==right_root:
            continue
    
        bigger_root, bigger_index = left_root, left_index
        smaller_root, smaller_index = right_root, right_index
        if ptr[right_root]<ptr[left_root]:
            bigger_root, bigger_index = right_root, right_index
            smaller_root, smaller_index = left_root, left_index
    
        ptr[bigger_root] += ptr[smaller_root]
        ptr[smaller_root] = bigger_root
        
    matched_records = defaultdict(lambda: [])
    for index in range(len(ptr)):
        local_not = unique_nots[index]
        root_not = unique_nots[find_root(index, ptr)]
        if root_not!=local_not:
            matched_records[root_not].append(local_not)

    return matched_records

# --> Linkage
def linkage_grouping(pairs):
    '''
        Perform grouping of matched records into a final schema file, identifying unique individuals.

        After the linkage between two databases, we use 'pairs' (containing the ID of the matched 
        records) to create a hash/dictionary structure associating a given record in one database 
        to all its matched records in the other. Dictionary contains a list of matched records.

        Args:
        -----
            pairs:
                pandas.DataFrame. A dataframe containing at least two columns representing
                the matched pairs of unique records: "left" and "right".  
        Return:
        -------
            result:
                collections.defaultdict. 
    '''
    pairs_t = list(pairs.groupby("left")["right"].value_counts().index)
    result = {}
    for k, v in pairs_t:
        result.setdefault(k, []).append(v)
    return result 