import os
import sys
import json
import random
from sets import Set
import libmc
from libmc import (
    MC_HASH_MD5, MC_POLL_TIMEOUT, MC_CONNECT_TIMEOUT, MC_RETRY_TIMEOUT
)

# Generate rewrite query base on synonym list and store in memcached 


mc = libmc.Client(
["127.0.0.1:11219"],comp_threshold=0, noreply=False, prefix=None,hash_fn=MC_HASH_MD5, failover=False
)
mc.config(MC_POLL_TIMEOUT, 100)  # 100 ms
mc.config(MC_CONNECT_TIMEOUT, 300)  # 300 ms
mc.config(MC_RETRY_TIMEOUT, 5)  # 5 s

#input： [nike, running, shoes] , Dict
#output: all rewrite query
def query_rewriter_helper(query_terms, synonyms_dict):
    if (len(query_terms) == 0):
        return []

    if (len(query_terms) == 1):
        if query_terms[0] not in synonyms_dict:
            return [query_terms[0]]
        else:
            return list(synonyms_dict[query_terms[0]])

    prev = query_rewriter_helper(query_terms[:-1], synonyms_dict)
    if query_terms[-1] in synonyms_dict:
        post = synonyms_dict[query_terms[-1]]
        return [s + '_' + c for s in prev for c in post]
    else:
        return [s + '_' + query_terms[-1] for s in prev]

if __name__ == "__main__":
    synonyms_input_file = sys.argv[1]
    ads_input_file = sys.argv[2]
    #synonyms_output_file = sys.argv[3]
    synonyms_dict = {}
    query_set = Set()

    with open(synonyms_input_file, "r") as lines:
        for line in lines:
            entry = json.loads(line.strip())
            if  "word" in entry and "synonyms" in entry :
                synonyms_dict[entry["word"]] = entry["synonyms"]
                synonyms_dict[entry["word"]].append(entry["word"])

    with open(ads_input_file, "r") as lines:
        for line in lines:
            entry = json.loads(line.strip())
            if  "query" in entry:
                if entry["query"] not in query_set:
                    query_set.add(entry["query"])

    for query in query_set:
        #print "query:", query
        query_terms = query.split(" ")
        query_key = "_".join(query_terms)
        synonyms = query_rewriter_helper(query_terms, synonyms_dict)
        #dedupe synonyms
        unique_synonyms = Set()
        final_synonyms = []
        for synonym in synonyms:
            #print synonym
            if synonym not in unique_synonyms:
                unique_synonyms.add(synonym)
                final_synonyms.append(synonym)

        mc.set(query_key, final_synonyms)
