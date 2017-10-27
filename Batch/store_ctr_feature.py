import os
import sys
import glob
import libmc
from libmc import (
    MC_HASH_MD5, MC_POLL_TIMEOUT, MC_CONNECT_TIMEOUT, MC_RETRY_TIMEOUT
)

# Store feature data of keys in memcached

def store_feature(feature_dir,key_prefix,memcache_client):
    path = feature_dir + "/part*"

    for filename in glob.glob(path):
        print "input data file:",filename
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip().strip("()")
                #print "input line:",line
                fields = line.split(",")
                key = key_prefix + "_" + fields[0].strip("''")
                #print key
                val = fields[1]
                #print val
                memcache_client.set(key,val)
                print "key=",key
                print "val=",val


if __name__ == "__main__":
    input_dir = './SearchAds/data/log/'
    #client = memcache.Client([('127.0.0.1', 11218)])
    client = libmc.Client(
    ["127.0.0.1:11218"],comp_threshold=0, noreply=False, prefix=None,hash_fn=MC_HASH_MD5, failover=False
    )
    client.config(MC_POLL_TIMEOUT, 100)  # 100 ms
    client.config(MC_CONNECT_TIMEOUT, 300)  # 300 ms
    client.config(MC_RETRY_TIMEOUT, 5)  # 5 s

    device_id_click = input_dir + "device_id_click"
    device_id_impression = input_dir + "device_id_impression"

    device_ip_click = input_dir + "device_ip_click"
    device_ip_impression = input_dir + "device_ip_impression"

    ad_id_click = input_dir + "ad_id_click"
    ad_id_impression = input_dir + "ad_id_impression"

    query_campaign_id_click =  input_dir + "query_campaign_id_click"
    query_campaign_id_impression = input_dir + "query_campaign_id_impression"

    query_ad_id_click = input_dir + "query_ad_id_click"
    query_ad_id_impression = input_dir + "query_ad_id_impression"

    store_feature(device_id_click, "didc",client)
    store_feature(device_id_impression, "didi",client)

    store_feature(device_ip_click, "dipc",client)
    store_feature(device_ip_impression, "dipi",client)

    store_feature(ad_id_click, "aidc",client)
    store_feature(ad_id_impression, "aidi",client)

    store_feature(query_campaign_id_click, "qcidc",client)
    store_feature(query_campaign_id_impression, "qcidi",client)

    store_feature(query_ad_id_click, "qaidc",client)
    store_feature(query_ad_id_impression, "qaidi",client)
