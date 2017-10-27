import os
import sys
import glob
import libmc
from libmc import (
    MC_HASH_MD5, MC_POLL_TIMEOUT, MC_CONNECT_TIMEOUT, MC_RETRY_TIMEOUT
)

# Gnerate training files - Process log file and select features from cachee

from pyspark import SparkContext
def process_query(query):
    fields = query.split(" ")
    output = "_".join(fields)
    return output

# log: Device IP, Device id,Session id,Query,AdId,CampaignId,Ad_category_Query_category(0/1),clicked(0/1)
def prepare_feature_val(fields,memcache_client):
    device_ip = fields[0]
    device_id = fields[1]
    query = process_query(fields[3])
    ad_id = fields[4]
    camp_id = fields[5]
    query_ad_category_match = fields[6]

    if query_ad_category_match == '1':
        query_ad_category_match = '1000000'
    else:
        query_ad_category_match = '0'

    device_ip_click_key = "dipc_" + device_ip
    device_ip_click_val = memcache_client.get(device_ip_click_key)
    print "key=",device_ip_click_key
    print "val=",device_ip_click_val
    if not device_ip_click_val:
        device_ip_click_val = "0"


    device_ip_impression_key = "dipi_" + device_ip
    device_ip_impression_val = memcache_client.get(device_ip_impression_key)
    print device_ip_impression_val
    if not device_ip_impression_val:
            device_ip_impression_val = "0"

    device_id_click_key = "didc_" + device_id
    device_id_click_val = memcache_client.get(device_id_click_key)
    if not device_id_click_val:
        device_id_click_val = "0"

    device_id_impression_key = "didi_" + device_id
    device_id_impression_val = memcache_client.get(device_id_impression_key)
    if not device_id_impression_val:
        device_id_impression_val = "0"

    ad_id_click_key = "aidc_" + ad_id
    ad_id_click_val = memcache_client.get(ad_id_click_key)
    if not ad_id_click_val:
        ad_id_click_val = "0"

    ad_id_impression_key = "aidi_" + ad_id
    ad_id_impression_val = memcache_client.get(ad_id_impression_key)
    if not ad_id_impression_val:
        ad_id_impression_val = "0"

    query_campaign_id_click_key = "qcidc_" + query + "_" + camp_id;
    query_campaign_id_click_val = memcache_client.get(query_campaign_id_click_key)
    if not query_campaign_id_click_val:
        query_campaign_id_click_val = "0"

    query_campaign_id_impression_key = "qcidi_" + query + "_" + camp_id;
    query_campaign_id_impression_val = memcache_client.get(query_campaign_id_impression_key)
    if not query_campaign_id_impression_val:
        query_campaign_id_impression_val = "0"

    query_ad_id_click_key = "qaidc_" + query + "_" + ad_id;
    query_ad_id_click_val = memcache_client.get(query_ad_id_click_key)
    if not query_ad_id_click_val:
        query_ad_id_click_val = "0"

    query_ad_id_impression_key = "qaidi_" + query + "_" + ad_id;
    query_ad_id_impression_val = memcache_client.get(query_ad_id_impression_key)
    if not query_ad_id_impression_val:
        query_ad_id_impression_val = "0"

    features = []
    features.append(str(device_ip_click_val))
    features.append(str(device_ip_impression_val))
    features.append(str(device_id_click_val))
    features.append(str(device_id_impression_val))
    features.append(str(ad_id_click_val))
    features.append(str(ad_id_impression_val))
    features.append(str(query_campaign_id_click_val))
    features.append(str(query_campaign_id_impression_val))
    features.append(str(query_ad_id_click_val))
    features.append(str(query_ad_id_impression_val))
    features.append(query_ad_category_match)

    line = ",".join(features)
    #print line
    return line

if __name__ == "__main__":
    file = sys.argv[1] #log file
    #client = memcache.Client([('127.0.0.1', 11218)])
    client = libmc.Client(
        ["127.0.0.1:11218"],comp_threshold=0, noreply=False, prefix=None,hash_fn=MC_HASH_MD5, failover=False
    )
    sc = SparkContext(appName="CTR_Features")
    output_dir = './SearchAds/data/log/'
    data = sc.textFile(file).map(lambda line: line.encode("utf8", "ignore").split(','))
    feature_data = data.map(lambda fields: (prepare_feature_val(fields, client),int(fields[7])))

    feature_data.saveAsTextFile(output_dir + "ctr_features_demo3")
    sc.stop()
