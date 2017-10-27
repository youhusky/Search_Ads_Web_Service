from __future__ import print_function

import sys
import json
import logging
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
from datetime import datetime
from pprint import pprint

import random

KAFKA_NODE=""
KAFKA_TOPIC_CHANCE="chance"
KAFKA_TOPIC_IMPRESSION="impression"
KAFKA_TOPIC_CLICK="click"
KAFKA_TOPIC_TEST = "test"

CHANCE_TABLE = "chance_c" 
IMPRESSION_TABLE = "impression_c"
CLICK_TABLE = "click_c"
COMPAIGN_TABLE = "compaign"

CHANCE_TABLEA = "chance_a" 
IMPRESSION_TABLEA = "impression_a"
CLICK_TABLEA = "click_a"

# cluster = Cluster(['127.0.0.1'])
# session = cluster.connect("records") 

# chance_wc = session.prepare("INSERT INTO %s (campaign, time, count) VALUES (?,?,?)" % CHANCE_TABLE)
# impression_wc = session.prepare("INSERT INTO %s (campaign, time, count, bid,budget,price) VALUES (?,?,?,?,?,?)" % IMPRESSION_TABLE)
# click_wc = session.prepare("INSERT INTO %s (campaign, time, count) VALUES (?,?,?)" % CLICK_TABLE)

# chance_wa = session.prepare("INSERT INTO %s (ad, time, count) VALUES (?,?,?)" % CHANCE_TABLEA)
# impression_wa = session.prepare("INSERT INTO %s (ad, time, count, category,brand) VALUES (?,?,?,?,?)" % IMPRESSION_TABLEA)
# click_wa = session.prepare("INSERT INTO %s (ad, time, count) VALUES (?,?,?)" % CLICK_TABLEA)


logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)




def getSqlContextInstance(sparkContext):
  if ('sqlContextSingletonInstance' not in globals()):
      globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
  return globals()['sqlContextSingletonInstance']


def tfunc1(t,rdd,rddb):
  # 
  try:
    #----- 
    rowRdd1 = rdd.map(lambda x: Row(id=x[0], count=x[1], price=x[2]))
    imprInfo = getSqlContextInstance(rdd.context).createDataFrame(rowRdd1) 
    imprInfo.registerTempTable("impr")
    imprInfo = imprInfo.select()


    rowRdd2= rddb.map(lambda w: Row(id=x[0], count=x[1]))
    click = getSqlContextInstance(rddb.context).createDataFrame(rowRdd2) 
    click.registerTempTable("click")
    getSqlContextInstance(rdd.context).cacheTable('click')
    click = click.select(click.id, click.count)
    
    # #---- join
    imprClick = imprInfo.join(click, click.id==impr.id, 'leftouter').select(impr.id, impr.count, click.count)
    imprClick.registerTempTable("imprClick")
    imprClick.show()
    return imprClick.rdd

  except 'Exception':
    pass




def tfunc2(t,rdd,rddb):
  # 
  try:
    #----- 
    rowRdd1 = rdd.map(lambda x: Row(id=x[0], chance=x[1]))
    chance = getSqlContextInstance(rdd.context).createDataFrame(rowRdd1) 
    chance.registerTempTable("chance")
    chance = chance.select()


    rowRdd2= rddb.map(lambda w: Row(id=x[0], count=x[1]))
    ic = getSqlContextInstance(rddb.context).createDataFrame(rowRdd2) 
    ic.registerTempTable("ic")
    getSqlContextInstance(rdd.context).cacheTable('ic')
    ic = ic.select()
    
    # #---- join
    info = chance.join(ic, chance.id==ic.id, 'leftouter').select(chance.id, chance.chance, ic.click, ic.impression, ic.price)
    info.registerTempTable("info")
    info.show()
    return info.rdd

  except 'Exception':
    pass


def process(rdd):
  print(">>>> BEGIN CASS")
  info = getSqlContextInstance(rdd.context).createDataFrame(rdd) 
  info.registerTempTable("info")
  info.write.format("org.apache.spark.sql.cassandra").\
           options(keyspace="record", table="campaign").\
           save(mode="append")

  print(">>>> END CASS")



if __name__ == "__main__":
	sc = SparkContext(appName="test-streaming")
	ssc = StreamingContext(sc, 5)

	chance_stream = KafkaUtils.createDirectStream(ssc, [KAFKA_TOPIC_CHANCE], {"metadata.broker.list": KAFKA_NODE})
	impression_stream = KafkaUtils.createDirectStream(ssc, [KAFKA_TOPIC_IMPRESSION], {"metadata.broker.list": KAFKA_NODE})
	click_stream = KafkaUtils.createDirectStream(ssc, [KAFKA_TOPIC_CLICK], {"metadata.broker.list": KAFKA_NODE})



	campaignChance = chance_stream.map(lambda record : record[1].split(" ")).\
						map(lambda x: (x[0],1)).\
						reduceByKey(lambda x,y : x+y)
	campaignImpr = impression_stream.map(lambda (k, v): json.loads(v)).\
						map(lambda x: (x['compaignId'],x['price'],1)).\
						educeByKey(lambda x,y : (x[0]+y[0], x[1]+y[1])).\
						map(lambda x : (x[0], x[1]/x[0]))
	campaignClick = click_stream.map(lambda (k, v): json.loads(v)).\
						map(lambda x: (x['compaignId'],1)).\
						reduceByKey(lambda x,y : (x[0]+y[0], x[1]+y[1])).\
						map(lambda x : (x[0], x[1]/x[0]))


	campaignImprClick = campaignImpr.transformWith(tfun1, campaignClick)
	campaignInfo = campaignImprClick.transformWith(tfun2, campaignChance)


	campaignInfo.foreachRDD(process)
	campaignInfo.pprint()

	ssc.start()
	ssc.awaitTermination()
