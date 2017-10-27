#from __future__ import print_function

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

cluster = Cluster(['127.0.0.1'])
session = cluster.connect("records") 

chance_wc = session.prepare("INSERT INTO %s (campaign, time, count) VALUES (?,?,?)" % CHANCE_TABLE)
impression_wc = session.prepare("INSERT INTO %s (campaign, time, count, bid,budget,price) VALUES (?,?,?,?,?,?)" % IMPRESSION_TABLE)
click_wc = session.prepare("INSERT INTO %s (campaign, time, count) VALUES (?,?,?)" % CLICK_TABLE)

chance_wa = session.prepare("INSERT INTO %s (ad, time, count) VALUES (?,?,?)" % CHANCE_TABLEA)
impression_wa = session.prepare("INSERT INTO %s (ad, time, count, category,brand) VALUES (?,?,?,?,?)" % IMPRESSION_TABLEA)
click_wa = session.prepare("INSERT INTO %s (ad, time, count) VALUES (?,?,?)" % CLICK_TABLEA)


logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)




def result(y):
	return y


def processChance(time, rdd):
	# record map
	num_of_record = rdd.count()
	if num_of_record == 0:
		return

	#time_chance_map = rdd.map(lambda record : (json.loads(record[0].decode('utf-8'))[0].get('adId'),json.loads(record[0].decode('utf-8'))[0].get('adId'), json.loads(record[1].decode('utf-8'))[0].get('compaignId')))
	time_chance_map = rdd.map(lambda record : record[1].split(" "))

	for x in time_chance_map.collect():
		logger.info('line:' + x[0])
		pprint('line:' + x[1])
		session.execute(chance_wc, (int(x[1]),int(float(datetime.now().strftime("%s.%f"))) * 1000, random.randint(180, 400),))
		session.execute(chance_wa, (int(x[0]),int(float(datetime.now().strftime("%s.%f"))) * 1000, random.randint(70, 200),))
		logger.info('received %d chance records for %s from kafka, adId is %s' % (num_of_record, x[0], x[1]))

def processImpression(time, rdd):
	# record map
	num_of_record = rdd.count()
	if num_of_record == 0:
		return

	#time_impr_map = rdd.map(lambda record : (json.loads(record[1].decode('utf-8'))[0].get('adId'),json.loads(record[1].decode('utf-8'))[0].get('adId'), json.loads(record[1].decode('utf-8'))[0].get('compaignId')))
	try:
		time_impr_map = rdd.map(lambda (k, v): json.loads(v)).map(lambda x: (x['adId'],x['compaignId'],x['category'],x['brand'],x['bid'],x['budget'], x['title'], x['price']))
	except KeyError:
		logger.info('error with %d', num_of_record)
		pprint(rdd.map(lambda (k, v): v).collect())

	for x in time_impr_map.collect():
		pprint('line:' + str(x[0][0]))
		session.execute(impression_wc, (x[1][0], int(float(datetime.now().strftime("%s.%f"))) * 1000, random.randint(34, 90), x[4][0], x[5][0], x[7][0], ))
		session.execute(impression_wa, (x[0][0], int(float(datetime.now().strftime("%s.%f"))) * 1000, random.randint(15, 30), x[2][0], x[3][0], ))
		logger.info('received %d impression records for %s from kafka, adId is %s' % (num_of_record, str(x[0][0]), str(x[1][0]),))


def processClick(time, rdd):
	# record map
	num_of_record = rdd.count()
	if num_of_record == 0:
		return

	time_click_map = rdd.map(lambda (k, v): json.loads(v)).map(lambda x: (x['adId'],x['compaignId'],x['category']))

	for x in time_click_map.collect():
		#session.execute(click_writer, (int(float(datetime.now().strftime("%s.%f"))) * 1000, x[0],x[1],))
		session.execute(click_wc, (x[1][0], int(float(datetime.now().strftime("%s.%f"))) * 1000, random.randint(4, 24),  ))
		session.execute(click_wa, (x[0][0], int(float(datetime.now().strftime("%s.%f"))) * 1000, random.randint(1, 20),  ))
		logger.info('received %d click records for %s from kafka, adId is %s' % (num_of_record, str(x[0][0]), str(x[1][0]), ))



if __name__ == "__main__":
	sc = SparkContext(appName="test-streaming")
	ssc = StreamingContext(sc, 5)

	chance_stream = KafkaUtils.createDirectStream(ssc, [KAFKA_TOPIC_CHANCE], {"metadata.broker.list": KAFKA_NODE})
	impression_stream = KafkaUtils.createDirectStream(ssc, [KAFKA_TOPIC_IMPRESSION], {"metadata.broker.list": KAFKA_NODE})
	click_stream = KafkaUtils.createDirectStream(ssc, [KAFKA_TOPIC_CLICK], {"metadata.broker.list": KAFKA_NODE})


	# chance_lines.count().pprint()

	chance_stream.foreachRDD(processChance)
	impression_stream.foreachRDD(processImpression)
	click_stream.foreachRDD(processClick)


	ssc.start()
	ssc.awaitTermination()
