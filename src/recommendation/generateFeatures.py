# !usr/bin/python
# generate ALS input data
# author:zhangyx,xiefq
# since 2016-05-20

from __future__ import division
import sys,time
import datetime
import MySQLdb
import random
import redis

from config import *

#
# calculate confidence 
#

BATCH_SIZE = 100000
LIMIT_SIZE = 300000


TRY_WEIGHT = 1
COLLECT_WEIGHT = 5
CONFIDENCE_MAX = 5
CONFIDENCE_MIN = 0



def calculateConfidence(dictionary, set_user,table, weight):
	try:
		db=MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PWD, db=MYSQL_DB, port=MYSQL_PORT)
		cursor=db.cursor()
		now_time = datetime.datetime.now()
		start_time = now_time + datetime.timedelta(days=(-1)*SAMPLE_START_DAY)

		cmd = "select count(1) from %s"%table + " where item_type=1 and time > '%s"%start_time + "'"
                print(cmd)
		cursor.execute(cmd)
		results=cursor.fetchone()
		num = results[0]
		print("Loading %s %d entries"%(table ,num))

		index = 0
		while(index < min(num, LIMIT_SIZE)):
			#print(table+":%d\n"%index)
			sys.stdout.write(str(int((index/num)*100)) + "%" + "\r")
			sys.stdout.flush()
			cmd = "select charid, item_id from %s"%table + " where item_type=1 and time > '%s' limit "%start_time + str(index) + "," + str(BATCH_SIZE)
			#print(cmd)
			cursor.execute(cmd)
			results = cursor.fetchall()
			for row in results:
				charid = row[0]
				set_user.add(charid)
				item_id = row[1]
				#time = row[5]
		#			print "charid=%d\titem_id=%d\ttime=%s"%(charid, item_id, time)
				if dictionary.has_key(charid):
					if dictionary[charid].has_key(item_id):
						dictionary[charid][item_id] += weight
					else:
						dictionary[charid][item_id] = weight
				else:
					dictionary[charid] = {item_id : weight}
			index += BATCH_SIZE

		sys.stdout.write("100%" + "\r")
		sys.stdout.flush()
		print

		cursor.close()
		db.close()
     
	except MySQLdb.Error,e:
		print "Mysql Error %d: %s" % (e.args[0], e.args[1])
 


# MAIN
if __name__ == "__main__":
	dictionary_confidence = {}
	list_confidence = []
	set_user = set()

	#confidence weight
	collect_weight = COLLECT_WEIGHT
	cancelcollect_weight = -COLLECT_WEIGHT
	try_weight = TRY_WEIGHT
	neglect_weight = -TRY_WEIGHT

	# calculate confidence by each behavior
#	calculateConfidence(dictionary_confidence, set_user,"dt_behavior_product_collect", collect_weight)
#	calculateConfidence(dictionary_confidence, set_user,"dt_behavior_product_cancelcollect", cancelcollect_weight)
	calculateConfidence(dictionary_confidence, set_user,"dt_behavior_product_try", try_weight)
	calculateConfidence(dictionary_confidence, set_user,"dt_behavior_product_neglect", neglect_weight)
	print("Processing ..\n")

	pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD,db=0)
	redisConn = redis.Redis(connection_pool=pool)

	for charid, value in dictionary_confidence.items():
		#clear user's history behaviors
		key = 'behavior:' + str(charid)
		redisConn.delete(key)
		for item_id, confidence in value.items():
    		#confidence between [MIN, MAX]
			if confidence > CONFIDENCE_MAX:
				confidence = CONFIDENCE_MAX
			if confidence < CONFIDENCE_MIN:
				confidence = CONFIDENCE_MIN
			list_confidence.append([charid, item_id, confidence])
			#update user's behavior 						
			redisConn.sadd(key,item_id)

	print("Writing ..\n")
#	print set_user
#	print list_confidence
	file_rating_all = open(RATINGS_ALL_PATH,'w')
	file_rating_base = open(RATINGS_BASE_PATH,'w')
	for c in list_confidence:
		charid=c[0]
		item_id=c[1]
		rating=c[2]
		text = str(charid)+','+str(item_id)+','+str(rating)+'\n'
		file_rating_all.write(text)
		if(random.random()<0.8):
			file_rating_base.write(text)
	file_rating_all.close()
	file_rating_base.close()
	
	file_user = open(USERS_PATH,'w')
	for charid in set_user:
		file_user.write(str(charid)+'\n')
	file_user.close()
	
	print "generateFeatures.py finished successfully"

