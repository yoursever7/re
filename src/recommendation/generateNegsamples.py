# !usr/bin/python
# generate negtive samples
# author:zhangyx
# since 2016-09-15

import MySQLdb
from datetime import timedelta,datetime
#from __future__ import print_function
import time
import sys
import os 
from pyspark import SparkContext
import json

from config import *
from utils  import *

YESTERDAY = 600


def readSQL(result_list, table):

    members = "charid, item_id"

    eval_time = datetime.now()
    train_time = eval_time - timedelta(days=YESTERDAY)
    condition = "item_type=1 and time>'%s' and time<'%s'"%(train_time.strftime("%Y-%m-%d"),eval_time.strftime("%Y-%m-%d"))

    result_list.extend(LoadDataFromSQL(members, table, condition))


def GetNewUse():
    
    use_list = []
    try_list = []
    collect_list = []
    readSQL(try_list,'dt_behavior_product_try')
    readSQL(collect_list,'dt_behavior_product_collect')
    #print 'sql:',time.time()
    #print collect_list 
    use_list.extend(try_list)
    use_list.extend(collect_list)
    return list(set(use_list) )   


def FindMiss(recommend_list, behavior_list):
    recommend_set = set(recommend_list) 
    behavior_set  = set(behavior_list)
    hit_set = recommend_set & behavior_set
    miss_set = recommend_set.difference(hit_set)
    return list(miss_set)


if __name__ == "__main__":
    """
        Usage: generateNegsamples
    """
    sc = SparkContext(appName="generateNegsamples")
    print 'start:',time.time()
    new_use_data = GetNewUse()    
    #print new_use_data
    print 'get new use data:',time.time()
    new_use_list_rdd = sc.parallelize(new_use_data)
    new_use_list_rdd = new_use_list_rdd.map(lambda l:map(str,l))\
                                   .groupByKey().mapValues(list)
    
    rec_list_rdd = sc.textFile(CF_RESULT_PATH)
    rec_list_rdd = rec_list_rdd.map(lambda l :json.loads(str(l)))\
                           .map(lambda l :[l[0], [d.values() for d in l[1:]]])\
                           .map(lambda l :[l[0], [d[0] for d in l[1] ]])

    data_ready_rdd = rec_list_rdd.join(new_use_list_rdd)
    user_neg_samples_list = data_ready_rdd.map(lambda l : (l[0], FindMiss(l[1][0], l[1][1]))).collect()
    print 'get result:',time.time()

    db=MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PWD, db=MYSQL_DB, port=MYSQL_PORT)
    cursor = db.cursor()
    for user_neg_samples in user_neg_samples_list:
       #print user_neg_samples
       user = user_neg_samples[0]
       #for product in user_neg_samples[1]:
       #   sql = "insert into dt_behavior_product_neglect(charid,item_type,item_id) values(" + str(user)+",1,"+str(product)+")"
       #   executeSQLinDB(db, sql)

       sql = "insert into dt_behavior_product_neglect(charid,item_type,item_id) values(%s, %s, %s)"
       data = []
       for product in user_neg_samples[1]:
          data.append((str(user), '1', str(product)))
       executemanySQLinDB(db, sql, data)

    db.close()

    print("generateNegsamples Finished!\n")
