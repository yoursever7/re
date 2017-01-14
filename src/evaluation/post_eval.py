# !usr/bin/python
# author:liuyueji
# data:2016.8.4

import MySQLdb
from datetime import timedelta,datetime
#from __future__ import print_function
import time
import sys
import os 
from pyspark import SparkContext
import json

from evaluate_config import *


def readSQL(li,table):
    try:
        db=MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PWD, db=MYSQL_DB, port=MYSQL_PORT)
        cursor=db.cursor()
        cmd = "select * from %s"%table + " where item_type=1"
        cursor.execute(cmd)
        li.extend( list(cursor.fetchall()))
        #print(type(results),type(results[0]),type(results[0][4]),results[0][0])
        #print(results) 
        cursor.close()
        db.close()
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" %(e.args[0], e.args[1])

def get_new_use():
    
    use_list = []
    try_list = []
    collect_list = []
    eval_time = datetime.now() 
    train_time = eval_time - timedelta(days=100) 
    readSQL(try_list,'dt_behavior_product_try')
    readSQL(collect_list,'dt_behavior_product_collect')
    print try_list 
    use_list.extend(try_list)
    use_list.extend(collect_list)
    time_f_list=filter(lambda l: l[4]<eval_time and l[4] > train_time ,use_list)    
    usr_item_more_list=map(lambda l: (l[1],l[3]) ,time_f_list)    
    return list(set(usr_item_more_list) )   


def process(a,b):
    c = list(set(a)&set(b))
    precision = float(len(c))/float(len(a))
    recall  = float(len(c))/float(len(b))
    return [precision,recall]

if __name__ == "__main__":
    """
        Usage: post evaluation 
    """
    sc = SparkContext(appName="post_evaluation")
    new_use_data = get_new_use()    
    #print new_use_data
    new_use_list_rdd = sc.parallelize(new_use_data)
    new_use_list = new_use_list_rdd.map(lambda l:map(str,l))\
                                   .groupByKey().mapValues(list)
    #print new_use_list.collect()

     
    #new_use_list_file = "../data/new_use.list"
    
    rec_list_rdd = sc.textFile(CF_RESULT_FILE)
    #new_use_list_rdd = sc.textFile(new_use_list_file)
    
    #rec_list = rec_list_rdd.map(lambda l: l.strip(']').split(",["))\
    #                       .map(lambda l:(l[0],l[1].split(',')))
#******************************************************************************************************************************
#    rec_list = rec_list_rdd.map(lambda l: l.split())\
#                           .map(lambda l :[l[0], json.loads(str(l[1]))])\
#                           .map(lambda l :[l[0], [d.values() for d in l[1]]])\
#                           .map(lambda l :[l[0], [d[0] for d in l[1] ]])
#                           #.map(lambda l :[l[0], [d[0] for d in l[1] if d[1]=='2']]) rec_list is only for personal recomendation
#10050 [{"product":"1","type":"0"},{"product":"60","type":"2"},{"product":"2","type":"1"}]
#[{"product":"1","type":"0"},{"product":"60","type":"2"},{"product":"2","type":"1"}]
#[[u'1', u'0'], [u'60', u'2'], [u'2', u'1']]
#******************************************************************************************************************************
    rec_list = rec_list_rdd.map(lambda l :json.loads(str(l)))\
                           .map(lambda l :[l[0], [d.values() for d in l[1:]]])\
                           .map(lambda l :[l[0], [d[0] for d in l[1] ]])

    #new_use_list = new_use_list_rdd.map(lambda l: l.strip(']').split(",["))\
    #                       .map(lambda l:(l[0],l[1].split(',')))
    #print '--------------------------------------------------'
    #print rec_list.collect()
    data_ready = rec_list.join(new_use_list)
    #print '--------------------------------------------------'
    #print data_ready.collect()
    result = data_ready.map(lambda l : (l[0],process(l[1][0],l[1][1])))
    #print '--------------------------------------------------'
    f = open(POST_EVAL_RESULT_FILE,'w+')
    print >>f,result.collect()
    #print '--------------------------------------------------'

     
    print >>f,'precision: ',result.map(lambda l: l[1][0]).mean()
    print >>f,'recall: ',result.map(lambda l: l[1][1]).mean()
    f.close()
