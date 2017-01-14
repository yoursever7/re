# !usr/bin/python
# recommend products to user by the model
# author:xiefq
# since 2016-06-01


"""
Collaborative Filtering
"""
from __future__ import print_function

import sys
import MySQLdb
import redis
import json

import datetime

from config import *

def getUserSexDict(user_sex_dict):
    try:
        db=MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PWD, db=MYSQL_DB, port=MYSQL_PORT)
        cursor=db.cursor()

        cmd ="select charid,sex from dt_user_charbase" 
        cursor.execute(cmd)
        results = cursor.fetchall()
        for row in results:
            charbase_id = row[0]
            sex = row[1]
            user_sex_dict[charbase_id]=sex

        cursor.close()
        db.close()
    except MySQLdb.Error,e:
        print ("Mysql Error %d: %s" % (e.args[0], e.args[1])) 

#calculate the Same Sex products
def selectSameSexProducts(products,user_sex):
    try:
        db=MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PWD, db=MYSQL_DB, port=MYSQL_PORT)
        cursor=db.cursor()

        cmd ="select id from dt_product_clothing where prefer=1 and sex="+user_sex 
        cursor.execute(cmd)
        results = cursor.fetchall()
        for row in results:
            products.add(row[0])

        cursor.close()
        db.close()
    except MySQLdb.Error,e:
        print ("Mysql Error %d: %s" % (e.args[0], e.args[1])) 

#calculate the hot products
def calculateHotProducts(all_hot_products):
    #<productId,hot>
    dict_hot = {}

    # behavior weight
    try_weight = 1
    collect_weight = 1
    
    now_time = datetime.datetime.now()
    start_time = now_time + datetime.timedelta(days=(-1)*HOT_START_DAY)
    calculateProductsHotValue(dict_hot,"dt_behavior_product_try",start_time,try_weight)
    calculateProductsHotValue(dict_hot,"dt_behavior_product_collect", start_time,collect_weight)
    sorted_dict_hot = sorted(dict_hot.items(),key=lambda d:d[1],reverse=True)
    for item in sorted_dict_hot:
        all_hot_products.append(item[0])
    #all_hot_products = [str(item) for item in all_hot_products]


#calculate the hot value of products
def calculateProductsHotValue(dict_hot,table,start_time,weight):
    try:
        db=MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PWD, db=MYSQL_DB, port=MYSQL_PORT)
        cursor=db.cursor()

        cmd ="select item_id,count(1)  from %s"%table + " where item_type=1 and time >= '%s"%start_time + "' group by item_id"
#        print(cmd)
        cursor.execute(cmd)
        results = cursor.fetchall()
        for row in results:
            product_id = row[0]
            count = row[1]
            hot_incr = count * weight

            if dict_hot.has_key(product_id):             
                dict_hot[product_id] += hot_incr
            else:
                dict_hot[product_id] = hot_incr
        cursor.close()
        db.close()
    except MySQLdb.Error,e:
        print ("Mysql Error %d: %s" % (e.args[0], e.args[1]))

#calculate the hot products
def calculateNewProducts(new_products,start_time):
    try:
        db=MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PWD, db=MYSQL_DB, port=MYSQL_PORT)
        cursor=db.cursor()

        cmd ="select id from dt_product_clothing where prefer=1 and create_at >= '%s"%start_time + "' order by create_at desc" 
        cursor.execute(cmd)
        results = cursor.fetchall()
        for row in results:
            new_products.append(row[0])

        cursor.close()
        db.close()
    except MySQLdb.Error,e:
        print ("Mysql Error %d: %s" % (e.args[0], e.args[1])) 

#Merge recommendation results
def MergeRecommendResults(userIDstr, hot_products, new_products, accurate_products, key_prefix,redisConn,file_result):
    #merge products
    merge_products = []
    recommend_products_set = set()

    hot_index = 0
    new_index = 0
    accurate_index = 0
    hot_size = len(hot_products)
    new_size = len(new_products)
    accurate_size = len(accurate_products)

    for k in range(0,RECOMMEND_NUM):
        rec_category = k % 3
        rec_product = -1
        rec_type = -1;
        has_product = False
        #hot
        if 0 == rec_category and hot_index < hot_size:
		            flag = False
                            for i in range(hot_index,hot_size):
                                    if(hot_products[i] not in recommend_products_set):
                                            flag = True
                                            break
                            if flag == True:
                                    rec_product = hot_products[i]
                                    rec_type = 0
                                    recommend_products_set.add(rec_product)
                                    hot_index = i+1
                                    has_product = True
        #new
        elif 1 == rec_category and new_index < new_size:
                            flag = False
                            for i in range(new_index,new_size):
                                    if(new_products[i] not in recommend_products_set):
                                            flag = True
                                            break
                            if flag == True:
                                    rec_product = new_products[i]
                                    rec_type = 1
                                    recommend_products_set.add(rec_product)
                                    new_index = i+1
                                    has_product = True
        #accurate
        if has_product == False and accurate_index < accurate_size:
                            flag = False
                            for i in range(accurate_index, accurate_size):
                                    if(accurate_products[i] not in recommend_products_set):
                                            flag = True
                                            break
                            if flag == True:
                                    rec_product = accurate_products[i]
                                    if key_prefix == 'global_cf:':
                                        rec_type = 2
                                    elif key_prefix == 'global_classify:':
                                        rec_type = 3
                                    recommend_products_set.add(rec_product)
                                    accurate_index = i+1
                                    has_product = True
        if has_product == True:
                             dict_product = {}
                             dict_product['type']=str(rec_type)
                             dict_product['product']=str(rec_product)
                             merge_products.append(dict_product)
        elif hot_index >= hot_size and new_index >= new_size and accurate_index >= accurate_size:
                             break

    recommend_results = json.dumps(merge_products)

    #print("user:" + userIDstr)
    #print("recommend_results")
    #print(recommend_results)

    p = redisConn.pipeline()
    key = key_prefix + userIDstr
    #delete the old recommend products
    p.delete(key)
    #set the new recommend products
    p.set(key,recommend_results)
    p.execute()

    #write result to file
    merge_products.insert(0, userIDstr)
    result = json.dumps(merge_products)
    file_result.write(result+'\n')

