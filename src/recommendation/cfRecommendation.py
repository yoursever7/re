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

from pyspark import SparkContext

# $example on$
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
# $example off$

from config import *
from recommendProducts import *



# MAIN
if __name__ == "__main__":   

    # new products
    all_new_products = []
    now_time = datetime.datetime.now()
    start_time = now_time + datetime.timedelta(days=(-1)*NEW_START_DAY)
    #new products
    calculateNewProducts(all_new_products,start_time)
    #all_new_products = [str(item) for item in all_new_products]
#    print("new products:")
#    print(all_new_products)

    #select products is able to try
    all_prefer_products = set()
    all_prefer_list=[]
    calculateNewProducts(all_prefer_list,"0000-00-00 00:00:00")
    for product in all_prefer_list:
        all_prefer_products.add(product)

    #hot products
    all_hot_products = []
    calculateHotProducts(all_hot_products)
    
    all_hot_products = filter(lambda x: True == (x in all_prefer_products), all_hot_products)
 #   print("hot products:")
 #   print(all_hot_products)

    user_sex_dict={}
    getUserSexDict(user_sex_dict)
#    print(user_sex_dict)

    male_products=set()
    selectSameSexProducts(male_products,'1')
#    print("male products:")
#    print(male_products)
    female_products=set()
    selectSameSexProducts(female_products,'2')
 #   print("female_products:")
#    print(female_products)

    # Init REDIS
    pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD,db=0)
    redisConn = redis.Redis(connection_pool=pool)

    # ALS Recommend 
    sc = SparkContext(appName="ALSRecommend")
    model = MatrixFactorizationModel.load(sc, MODEL_PATH)

    # Recommend products for each user individually
    recommend_fail_count = 0
    recommend_limit_count = 0
    recommend_successful_count = 0

    file_user = open(USERS_PATH)
    file_result = open(CF_RESULT_PATH,'w')

    for user in file_user:
        user = user.strip('\n')
        
        # als recommend result
        als_products=[]
        try:
            topKRecs = model.recommendProducts(int(user), ALS_RECOMMEND_K)
        except:
            recommend_fail_count += 1
            continue

        for item in topKRecs:
            als_products.append(item.product)
        #print("als products:")
        #print(als_products)

        #if this user already has behavior on this product,remove this product
        behavior_key = 'behavior:' + str(user)
        hot_products = filter(lambda x: 0 == redisConn.sismember(behavior_key,x), all_hot_products)
        new_products = filter(lambda x: 0 == redisConn.sismember(behavior_key,x), all_new_products)
        als_products = filter(lambda x: 0 == redisConn.sismember(behavior_key,x), als_products)

        als_products = filter(lambda x: True == (x in all_prefer_products), als_products)

        #filter by sex
        if False == user_sex_dict.has_key(int(user)):
            continue

        user_sex = user_sex_dict[int(user)]
        same_sex_products ={}
        if user_sex==1:
            same_sex_products=male_products
        else:
            same_sex_products=female_products
        #print("\n\nunfilter results:")
        #print(hot_products)
        #print(new_products)
        #print(als_products)
        hot_products = filter(lambda x: True == (x in same_sex_products), hot_products)
        new_products = filter(lambda x: True == (x in same_sex_products), new_products)
        als_products = filter(lambda x: True == (x in same_sex_products), als_products)
        #print("\n\nfilter results:")
        #print(hot_products)
        #print(new_products)
        #print(als_products)
        
        rec_len = len(als_products)
        if rec_len < LIMIT_ALS_RECOMMEND_LEN:
            recommend_limit_count += 1
            continue
        
        MergeRecommendResults(str(user), hot_products, new_products, als_products, "global_cf:",redisConn,file_result)
        recommend_successful_count += 1

    file_user.close()
    file_result.close()
    print("Recommend Finished!\n")
    print("Report:\n")
    print("recommend_fail_count       %d\n"%recommend_fail_count)
    print("recommend_limit_count      %d\n"%recommend_limit_count)
    print("recommend_successful_count %d\n"%recommend_successful_count)

