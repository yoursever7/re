# !usr/bin/python
# classify recommendation, for cold starting
# author:zhangyx
# since 2016-09-15

# cmd.sh
# $SPARK_HOME/bin/spark-submit --master spark://10.105.247.189:7077 classifyRecommendation.py > log.txt
#

"""
Global Classify Recommendation.
Solving User Code Start.
"""
from __future__ import print_function

import sys
import MySQLdb
import redis
import json
import itertools
from operator import itemgetter
from pyspark import SparkContext

from config import *
from utils  import *
from recommendProducts import *

CHOOSE = 5.0
BETA = 5.0


def Normalize(data_dic):

    if len(data_dic) == 0:
       return data_dic

    if len(data_dic) == 1:
       for key, value in data_dic.iteritems():
          data_dic[key] = 0.5
       return data_dic

    min = 0.0
    max = 0.0
    for key, value in data_dic.iteritems():
       #if value < min:
       #   min = value
       if value > max:
          max = value

    #print("min ", min, " max ", max)
    for key, value in data_dic.iteritems():
       data_dic[key] = (data_dic[key] - min) / (max - min)

    return data_dic

def UserClassifyRecommendation(sc):

    users_list_fromDB = LoadDataFromSQL("charid, sex, age", "dt_user_charbase", "")
    usersRDD = sc.parallelize(users_list_fromDB).map(lambda l: (int(l[0]), int(l[1]), int(l[2])))
    users_list_fromDB = []

    items_list_fromDB = LoadDataFromSQL("id", "dt_product_clothing", "")
    itemsRDD = sc.parallelize(items_list_fromDB).map(lambda l: int(l[0]))
    items_list_fromDB = []
    items_list = itemsRDD.collect()

    #
    # generate user classifications
    #
    gender_list = [1, 2] # M:1, F:2
    age_list = []
    for i in range(0, 60):
       age_list.append(i)
    age_list.append(60)
    user_classification = itertools.product(gender_list, age_list)

    #
    # Load ratings
    #
    data = sc.textFile(RATINGS_BASE_PATH)
    ratings = data.map(lambda l: l.split(','))\
        .map(lambda l: (int(l[0]), int(l[1]), float(l[2])))

    ratings = ratings.filter(lambda l: l[2] >= CHOOSE)
    ratings_list = ratings.collect()

    #
    # Calculate Ni for each item
    #
    Ni_dic = {}
    for k in range(len(items_list)):
       itemID = items_list[k]
       Ni_dic[itemID] = []

    for rating in ratings_list:
       itemID = rating[1]
       contain = Ni_dic.get(itemID)
       if contain == None:
          print("cannot find %d\n"%itemID)
       else:
          contain.append(rating[0])

    age_score_dic_group = {}
    gender_score_dic_group = {}
    #occupation_score_dic_group = {}

    #
    # Now I will calcualte p(f, i) = |N(i) and U(f)| / (N(i) + BETA)
    #

    # Calculate scores for each age range
    for age in age_list:
       Uf = usersRDD.filter(lambda l: int(l[2]) == age).map(lambda l: int(l[0])).collect()
       score_dic = {}
       for itemID in items_list:
          JoinNiUf = list(set(Ni_dic[itemID]) & set(Uf))
          if len(JoinNiUf) > 0:
             score = len(JoinNiUf) / (len(Ni_dic[itemID]) + BETA)
             score_dic[itemID] = score
       Normalize(score_dic)
       age_score_dic_group[age] = score_dic

       #sorted_score_list = sorted(score_dic.iteritems(), key=itemgetter(1), reverse=True)
       #print("Sorted score list: age ", age)
       #for i in range(len(sorted_score_list)):
       #   itemID = sorted_score_list[i][0]
       #   print(sorted_score_list[i])
       #   #print(itemtitles_list[itemID-1])

    # Calculate scores for each gender
    for gender in gender_list:
       Uf = usersRDD.filter(lambda l: int(l[1]) == gender).map(lambda l: int(l[0])).collect()
       score_dic = {}
       for itemID in items_list:
          JoinNiUf = list(set(Ni_dic[itemID]) & set(Uf))
          if len(JoinNiUf) > 0:
             score = len(JoinNiUf) / (len(Ni_dic[itemID]) + BETA)
             score_dic[itemID] = score
       Normalize(score_dic)
       gender_score_dic_group[gender] = score_dic

       #sorted_score_list = sorted(score_dic.iteritems(), key=itemgetter(1), reverse=True)
       #print("Sorted score list: gender ", gender)
       #for i in range(len(sorted_score_list)):
       #   itemID = sorted_score_list[i][0]
       #   print(sorted_score_list[i])
       #   #print(itemtitles_list[itemID-1])

    #
    # Merge 
    #
    classify_result_dic = {}

    for classification in user_classification:
       gender = classification[0]
       age = classification[1]

       merged_score_dic = {}
       for (itemID, score) in age_score_dic_group[age].items():
          if merged_score_dic.has_key(itemID):
             merged_score_dic[itemID] += score
          else:
             merged_score_dic[itemID] = score

       for (itemID, score) in gender_score_dic_group[gender].items():
          if merged_score_dic.has_key(itemID):
             merged_score_dic[itemID] += score
          else:
             merged_score_dic[itemID] = score

       merged_score_list = sorted(merged_score_dic.iteritems(), key=itemgetter(1), reverse=True) # (itemID, score)
       merged_list = map(lambda l: l[0], merged_score_list) # itemID

       classify_result_dic["%s_%s"%(gender, age)] = merged_list

       #print("\nMerged score list:")
       #print("%s_%s\n"%(gender, age))
       #for i in range(len(merged_score_list)):
       #   itemID = merged_score_list[i][0]
       #   print(merged_score_list[i])
       #   #print(item_titles_list[itemID-1])

    return classify_result_dic

#
# MAIN
#
if __name__ == "__main__":   

    # new products
    all_new_products = []
    now_time = datetime.datetime.now()
    start_time = now_time + datetime.timedelta(days=(-1)*NEW_START_DAY)
    #new products
    calculateNewProducts(all_new_products,start_time)
    #all_new_products = [str(item) for item in all_new_products]
    print("new products:")
    print(all_new_products)

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
    print("hot products:")
    print(all_hot_products)

    # Init REDIS
    pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD,db=0)
    redisConn = redis.Redis(connection_pool=pool)

    # Recommend products for each user-classification
    sc = SparkContext(appName="ClassifyRecommend")
    classify_result_dic = UserClassifyRecommendation(sc)

    file_result = open(CLASSIFY_RESULT_PATH, 'w')

    for classification, classify_products in classify_result_dic.iteritems():

        if len(classify_products) > CLASSIFY_RECOMMEND_K:
            classify_products = classify_products[0: CLASSIFY_RECOMMEND_K]
        #classify_products = [str(item) for item in classify_products]
        #print("\nMerged score list:")
        #print("%s\n"%(classification))
        #for i in range(len(score_list)):
        #   itemID = score_list[i]
        #   print(score_list[i])

        hot_products = all_hot_products
        new_products = all_new_products
        
        # TODO filter the products which prefer is not 1
        classify_products = filter(lambda x: True == (x in all_prefer_products), classify_products)

        #print("\n\nfilter results:")
        #print(new_products)
        #print(hot_products)
        #print(classify_products)

        MergeRecommendResults(classification, hot_products, new_products, classify_products, "global_classify:", redisConn, file_result)


    # END
    # VERIFY DATA IN REDIS
    #print("\n\nVERIFY DATA IN REDIS\n\n")
    #for classification, score_list in classify_result_dic.iteritems():
    #   print("global_classify:" + classification)
    #   print(redisConn.get("global_classify:" + classification))

    #keys = redisConn.keys("global_classify:*")

    #for key in keys:
    #   print(key)
       #print(redisConn.get(key))

    file_result.close()
    print("Recommend Finished!\n")
