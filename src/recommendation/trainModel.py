# !usr/bin/python
# train the ALS model 
# author:xiefq
# since 2016-06-01

"""
Collaborative Filtering
"""
from __future__ import print_function

import sys
import redis
import shutil
import os

from pyspark import SparkContext

# $example on$
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
# $example off$

from config import *

if __name__ == "__main__":
    sc = SparkContext(appName="Train_ALS_Model")
    # $example on$
    # Load and parse the rating data
    data = sc.textFile(RATINGS_BASE_PATH)


    ratings = data.map(lambda l: l.split(','))\
        .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

    # Build the recommendation model using Alternating Least Squares
    file_params = open(PARAMS_PATH)
    params_str = file_params.readline()
    params_str=params_str.strip('\n')
    params=params_str.split(" ")
    
    file_params.close()
    rank = int(params[0])
    numIterations = int(params[1])
    lambda_ = float(params[2])
    alpha = float(params[3])
    #trainImplicit(ratings, rank, iterations=5, lambda_=0.01, blocks=-1, alpha=0.01, nonnegative=False, seed=None)
    model = ALS.trainImplicit(ratings, rank, numIterations, lambda_, -1, alpha)

    #Save model
    shutil.rmtree(MODEL_PATH)
    os.mkdir(MODEL_PATH)
    model.save(sc, MODEL_PATH)

    # Evaluate the model on training data
    testdata = ratings.map(lambda p: (p[0], p[1]))
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
    print("Mean Squared Error = " + str(MSE))

    #predictRating = model.predict(10038, 28)
    #print("10038:28 = " + str(predictRating))

    # Recommend top 10 items for user_12;  Only for test
    #print("\nRecommend top 20 items\n")
    #userId = 10046
    #K = 20
    #topKRecs = model.recommendProducts(userId, K)
    #print(topKRecs)


    # Predict user_12 item_123
#    predictRating = model.predict(12, 123)
#    print("\npredictRating(12, 123) = " + str(predictRating))
#
#    # Build item table
#    items = sc.textFile("ml-100k/u.item")
#    titles = items.map(lambda l: l.split('|'))\
#                   .map(lambda l: [int(l[0]), l[1]])\
#                   .collect()
#
#    # Known top 10 items by user_12
#    print("\nKnown top 10 items\n")
#    itemsForUser = ratings.keyBy(lambda l: l.user).lookup(12)
#    print(itemsForUser)
#    RDDitemsForUser = sc.parallelize(itemsForUser)
#    recommendlist = RDDitemsForUser.sortBy(lambda l: -l.rating)\
#                 .map(lambda l: titles[l.product-1])\
#                 .take(10)
#    for i, l in recommendlist:
#        print(l)
#
#    # Recommend top 10 items for user_12
#    print("\nRecommend top 10 items\n")
#    userId = 12
#    K = 10
#    topKRecs = model.recommendProducts(userId, K)
#    print(topKRecs)
#
#    recommendForUser = sc.parallelize(topKRecs)
#    recommendlist = recommendForUser.map(lambda l: titles[l.product-1])\
#                 .collect()
#
#    for i, l in recommendlist:
#       print(l)
#
#    # Item recommendation
#    print("\nItem recommendation\n")
#    itemId = 567
#    itemFactor = model.productFeatures.collect() #.lookup(itemId).head
#    #itemVector = new DoubleMatrix(itemFactor)
#    
#    print(itemFactor)

