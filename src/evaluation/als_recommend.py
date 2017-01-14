# !usr/bin/python
# Calculate MSE and MAPK
# author:liuyj
# since 2016-09-15

# Usage:
# $SPARK_HOME/bin/spark-submit --master spark://127.0.0.1:7077 als_recommend.py rank iteration lambda topK > log.txt
#

import time
import sys
import os 
import re

from pyspark import SparkContext

# $example on$
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.mllib.evaluation import RankingMetrics
# $example off$
from evaluate_config import *

if __name__ == "__main__":
    start_time = time.time()
    #print ( time.time())
    name = "V3_CF_"+sys.argv[1]+"_"+sys.argv[2]+"_"+sys.argv[3]+"_"+sys.argv[4]+"_"+sys.argv[5]
    print name 
    sc = SparkContext(appName=name)
    
    # $example on$
    # Load and parse the data
    #data_file = "ml-100k/u.data"
    #train_data_file = "ml-100k/u1.base"
    #split_str = '\t'
    data_file = RATINGS_ALL_FILE 
    train_data_file = RATINGS_BASE_FILE
    #split_str = ','
    all_data = sc.textFile(data_file)
    ratings = all_data.map(lambda l: re.split('\s|,',l))\
                     .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

    K =int(sys.argv[5])
    train_data = sc.textFile(train_data_file)
    base_ratings = train_data.map(lambda l: re.split('\s|,',l))\
        .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))
    #print ( time.time())

    # Build the recommendation model using Alternating Least Squares
    rank = int( sys.argv[1] )
    numIterations = int(sys.argv[2])
    lamb= float(sys.argv[3])
    alpha = float(sys.argv[4])
#    print ("rank = " + str(rank))
#    print ("numIterations = " + str(numIterations))
#    print ("lamb = " + str(lamb))
    model = ALS.trainImplicit(ratings=base_ratings, rank=rank, iterations=numIterations, lambda_=lamb,  alpha=alpha)
    #model = ALS.train(base_ratings, rank, numIterations, lamb)
    #model.save(sc,"./model/")
    #print ( time.time())

    # Evaluate the model on training data
    testdata = ratings.map(lambda p: (p[0], p[1]))
    predictions_all = model.predictAll(testdata)
    predictions = predictions_all.map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
#    print("Mean Squared Error = " + str(MSE))
    #print ( time.time())


    topKRecs4users = model.recommendProductsForUsers(K).flatMap(lambda l: l[1])\
                                                       .map(lambda r : (r.user,r.product))\
                                                       .groupByKey().mapValues(list)\
                                                       .sortByKey()
#[(1,  (Rating(user=1, product=272, rating=5.332170229745589),
#[Rating(user=1, product=272, rating=5.332170229745589),
# [ (1,272),(1,154),......]
#[(1, [272, 154, 285, 100, 188, 178, 64, 172, 48, 210]),
    #print (topKRecs4users.take(10))
    movies4Users = ratings.sortBy(lambda l: l.rating,ascending=False)\
                          .map(lambda r : (r.user,r.product))\
                          .groupByKey().mapValues(list)\
                          .sortByKey()\
                          .map(lambda l: (l[0],l[1][:K]))
    #print (movies4Users.take(10))
    map_ready_usr = topKRecs4users.join(movies4Users)\
                              .sortByKey()
    map_ready = map_ready_usr.map(lambda l: l[1])
    #print (map_ready.take(10))
    #print ( time.time())
    metrics = RankingMetrics(map_ready)
    MAPK    = metrics.meanAveragePrecision
    #print (MAPK)
    log_path_pre = RESULT_LOG_DIR
    log_path= log_path_pre+sys.argv[1]+"_"+sys.argv[2]+"_"+sys.argv[3]+"_"+sys.argv[4]+"_"+sys.argv[5]+"_log"
    if( os.path.exists(log_path)):
        __import__('shutil').rmtree(log_path)
        print(log_path+": already exists!")
    
    #map_ready_usr.repartition(1).saveAsTextFile(log_path)
    #print ( time.time())

    end_time = time.time()
    run_time = end_time - start_time
    f = open(RESULT_FILE,'w+')
    print >>f,(sys.argv[1]+"\t"+sys.argv[2]+"\t"+sys.argv[3]+"\t"+sys.argv[4]+"\t"+sys.argv[5]+"\t"+str(MSE)+"\t"+str(MAPK)+"\t"+str(run_time))
    f.close()
