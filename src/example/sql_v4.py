#! usr/bin/python
# -*- coding: utf-8 -*-
# author:liuyueji
# data:2016.8.13



# v4: 
# insert clothing and usr 
# change insert method

import MySQLdb
from datetime import timedelta,datetime
HOST = '10.105.50.42'
USER = 'analysis'
PWD = 'qinglong123'
DB = 'interface'
PORT = 3306
import time
import re
from datetime import datetime
import random
from pyspark import SparkContext

if __name__ == "__main__":
    result_file = '../ml-20m/movies.csv' 
    f = open(result_file,"r")
    sc = SparkContext(appName="sql_insert_clothing_usr")
    try:
        
        db=MySQLdb.connect(host=HOST, user=USER, passwd=PWD, db=DB, port=PORT)
        cursor=db.cursor()
        print 'start:',time.time()
        data1 = sc.textFile(result_file)
        print 'data1:',time.time()
        data2 = data1.map(lambda line :line.encode("utf-8") )  
        print 'data2:',time.time()
        data3 = data2.filter(lambda line : line!='movieId,title,genres')  
        print 'data3:',time.time()
        data4  = data3.map(lambda line : [re.search(r"^\d+,",line).group(0),\
                                          re.search(r"\"(.+)\"",line).group(0) if re.search(r"\"(.+)\"",line) else re.search(r",(.+),",line).group(0)])\
                      .map(lambda line : [long(line[0].strip(',')),line[1].strip('\"').strip(',')[:255],1])\
                      .collect()  
        print len(data4)
        print data4
        print 'data collect:',time.time()
        usr_time = datetime.now()
        usr = sc.parallelize(range(1,138494))\
                .map(lambda l : [long(l),long(l/10), 2 if (l%2 == 0) else 1,random.randint(0,70),1,usr_time,usr_time])\
                .collect()
        #print usr[0:15]
        
        sql_usr = """ insert into dt_user_charbase(charid,uid,sex,age,status,create_at,delete_at) value(%s,%s,%s,%s,%s,%s,%s)"""
        sql_clothing = """ insert into dt_product_clothing(id,name,prefer) value(%s,%s,%s)"""

        #cursor.executemany(sql_usr,[usr[138492]])
        #cursor.executemany(sql_usr,usr)
        #print 'usr insert:',time.time()
        cursor.executemany(sql_clothing,data4)
        print 'clothing insert:',time.time()

        db.commit()
        print 'db commit:',time.time()

        #cmd = "select * from %s"%table + " where item_type=1 and uid=1 "
        #cursor.execute(cmd)
        #print type(list(cursor.fetchall())[0][1])
        #print list(cursor.fetchall())
        cursor.close()
        db.close()
     
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])





    f.close()

