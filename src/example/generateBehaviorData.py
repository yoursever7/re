# !usr/bin/python
# generate behavior data
# author:xiefq
# since 2016-07-23


from __future__ import print_function

import sys
import MySQLdb
import random

from config import *

def excuteSQL(cmd):
    try:
        db=MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PWD, db=MYSQL_DB, port=MYSQL_PORT)
        cursor=db.cursor()

        cursor.execute(cmd)
        db.commit()
        cursor.close()
        db.close()
    except MySQLdb.Error,e:
        print ("Mysql Error %d: %s" % (e.args[0], e.args[1])) 


# MAIN
if __name__ == "__main__":   

	file_user = open(USERS_PATH)

	products=[]

	db=MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PWD, db=MYSQL_DB, port=MYSQL_PORT)
	cursor=db.cursor()
	cmd ="select id from dt_product_clothing where prefer=1" 
	cursor.execute(cmd)
	results = cursor.fetchall()
	for row in results:
		products.append(row[0])
	cursor.close()
	db.close()
		
	products_size=len(products)
	print(products)
	each_items = 10;


	for user in file_user:
		user = user.strip('\n')
		for x in range(0,each_items):
			item = random.randint(0,products_size-1)
			#print(item)
			#sql = "insert into dt_behavior_product_neglect(charid,item_type,item_id,time) values(" + str(user)+ ",1," +str(products[item]) + ")"
			sql = "insert into dt_behavior_product_try(charid,item_type,item_id) values(" + str(user)+",1,"+str(products[item])+")"
			#print(sql)
			excuteSQL(sql)
			#sql = "insert into dt_behavior_product_collect(uid,item_type,item_id) values(" + str(user)+",1,"+str(products[item])+")"
			#excuteSQL(sql)
