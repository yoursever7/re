# !usr/bin/python
# utils
# author:zhangyx
# since 2016-09-15

from __future__ import print_function

import sys
import MySQLdb
import random

from config import *


def LoadDataFromSQL(members, table):
    result_list = []
    try:
        db=MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PWD, db=MYSQL_DB, port=MYSQL_PORT)
        cursor=db.cursor()
        cmd = "select " + members + " from " + table
        #print cmd
        cursor.execute(cmd)
        result_list.extend( list(cursor.fetchall()))
        cursor.close()
        db.close()
    except MySQLdb.Error,e:
        print("Mysql Error %d: %s"%(e.args[0], e.args[1]))
    return result_list


def LoadDataFromSQL(members, table, condition):
    result_list = []
    try:
        db=MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PWD, db=MYSQL_DB, port=MYSQL_PORT)
        cursor=db.cursor()
        cmd = "select " + members + " from " + table
        if condition != "":
            cmd = cmd + " where " + condition
        #print cmd
        cursor.execute(cmd)
        result_list.extend( list(cursor.fetchall()))
        cursor.close()
        db.close()
    except MySQLdb.Error,e:
        print("Mysql Error %d: %s"%(e.args[0], e.args[1]))
    return result_list


def executeSQL(cmd):
    try:
        db=MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PWD, db=MYSQL_DB, port=MYSQL_PORT)
        cursor=db.cursor()

        cursor.execute(cmd)
        db.commit()
        cursor.close()
        db.close()
    except MySQLdb.Error,e:
        print("Mysql Error %d: %s"%(e.args[0], e.args[1]))


def executeSQLinDB(db, cmd):
    try:
        cursor=db.cursor()
        cursor.execute(cmd)
        db.commit()
        cursor.close()
    except MySQLdb.Error,e:
        print("Mysql Error %d: %s"%(e.args[0], e.args[1]))


def executemanySQLinDB(db, cmd, data):
    try:
        cursor=db.cursor()
        cursor.executemany(cmd, data)
        db.commit()
        cursor.close()
    except MySQLdb.Error,e:
        print("Mysql Error %d: %s"%(e.args[0], e.args[1]))

