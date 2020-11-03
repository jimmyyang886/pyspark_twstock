#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkConf, SparkContext, sql
from pyspark.sql import SparkSession
from datetime import datetime
import os
import subprocess
import re
#from pyspark.sql.types import StringType,DateType, FloatType, IntegerType, DecimalType

#DataTypeList
'''
'BinaryType', 'BooleanType', 'ByteType', 'DateType',
'DecimalType', 'DoubleType', 'FloatType', 'IntegerType',
LongType', 'ShortType', 'StringType', 'TimestampType'
'''
## Import data from mysql to hdfs
# Changing path, IP,  user_name, user_password for your system

        
def broker_transaction2parquet(date):
    spark = SparkSession.builder\
                        .appName("Import_broker_transaction")\
                        .getOrCreate()


    schema = "bID STRING, sID STRING, Date Date, sn INTEGER, price FLOAT, buy INTEGER, sell INTEGER"
    
    broker_transaction = spark.read.jdbc(
                                         url = "jdbc:mysql://192.168.246.128:3306/twstock",
                                         table = f"(SELECT * FROM broker_transaction WHERE Date = '{date}')AS my_table",
                                         properties = {'user': 'teb101Club', 'password': passwd, 'customSchema' :schema}
                                        )
    #broker_transaction.printSchema()
    #broker_transaction.show(5)

    path = "hdfs://master/user/spark/twstock/raw_data/broker_transaction"
    print(f"wrie to {path}/{date}.parquet")
    broker_transaction.write.mode('overwrite').parquet(f"{path}/{date}.parquet")
    #broker_transaction.unpersist()

    

if __name__=='__main__':
    sc = SparkContext()

    uname = 'teb101Club'
    passwd = 'teb101Club'

    source_dir= "/user/spark/twstock/raw_data/broker_transaction"


    cmd = 'hdfs dfs -find {} -name *.parquet'.format(source_dir).split()
    #cmd = 'hdfs dfs -find {} -name *.parquet'.format(source_dir)
    files = subprocess.check_output(cmd).decode().strip().split('\n')
    #files = subprocess.check_output(cmd)
    #print(files)
    #ExDateList=[]
    #for _file in files:
        #filename = path.split(os.path.sep)[-1].split('.parquet')[0]
    #    ExDateList.append(re.findall('[0-9]{4}-[0-9]{2}-[0-9]{2}', _file)[0])
        #print(re.findall('[0-9]{4}-[0-9]{2}-[0-9]{2}', _file))
    def finddate(x):
        re.findall('[0-9]{4}-[0-9]{2}-[0-9]{2}', _file)[0]

    ExDaterdd = sc.parallelize(files)
    ExDaterdd=ExDaterdd.map(lambda x: re.findall('[0-9]{4}-[0-9]{2}-[0-9]{2}', x)[0]) 
#    print(ExDaterdd.collect())


    ExDateCounts = ExDaterdd.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a+b)

    ExDaterdd = ExDaterdd.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a+b)

    spark = SparkSession.builder\
                        .appName("exist date")\
                        .getOrCreate()

    ExDatedf=spark.createDataFrame(ExDaterdd,['date', 'count'])
    ExDatedf=ExDatedf.filter("count >= 2")



    spark = SparkSession.builder\
                        .appName("Get_Transactions_Date")\
                        .getOrCreate()


    dateDF = spark.read.jdbc(url = "jdbc:mysql://192.168.246.128:3306/twstock",
                              table = f"(SELECT distinct Date FROM broker_transaction)AS my_table",
                              properties = {'user': uname, 'password': passwd})

    dateRDD=dateDF.join(ExDatedf, on='date', how='leftouter').filter("count is null").select('date').sort("date").rdd.map(lambda x: x[0].isoformat())
    #print(dateRDD.collect())
    #dateRDD.map(broker_transaction2parquet).collect().unpersist()
    dateRDD.foreach(broker_transaction2parquet)


    #date = '2020-07-08'
    #broker_transaction2parquet(date)
