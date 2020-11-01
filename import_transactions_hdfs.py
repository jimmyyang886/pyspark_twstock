#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkConf, SparkContext, sql
from pyspark.sql import SparkSession
from datetime import datetime
import os, subprocess
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

#def transactions2parquet(path, date, uname, passwd):
def transactions2parquet(date):

    #sc = SparkContext()
    spark = SparkSession.builder\
                        .appName("Import_transactions")\
                        .getOrCreate()

#    schema = StructType([StructField("sID", StringType()),
#        StructField("Date", DateType()), 
#        StructField("capacity", DecimalType()),
#        StructField("turnover", DecimalType()),
#        StructField("open", FloatType()),
#        StructField("high", FloatType()),
#        StructField("low", FloatType()),
#        StructField("close", FloatType()),
#        StructField("change", FloatType()),
#        StructField("transaction", DecimalType())])

    uname = 'xxxxx'
    passwd = 'xxxxx'
    
    schema = """sID STRING, Date Date, capacity INTEGER, turnover LONG ,
        open FLOAT, high FLOAT, low FLOAT, close FLOAT,
        change FLOAT, transaction INTEGER """
    
    transactions = spark.read.jdbc(
                                   url = "jdbc:mysql://192.168.246.128:3306/twstock",
                                   table = f"(SELECT * FROM transactions WHERE Date = '{date}')AS my_table",
                                   #customSchema = schema,
                                   properties = {'user': uname, 'password': passwd, 'customSchema' : schema} 
                                  )


    path = "hdfs://master/user/spark/twstock/raw_data/transactions"
    transactions.write.mode('overwrite').parquet(f"{path}/{date}.parquet")
        

if __name__=='__main__':
    sc = SparkContext()

    source_dir= "/user/spark/twstock/raw_data/transactions"
    cmd = 'hdfs dfs -find {} -name *.parquet'.format(source_dir).split()
    files = subprocess.check_output(cmd).decode().strip().split('\n')

    #def finddate(x):
    #    re.findall('[0-9]{4}-[0-9]{2}-[0-9]{2}', _file)[0]

    ExDaterdd = sc.parallelize(files)
    ExDaterdd=ExDaterdd.map(lambda x: re.findall('[0-9]{4}-[0-9]{2}-[0-9]{2}', x)[0])
    #print(ExDaterdd.collect())

    ExDateCounts = ExDaterdd.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a+b)
    ExDaterdd = ExDaterdd.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a+b)

    spark = SparkSession.builder\
                        .appName("exist date")\
                        .getOrCreate()

    ExDatedf=spark.createDataFrame(ExDaterdd,['date', 'count'])
    ExDatedf=ExDatedf.filter("count >= 2")

    
    uname = 'xxxxx'
    passwd = 'xxxxx'

    spark = SparkSession.builder\
                        .appName("Get_Transactions_Date")\
                        .getOrCreate()

    dateDF = spark.read.jdbc(url = "jdbc:mysql://192.168.246.128:3306/twstock",
                              table = f"(SELECT distinct Date FROM transactions)AS my_table",
                              properties = {'user': uname, 'password': passwd})

    dateRDD=dateDF.join(ExDatedf, on='date', how='leftouter')\
            .filter("count is null").filter("date>'2020-05-22'")\
            .select('date').sort("date").rdd.map(lambda x: x[0].isoformat())

    dateRDD.foreach(transactions2parquet)
    

