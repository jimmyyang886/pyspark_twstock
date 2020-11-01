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

    uname = 'xxxxx'
    passwd = 'xxxxx'
    
    schema = "bID STRING, sID STRING, Date Date, sn INTEGER, price FLOAT, buy INTEGER, sell INTEGER"
    
    broker_transaction = spark.read.jdbc(
                                         url = "jdbc:mysql://192.168.246.128:3306/twstock",
                                         table = f"(SELECT * FROM broker_transaction WHERE Date = '{date}')AS my_table",
                                         properties = {'user': uname, 'password': passwd, 'customSchema' :schema}
                                        )
    #broker_transaction.printSchema()
    #broker_transaction.show(5)

    path = "hdfs://master/user/spark/twstock/raw_data/broker_transaction"
    print(f"wrie to {path}/{date}.parquet")
    broker_transaction.write.mode('overwrite').parquet(f"{path}/{date}.parquet")
    #broker_transaction.unpersist()

    

if __name__=='__main__':

    broker_transaction2parquet(datetime.today().date().isoformat())


