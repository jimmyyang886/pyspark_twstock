#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkConf, SparkContext, sql
from pyspark.sql import SparkSession
from datetime import datetime
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
    #transactions.repartition(1).write.mode('overwrite').parquet(f"{path}/{date}.parquet")
    #transactions.coalesce(1).write.mode('overwrite').parquet(f"{path}/{date}.parquet")
    transactions.write.mode('overwrite').parquet(f"{path}/{date}.parquet")
        

if __name__=='__main__':

    transactions2parquet(datetime.today().date().isoformat())
    

