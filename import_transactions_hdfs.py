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


def transactions2parquet(date):
    #sc = SparkContext()
    spark = SparkSession.builder\
                        .appName("Import_transactions")\
                        .getOrCreate()


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
    return transactions.write.mode('overwrite').parquet(f"{path}/{date}.parquet")
        

if __name__=='__main__':

    uname = 'xxxxx'
    passwd = 'xxxxx'
    #date = '2020-05-22'
    #transactions2parquet(date)

    spark = SparkSession.builder\
                        .appName("Get_Transactions_Date")\
                        .getOrCreate()

    dateDF = spark.read.jdbc(url = "jdbc:mysql://192.168.246.128:3306/twstock",
                              table = f"(SELECT distinct Date FROM broker_transaction)AS my_table",
                              properties = {'user': uname, 'password': passwd})

    #dateDF.show()

    #sc = SparkContext()
    #dateList = ['2020-05-22', '2020-05-25']
    #dateRDD = sc.parallelize(dateList)
    #dateRDD.map(transactions2parquet).collect() 
    #datelist=dateDF.rdd.map(lambda x: x[0].isoformat())
    #datelist.take(5)

    dateRDD=dateDF.rdd.map(lambda x: x[0].isoformat())
    #for n in dateRDD.collect():
    #    print(n)
    dateRDD.map(transactions2parquet).collect() 

