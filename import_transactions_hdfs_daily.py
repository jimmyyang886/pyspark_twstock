#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkConf, SparkContext, sql
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
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

    schema = """sID STRING, Date Date, capacity INTEGER, turnover LONG ,
        open FLOAT, high FLOAT, low FLOAT, close FLOAT,
        change FLOAT, transaction INTEGER """
    
    transactions = spark.read.jdbc(
                                   url = "jdbc:mysql://192.168.246.128:3306/twstock",
                                   table = f"(SELECT * FROM transactions WHERE Date = '{date}')AS my_table",
                                   properties = {'user': 'teb101Club', 'password': 'teb101Club', 'customSchema' : schema} 
                                  )

    #transactions.show(10)
    #transactions.printSchema()
    #transactions = transactions.withColumn("date", transactions["date"].cast(DateType()))


    #transactions = transactions.select(
    #    transactions.sID.cast(StringType()),
    #    transactions.Date.cast(DateType()),
    #    transactions.capacity.cast(DecimalType()),
    #    transactions.turnover.cast(DecimalType()),
    #    transactions.open.cast(FloatType()),
    #    transactions.high.cast(FloatType()),
    #    transactions.low.cast(FloatType()),
    #    transactions.close.cast(FloatType()),
    #    transactions.change.cast(FloatType()),
    #    transactions.transaction.cast(IntegerType()),
    #)

    #transactions.printSchema()
    #transactions.show(5)

    path = "hdfs://master/user/spark/twstock/raw_data/transactions"
    #transactions.repartition(1).write.mode('overwrite').parquet(f"{path}/{date}.parquet")
    #transactions.coalesce(1).write.mode('overwrite').parquet(f"{path}/{date}.parquet")
    transactions.write.mode('overwrite').parquet(f"{path}/{date}.parquet")
        

if __name__=='__main__':

    if datetime.now().hour>14:
        Curday=datetime.today().date().isoformat()
    else:
        Curday=(datetime.today()-timedelta(days=1)).date().isoformat()

    transactions2parquet(Curday)
    

