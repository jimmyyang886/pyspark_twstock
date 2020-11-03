#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkConf, SparkContext, sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import *

def history_report(year_month):
    sc = SparkContext()
    spark = SparkSession.builder\
                        .appName("history_report")\
                        .getOrCreate()

    history_report = spark.read.parquet(f"hdfs://master//user/spark/twstock/analysis/daily_report/{year_month}*.parquet")
    history_report.createOrReplaceTempView("history_report")

    sql = "SELECT sID,bID,Date,total_buy,total_sell,\
                  (total_buy*avg_buy_price) as cost,(total_sell*avg_sell_price) as recieve,\
                  storage,close\
           FROM history_report\
           ORDER BY sID,bID,Date"
    ## Using Window to cumsum data
    window = Window.orderBy("Date").partitionBy("sID","bID").rangeBetween(Window.unboundedPreceding, 0)
    history_report = spark.sql(sql).withColumn('cumsum_storage', sum('storage').over(window))\
                                   .ithColumn('cumsum_cost', (sum('cost')).over(window))\
                                   .withColumn('cumsum_buy', (sum("total_buy")).over(window))\
                                   .withColumn('cumsum_recieve', (sum('recieve')).over(window))\
                                   .withColumn('cumsum_sell', (sum('total_sell')).over(window))


    
    # alias column name
    history_report = history_report.select("sID","bID","Date","cumsum_cost","cumsum_buy","cumsum_recieve","cumsum_sell",\
                                           (col("cumsum_cost") / col("cumsum_buy")).alias("avg_buy_price"),\
                                           (col("cumsum_recieve") / col("cumsum_sell")).alias("avg_sell_price"),\
                                           "close","cumsum_storage")
    # changing Type of data
    history_report = history_report.withColumn("avg_buy_price",col("avg_buy_price").cast(FloatType()))\
                                   .withColumn("avg_sell_price",col("avg_sell_price").cast(FloatType()))\
                                   .withColumn("cumsum_storage",col("cumsum_storage").cast(FloatType()))

    history_report.write.mode("append").parquet(f"/user/admin/twstock/analysis/history/")
#     cumsum_report.repartition(1).write.format('jdbc')\
#                                     .options(
#                                              url="jdbc:mysql://IP:3306/database_name?useSSL=false&rewriteBatchedStatements=true",
#                                              driver='com.mysql.jdbc.Driver',
#                                              dbtable='table_name',
#                                              user='user_name',
#                                              password='user_password'
#                                              ).mode('append').save()


spark = SparkSession.builder\
                       .appName("history_report")\
                       .getOrCreate()

history_report = spark.read.parquet("hdfs://master//user/spark/twstock/analysis/broker_transaction/DR/2020-10-30")
history_report.show(10)
