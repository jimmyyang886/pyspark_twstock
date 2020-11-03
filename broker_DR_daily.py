#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkConf, SparkContext, sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import subprocess
import re
from datetime import datetime, timedelta

def daily_report(date):
    ## Maybe reading data from mysql is better
    
    ## Create table "Transaction"
    spark = SparkSession.builder\
                        .appName("daily_report")\
                        .getOrCreate()

    Transaction = spark.read.parquet(f"hdfs://master/user/spark/twstock/raw_data/broker_transaction/{date}.parquet")
    Transaction.createOrReplaceTempView("Transaction")
    Transaction = spark.sql("SELECT sID,bID,Date,buy,buy*price cost,sell,sell*price recieve,price FROM Transaction")

    ## Create table "Open_Close"
    Open_Close = spark.read.parquet(f"hdfs://master/user/spark/twstock/raw_data/transactions/{date}.parquet")
    Open_Close = Open_Close.createOrReplaceTempView("Open_Close")
    Open_Close = spark.sql("SELECT sID,open,close FROM Open_Close")

    ## Combine "Transaction" + "Open_Close"
    # aggregate "sum" and "avg_price"
    daily_tbl = Transaction.join(Open_Close,"sID")

    basic_agg = daily_tbl.groupBy("sID","bID","Date","open","close")\
                         .agg(sum("buy").alias("total_buy"),
                              sum("cost").alias("total_cost"),
                              sum("sell").alias("total_sell"),
                              sum("recieve").alias("total_recieve"),
                              (sum("buy") - sum("sell")).alias("storage"),
                              (sum("cost") / sum("buy")).alias("avg_buy_price"),
                              (sum("recieve") / sum("sell")).alias("avg_sell_price")
                              )
    ## Fillna
    basic_agg = basic_agg.na.fill(0, "avg_buy_price").na.fill(0, "avg_sell_price")

    ## Split "day_trade" "over_bought" "over_sold"
    # when() to set "if,elif,else" 
    over_bought = basic_agg.filter(basic_agg["storage"] > 0)\
                           .groupBy("sID","bID","Date","open","close",
                                    "total_buy","total_cost","total_sell","total_recieve",
                                    "storage","avg_buy_price","avg_sell_price")\
                           .agg(sum("total_recieve").alias("realize_ProfitLoss"),
                                (sum("storage") * (sum("close") - sum("avg_buy_price"))).alias("unrealize_ProfitLoss"),
                                (sum("total_recieve") + (sum("storage") * (sum("close") - sum("avg_buy_price")))).alias("total_ProfitLoss"),
                                ((sum("total_recieve") + (sum("storage") * (sum("close") - sum("avg_buy_price")))) / 
                                sum("total_cost")).alias("return_rate_%"),
                               )

    over_sold = basic_agg.filter(basic_agg["storage"] < 0)\
                         .groupBy("sID","bID","Date","open","close",
                                  "total_buy","total_cost","total_sell","total_recieve",
                                  "storage","avg_buy_price","avg_sell_price")\
                         .agg((sum("total_recieve") - sum("total_cost")).alias("realize_ProfitLoss"),
                              (sum("storage") * 0).alias("unrealize_ProfitLoss"),
                              (sum("total_recieve") - sum("total_cost")).alias("total_ProfitLoss"),
                              (when((sum("total_sell") * sum("avg_buy_price")) > 0,
                                    (sum("total_recieve") - sum("total_cost")) / (sum("total_sell") * sum("avg_buy_price")))
                              ).alias("return_rate_%"),
                             )

    day_trading = basic_agg.filter(basic_agg["storage"] == 0)\
                           .groupBy("sID","bID","Date","open","close",
                                    "total_buy","total_cost","total_sell","total_recieve",
                                    "storage","avg_buy_price","avg_sell_price")\
                           .agg((sum("total_recieve") - sum("total_cost")).alias("realize_ProfitLoss"),
                                (sum("storage")).alias("unrealize_ProfitLoss"),
                                (sum("total_recieve") - sum("total_cost")).alias("total_ProfitLoss"),
                                ((sum("total_recieve") - sum("total_cost")) / sum("total_cost")).alias("return_rate_%"),
                               )


    daily_report = over_bought.union(over_sold).union(day_trading)
    # use withColumn() changing "fixed-point" of data
    # use withColumn() changing "Type" of data
    daily_report = daily_report.withColumn("open", round(daily_report["open"], 2))\
                               .withColumn("close", round(daily_report["close"], 2))\
                               .withColumn("total_cost", round(daily_report["total_cost"], 2))\
                               .withColumn("total_recieve", round(daily_report["total_recieve"], 2))\
                               .withColumn("avg_buy_price", round(daily_report["avg_buy_price"], 2))\
                               .withColumn("avg_sell_price", round(daily_report["avg_sell_price"], 2))\
                               .withColumn("realize_ProfitLoss", round(daily_report["realize_ProfitLoss"], 2))\
                               .withColumn("unrealize_ProfitLoss", round(daily_report["unrealize_ProfitLoss"], 2))\
                               .withColumn("total_ProfitLoss", round(daily_report["total_ProfitLoss"], 2))\
                               .withColumn("return_rate_%", round(daily_report["return_rate_%"], 2))

    daily_report = daily_report.withColumn("sID",col("sID").cast(StringType()))\
                               .withColumn("bID",col("bID").cast(StringType()))\
                               .withColumn("Date",col("Date").cast(DateType()))\
                               .withColumn("open",col("open").cast(FloatType()))\
                               .withColumn("close",col("close").cast(FloatType()))\
                               .withColumn("total_buy",col("total_buy").cast(IntegerType()))\
                               .withColumn("total_cost",col("total_cost").cast(FloatType()))\
                               .withColumn("total_sell",col("total_sell").cast(IntegerType()))\
                               .withColumn("total_recieve",col("total_recieve").cast(FloatType()))\
                               .withColumn("storage",col("storage").cast(IntegerType()))\
                               .withColumn("avg_buy_price",col("avg_buy_price").cast(FloatType()))\
                               .withColumn("avg_sell_price",col("avg_sell_price").cast(FloatType()))\
                               .withColumn("realize_ProfitLoss",col("realize_ProfitLoss").cast(FloatType()))\
                               .withColumn("unrealize_ProfitLoss",col("unrealize_ProfitLoss").cast(FloatType()))\
                               .withColumn("total_ProfitLoss",col("total_ProfitLoss").cast(FloatType()))\
                               .withColumn("return_rate_%",col("return_rate_%").cast(FloatType()))

    ## Saving parquet()
    # depending on size of partition to use repartition()

    # Example for saving to mysql
#     .write.format('jdbc').options(
#                                   url="jdbc:mysql://120.97.27.92:3306/twstock?useSSL=false&rewriteBatchedStatements=true",
#                                   driver='com.mysql.jdbc.Driver',
#                                   dbtable='table_name',
#                                   user='user_name',
#                                   password='user_password'
#                                  ).mode('append').save()
    daily_report.repartition(1).write.mode('overwrite').parquet(f"hdfs://master/user/spark/twstock/analysis/broker_transaction/DR/{date}")


if __name__=='__main__':

    if datetime.now().hour>14:
        Curday=datetime.today().date().isoformat()
    else:
        Curday=(datetime.today()-timedelta(days=1)).date().isoformat()

    daily_report(Curday)



