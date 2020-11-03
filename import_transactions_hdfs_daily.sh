#!/bin/bash
/home/spark/spark-2.4.5-bin-hadoop2.7/bin/spark-submit \
--num-executors 1 --executor-cores 1 --executor-memory 1G --master  spark://master:7077 \
/home/spark/PythonProjects/twstock_ETL_spark/import_transactions_hdfs_daily.py
