#!/bin/bash
spark-submit --num-executors 1 --executor-cores 1 --executor-memory 1G --master  spark://master:7077 \
	/home/spark/PythonProjects/twstock_ETL_spark/import_transactions_hdfs_daily.py
