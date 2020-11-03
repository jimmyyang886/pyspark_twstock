#!/bin/bash
spark-submit --num-executors 3 --executor-cores 3 --executor-memory 1G --master \
      	spark://master:7077 import_broker_hdfs_dialy.py
