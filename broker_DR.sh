#!/bin/bash
/home/spark/spark-2.4.5-bin-hadoop2.7/bin/spark-submit \
--num-executors 3 --executor-cores 3 --executor-memory 1G --master \
spark://master:7077 broker_DR.py
