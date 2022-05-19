#!/bin/bash

execution=$1

spark-submit --verbose \
  --py-files ../source/constants.py \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 \
  --deploy-mode client \
  --conf spark.eventLog=enabled \
  --conf spark.eventLog.dir=/home/spark \
  ../source/$execution