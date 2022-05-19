#!/bin/bash

url=$1

echo "|---- Getting data from a URL ----|"
wget -O file.csv $url

echo "|---- Putting file on Data lake ----|"
hdfs dfs -put -f file.csv /data/ingestion

echo "|---- Delete file from File System ----|"
rm -f file.csv