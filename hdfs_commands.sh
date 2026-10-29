#!/usr/bin/env bash
set -e

hdfs dfs -mkdir -p /user/cloudera/bdct/input
mkdir -p ~/bdct_lab
cd ~/bdct_lab

printf 'hello hadoop\n' > file1.txt
printf 'big data lab\n' > file2.txt

hdfs dfs -put -f file1.txt file2.txt /user/cloudera/bdct/input/
hdfs dfs -ls /user/cloudera/bdct/input
hdfs dfs -cat /user/cloudera/bdct/input/file1.txt

hdfs dfs -get -f /user/cloudera/bdct/input/file1.txt ~/bdct_lab/downloaded_file.txt
cat ~/bdct_lab/downloaded_file.txt
