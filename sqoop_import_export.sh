#!/usr/bin/env bash
set -e

# Edit the connection URL and username to match your Cloudera lab.
# The -P option prompts for the MySQL password.
# Ensure the MySQL JDBC driver and Sqoop are installed.

sqoop import \
  --connect jdbc:mysql://localhost:3306/bdctdb \
  --username root -P \
  --table students \
  --target-dir /user/cloudera/sqoop/students \
  --num-mappers 1

hdfs dfs -ls /user/cloudera/sqoop/students
hdfs dfs -cat /user/cloudera/sqoop/students/part-m-00000

# Import the MySQL table directly into an existing Hive database.
sqoop import \
  --connect jdbc:mysql://localhost:3306/bdctdb \
  --username root -P \
  --table students \
  --hive-import \
  --hive-database bdct \
  --hive-table students_sqoop \
  --create-hive-table \
  --num-mappers 1

# Export requires the MySQL destination table students_export to exist first.
sqoop export \
  --connect jdbc:mysql://localhost:3306/bdctdb \
  --username root -P \
  --table students_export \
  --export-dir /user/cloudera/sqoop/export \
  --input-fields-terminated-by '\t' \
  --num-mappers 1
