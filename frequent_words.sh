#!/usr/bin/env bash
set -e

RESULT='/user/cloudera/wordcount/output/part-r-00000'

echo "Words sorted by frequency (highest first):"
hdfs dfs -cat "$RESULT" | sort -k2,2nr

echo
echo "Most frequent word:"
hdfs dfs -cat "$RESULT" | sort -k2,2nr | head -1
