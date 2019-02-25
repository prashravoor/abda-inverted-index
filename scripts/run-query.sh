#!/bin/bash

if [ $# -ne 3 ]; then
    echo "Usage: <Query Term> <Index Location> <Output Location>"
    exit 2
fi

if ! [ -e ~/abda/inverted-index/Query.jar ]; then
    echo Compiling Query Jar
    hadoop com.sun.tools.javac Main ~/abda/inverted-index/QueryDocs.java -d ~/abda/inverted-index/query
    jar cf ~/abda/inverted-index/Query.jar ~/abda/inverted-index/*.class
fi

echo Removing output directory "$3" from hdfs, if it exists
hdfs dfs -rm -r "$3" 2> /dev/null

hadoop jar ~/abda/inverted-index/CosineQuery.jar CosineQuery "$1" "$2" "$3" 2>&1 | tee out.log

if [ $? -ne 0 ]; then
    echo The Task has failed!
    exit 3
fi

echo The MapReduce tasks were successful. Results are

hdfs dfs -cat "$3"/part* 2> /dev/null | cut -f2
