#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./top-n-words.sh <N>"
    exit 1
fi

N=$1

# 1. Clean up
hadoop fs -rm -r -f /tmp/shakespeare_input /tmp/shakespeare_output 2>/dev/null
rm -f WordCount*.class WordCount.jar

# 2. Set up input - Using the loose files we saw in your home directory
hadoop fs -mkdir -p /tmp/shakespeare_input
hadoop fs -put shakespeare-*.txt /tmp/shakespeare_input/

# 3. Compile and Run
# Removed the .java extension here because the helper script adds it
./compile-map-reduce WordCount
./run-map-reduce WordCount /tmp/shakespeare_input /tmp/shakespeare_output

# 4. Sort and display Top N
hadoop fs -cat /tmp/shakespeare_output/part-* | sort -k2,2nr | head -n "$N" | awk '{print $1 " " $2}'

# 5. Final clean up
hadoop fs -rm -r -f /tmp/shakespeare_input /tmp/shakespeare_output
rm -f WordCount*.class WordCount.jar

