#!/bin/bash

# === configure ===
JAR_NAME=moviesim.jar
MAIN_CLASS=MovieSimilarities
# INPUT_LOCAL=input/u.data
INPUT_LOCAL=input/ratings.dat
# MOVIE_META=u.item
MOVIE_META=movies.dat
INPUT_HDFS=/user/$USER/moviesim_input
OUTPUT_HDFS=/user/$USER/moviesim_output
HADOOP_CLASSPATH=$(hadoop classpath)

# === Compile Java files ===
echo "Compiling Java files..."
rm -rf classes
mkdir -p classes
javac -classpath "$HADOOP_CLASSPATH" -d classes *.java

# === Package jar ===
echo "Creating jar file..."
jar -cvf $JAR_NAME -C classes/ .

# === Upload data to HDFS ===
echo "Uploading input data to HDFS..."
hadoop fs -rm -r -f $INPUT_HDFS
hadoop fs -rm -r -f $OUTPUT_HDFS
hadoop fs -rm -r -f /user/$USER/step1_output
hadoop fs -rm -r -f /user/$USER/step2_output
hadoop fs -rm -r -f /user/$USER/step3_output
hadoop fs -rm -r -f /user/$USER/moviesim_output

hadoop fs -mkdir -p $INPUT_HDFS
hadoop fs -put -f $INPUT_LOCAL $INPUT_HDFS/
hadoop fs -put -f $MOVIE_META /user/$USER/

# === Run MapReduce job ===
echo "Running Hadoop job..."
hadoop jar $JAR_NAME $MAIN_CLASS \
  $INPUT_HDFS \
  $OUTPUT_HDFS \
  -files $MOVIE_META

# === 下载输出 ===
echo "Job completed. Fetching output..."
rm -f output.txt
# hadoop fs -ls /user/$USER/moviesim_output

hadoop fs -getmerge $OUTPUT_HDFS output.txt
head -n 30 output.txt