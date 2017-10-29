#!/usr/bin/env bash
hdfs dfs -copyFromLocal /spark-core /spark-core
TEST_RESOURCES='/spark-core/dataset/local/'
hdfs dfs -rm -r /results
spark-submit --conf spark.ui.port=10500 --class com.epam.hubd.spark.scala.core.homework.MotelsHomeRecommendation target/spark-sql-hw.jar $TEST_RESOURCES/bids.txt $TEST_RESOURCES/motels.txt $TEST_RESOURCES/exchange_rate.txt /results
hdfs dfs -cat /results/aggregated/* | head -1000