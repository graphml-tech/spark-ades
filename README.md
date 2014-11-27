Spark ADES Application
==============

## Introduction
This project is to rewrite for ADES(https://github.com/cloudera/ades) using Spark. I port pig UDF into spark programming like bin, quantile, and choose2, ebgm, etc. As a result, we have an unified pipelines.

## Build
You can build the application via maven or sbt.

1. maven:
mvn install

2. sbt: 
sbt/sbt assembly

## Run the application
1. Please refer to cloudera ades to prepare for AERS data under hdfs.
2. Download Spark https://spark.apache.org/downloads.html, by default,it uses hadoop 1.0.4, your HDFS env should be 1.0.4 to be compatible.
3. Submit spark ADES application to spark, e.g.

  bin/spark-submit --class com.spark.ades.ADE --master local[2] --conf spark.driver.memory=3g \
      ~/javaprojects/hadoop/spark-aersapp/target/sparkades-0.0.1-SNAPSHOT-jar-with-dependencies.jar

This will run the application in a single local process.  If the cluster is running a Spark standalone
cluster manager, you can replace "--master local" with "--master spark://`<master host>`:`<master port>`".

## Pipelines
You can execute step one by one since every pig script in ADES has the corresponding Spark main class. Here is the following mapping,

1. Step 1 => com.spark.ades.Step1ETL
2. Step 2 => com.spark.ades.Step2JoinAll
3. Step 3 => com.spark.ades.Step3DrugReactStat
4. Step 4 => com.spark.ades.Step4ApplyEbgm
5. All steps => com.spark.ades.ADE



