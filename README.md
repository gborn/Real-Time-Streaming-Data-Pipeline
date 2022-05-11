# Real-Time Streaming Data Pipeline
This is a Proof-of-concept for building a data pipeline to process streaming data using Kafka and Spark Streaming in a Hadoop Cluster.

For our use case we consider data centers spread across multiple regions that generate large amount of real time events such as server status. In this PoC we designed a data pipeline to process these data in real-time and generate insights which can be used to monitor amd track server status regularly, and thus helping resolve server issues faster. Since the data is huge and coming in real-time, we need to choose the right architecture with scalable storage and computation frameworks/technologies. Hence we build a Real Time Data Pipeline Using Apache Kafka, Apache Spark, Hadoop, PostgreSQL in a docker environment.

The pipeline can be run with other sources of streaming data like tweets, stocks, etc. with minimal changes.

Data pipeline steps:
* Generate streams of data that are written to Kafka queue
* Pull events from Kafka, process and transform stream data using Spark Streaming
* Store raw data in HDFS and processed data in RDBMS table
* Build a dashboard using Django to monitor events in real time
