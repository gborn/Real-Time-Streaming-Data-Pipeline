#!/bin/bash

spark-submit \
        --master yarn\
        --jars /opt/spark_workarea/spark_dep_jars/postgresql-42.2.16.jar,\
               /opt/spark_workarea/spark_dep_jars/spark-sql-kafka-0-10_2.12-3.0.1.jar,\
               /opt/spark_workarea/spark_dep_jars/kafka-clients-2.6.0.jar,\
               /opt/spark_workarea/spark_dep_jars/spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar \
        --conf spark.executor.extraClassPath=/opt/spark_workarea/spark_dep_jars/postgresql-42.2.16.jar:\
               /opt/spark_workarea/spark_dep_jars/spark-sql-kafka-0-10_2.12-3.0.1.jar:\
               /opt/spark_workarea/spark_dep_jars/kafka-clients-2.6.0.jar:\
               /opt/spark_workarea/spark_dep_jars/spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar \
        --conf spark.executor.extraLibrary=/opt/spark_workarea/spark_dep_jars/postgresql-42.2.16.jar:\
               /opt/spark_workarea/spark_dep_jars/spark-sql-kafka-0-10_2.12-3.0.1.jar:\
               /opt/spark_workarea/spark_dep_jars/kafka-clients-2.6.0.jar: \
               /opt/spark_workarea/spark_dep_jars/spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar \
        --conf spark.driver.extraClassPath=/opt/spark_workarea/spark_dep_jars/postgresql-42.2.16.jar:\
               /opt/spark_workarea/spark_dep_jars/spark-sql-kafka-0-10_2.12-3.0.1.jar:\
               /opt/spark_workarea/spark_dep_jars/kafka-clients-2.6.0.jar:\
               /opt/spark_workarea/spark_dep_jars/spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar
       --py-files realtime_streaming_pipeline.zip
       app.py