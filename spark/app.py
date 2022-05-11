from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as fn
from spark_utils import get_spark_session
from read import from_kafka
from transform import preprocess, transform
from write import to_hdfs, to_dbms, to_console
import os

# Kafka Broker/Cluster Details
HOST_IP_ADDRESS = os.environ.get('HOST_IP_ADDRESS')
KAFKA_TOPIC_NAME_CONS = "server-live-status"
KAFKA_BOOTSTRAP_SERVERS_CONS = f'{HOST_IP_ADDRESS}:9092'
# https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.12/3.0.1
# https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/2.6.0
# https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10-assembly_2.12/3.0.1

# PostgreSQL Database Server Details
postgresql_host_name = HOST_IP_ADDRESS
postgresql_port_no = "5432"
postgresql_user_name = "demouser"
postgresql_password = "demouser"
postgresql_database_name = "event_message_db"
postgresql_driver = "org.postgresql.Driver"
# https://mvnrepository.com/artifact/org.postgresql/postgresql/42.2.16
# --packages org.postgresql:postgresql:42.2.16

# https://mvnrepository.com/artifact/org.apache.commons/commons-pool2/2.8.1

if __name__ == "__main__":
    """
    Main driver program for the Spark Streaming
    """
    print("Real-Time Streaming Data Pipeline Started ...")

    # Get spark session
    env = os.environ.get('ENVIRON')
    spark = get_spark_session(env, "Real-Time Streaming Data Pipeline")
    spark.sparkContext.setLogLevel("ERROR")
    #spark.sparkContext.setLogLevel("INFO")

    # Construct a streaming DataFrame that reads from server-live-status kafka topic
    event_message_detail_df = from_kafka(KAFKA_BOOTSTRAP_SERVERS_CONS, KAFKA_TOPIC_NAME_CONS)
    print("Printing Schema of event_message_detail_df: ")
    event_message_detail_df.printSchema()

    # Tranform data using following schema
    # Schema Code Block Starts Here
    event_message_detail_schema = StructType([
      StructField("event_id", StringType()),
      StructField("event_server_status_color_name_severity_level", StringType()),
      StructField("event_datetime", StringType()),
      StructField("event_server_type", StringType()),
      StructField("event_country_code", StringType()),
      StructField("event_country_name", StringType()),
      StructField("event_city_name", StringType()),
      StructField("event_estimated_issue_resolution_time", IntegerType()),
      StructField("event_server_status_other_param_1", StringType()),
      StructField("event_server_status_other_param_2", StringType()),
      StructField("event_server_status_other_param_3", StringType()),
      StructField("event_server_status_other_param_4", StringType()),
      StructField("event_server_status_other_param_5", StringType()),
      StructField("event_server_config_other_param_1", StringType()),
      StructField("event_server_config_other_param_2", StringType()),
      StructField("event_server_config_other_param_3", StringType()),
      StructField("event_server_config_other_param_4", StringType()),
      StructField("event_server_config_other_param_5", StringType())
    ])
    # Schema Code Block Ends Here
    event_message_detail_df_transformed = preprocess(event_message_detail_df, event_message_detail_schema)

    # Write raw data into HDFS
    #hdfs_event_message_path = "hdfs:/f/{HOST_IP_ADDRESS}:9000/data/json/event_message_detail_raw_data"
    #hdfs_event_message_checkpoint_location_path = "hdfs:/f/{HOST_IP_ADDRESS}:9000/data/checkpoint/event_message_detail_raw_data"
    #hdfs_event_message_path = "hdfs://masternode:9000/data/json/event_message_detail_raw_data"
    #hdfs_event_message_checkpoint_location_path = "hdfs://masternode:9000/data/checkpoint/event_message_detail_raw_data"
    hdfs_event_message_path = "file:///C://docker_workarea//spark_job//data//json//event_message_detail_raw_data"
    hdfs_event_message_checkpoint_location_path = "file:///C://docker_workarea//spark_job//data//checkpoint//event_message_detail_raw_data"
    to_hdfs(event_message_detail_df_transformed, hdfs_event_message_path, hdfs_event_message_checkpoint_location_path)

    # Tranform raw data to add aggregate metrics
    event_message_detail_agg_df = transform(event_message_detail_df_transformed)

    # write transformed data to Postgres table
    postgresql_table_name = "event_message_detail_agg_tbl"

    #Create the Database properties
    db_properties = {}
    db_properties['user'] = postgresql_user_name
    db_properties['password'] = postgresql_password
    db_properties['driver'] = postgresql_driver
    postgresql_jdbc_url = "jdbc:postgresql://" + postgresql_host_name + ":" + str(postgresql_port_no) + "/" + postgresql_database_name
    
    # write result dataframe to PostgresSQL table
    to_dbms(event_message_detail_agg_df, postgresql_table_name, postgresql_jdbc_url, db_properties)

    # Write result dataframes into console for debugging purpose
    event_message_detail_write_stream = to_console(event_message_detail_df_transformed)
    event_message_detail_agg_write_stream = to_console(event_message_detail_agg_df)

    event_message_detail_write_stream.awaitTermination()

    print("Real-Time Streaming Data Pipeline Completed.")