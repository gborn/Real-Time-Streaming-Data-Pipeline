
def from_kafka(spark, bootstrap_servers_cons, topic_name_cons, offset='latest'):
    """
    Reads files in given directory and returns spark dataframe
    @spark spark session object
    @data_dir directory to read files from
    @file_pattern prefix for files
    @file_format one of csv, json, or parquet
    @returns spark dataframe
    """
    df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers_cons) \
            .option("subscribe", topic_name_cons) \
            .option("startingOffsets", offset) \
            .load()

    return df