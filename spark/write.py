
def to_hdfs(df, hdfs_path,  hdfs_checkpoint_location_path, file_format='json', processing_time='20 seconds'):
    """
    writes streaming dataframe to hdfs_path using given file_format
    @df dataframe to write
    @file_format target file format
    @processing_time time to trigger 
    @hdfs_path target hdfs path  where event messages will be written
    @hdfs_checkpoint_location_path hdfs checkpoint location path for event messages
    """
    df.writeStream \
        .trigger(processingTime=processing_time) \
        .format(file_format) \
        .option("path", hdfs_path) \
        .option("checkpointLocation", hdfs_checkpoint_location_path) \
        .start()


def to_console(df):
    """
    writes df to console
    """
    write_stream = df.writeStream \
        .trigger(processingTime='20 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    return write_stream
    

def save_to_postgresql_table(current_df, epoc_id, postgresql_table_name, postgresql_jdbc_url, db_properties):
    """
    Connects to PostgresSQL table and saves current_df data
    """
    print("Inside save_to_postgresql_table function")
    print("Printing epoc_id: ")
    print(epoc_id)
    print("Printing postgresql_table_name: " + postgresql_table_name)

    #Save the dataframe to the table.
    current_df.write.jdbc(url = postgresql_jdbc_url,
                  table = postgresql_table_name,
                  mode = 'append',
                  properties = db_properties)
    print("Exit out of save_to_postgresql_table function")

def to_dbms(df, postgresql_table_name, postgresql_jdbc_url, ):
    """
    writes dataframe to a postgres table
    """
    df.writeStream \
        .trigger(processingTime='20 seconds') \
        .outputMode("update") \
        .foreachBatch(lambda current_df, epoc_id: save_to_postgresql_table(current_df, epoc_id, postgresql_table_name, postgresql_jdbc_url, db_properties)) \
        .start()