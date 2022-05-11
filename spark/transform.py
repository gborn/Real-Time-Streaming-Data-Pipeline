
def preprocess(df, df_schema):
    """
    Enforce Schema, Preprocess data, and 
    add status_color_name, status_severity_level columns
    """
    df_1 = df.selectExpr("CAST(value AS STRING)")
    df_2 = df_1.select(from_json(col("value"), df_schema).alias("event_message_detail"))
    df_3 = df_2.select("event_message_detail.*")
    print("Printing Schema of df_3: ")
    df_3.printSchema()

    # Data Transformation
    # 1. Split the column event_server_status_color_name_severity_level to
    # event_server_status_color_name, event_server_status_severity_level
    # 2. Add new column event_message_count with counter 1
    # event_server_status_color_name_severity_level --> Green|Severity 3
    df_4 = df_3\
        .withColumn("event_server_status_color_name", split(df_3["event_server_status_color_name_severity_level"], '\|')[0]) \
        .withColumn("event_server_status_severity_level", split(df_3["event_server_status_color_name_severity_level"], '\|')[1]) \
        .withColumn("event_message_count", lit(1))

    return df_4


def transform(df):
    """
    Add new metrics,  total resolution time, and total message count for each country
    @df dataframe to transform
    """
    '''df_transformed = df.select(
      col("event_country_code"),
      col("event_country_name"),
      col("event_city_name"),
      col("event_server_status_color_name"),
      col("event_server_status_severity_level"),
      col("event_estimated_issue_resolution_time"),
      col("event_message_count"))'''

    df_transformed = df.select(["event_country_code", \
      "event_country_name", \
      "event_city_name", \
      "event_server_status_color_name", \
      "event_server_status_severity_level", \
      "event_estimated_issue_resolution_time", \
      "event_message_count"])

    # Data Processing/Data Transformation
    aggregated_events_by_country_df = df_transformed\
        .groupby("event_country_code",
                 "event_country_name",
                 "event_city_name",
                 "event_server_status_color_name",
                 "event_server_status_severity_level")\
        .agg(fn.sum('event_estimated_issue_resolution_time').alias('total_estimated_resolution_time'),
             fn.count('event_message_count').alias('total_message_count'))
        
    return aggregated_events_by_country_df