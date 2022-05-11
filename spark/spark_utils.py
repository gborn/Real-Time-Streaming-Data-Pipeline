
from pyspark.sql import SparkSession

def get_spark_session(env, app_name):
    """
    returns spark session
    """
    # local cluster
    if env == 'DEV':
        spark = SparkSession \
            .builder \
            .appName(app_name) \
            .master("local[*]") \
            .getOrCreate()

    # production cluster with yarn as resource manager
    elif env == 'PROD':
        spark = SparkSession \
            .builder \
            .appName(app_name) \
            .master("yarn") \
            .config("spark.jars", "file:///D://spark_dependency_jars//commons-pool2-2.8.1.jar,file:///D://spark_dependency_jars//postgresql-42.2.16.jar,file:///D://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar,file:///D://spark_dependency_jars//kafka-clients-2.6.0.jar,file:///D://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
            .config("spark.executor.extraClassPath", "file:///D://spark_dependency_jars//commons-pool2-2.8.1.jar:file:///D://spark_dependency_jars//postgresql-42.2.16.jar:file:///D://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar:file:///D://spark_dependency_jars//kafka-clients-2.6.0.jar:file:///D://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
            .config("spark.executor.extraLibrary", "file:///D://spark_dependency_jars//commons-pool2-2.8.1.jar:file:///D://spark_dependency_jars//postgresql-42.2.16.jar:file:///D://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar:file:///D://spark_dependency_jars//kafka-clients-2.6.0.jar:file:///D://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
            .config("spark.driver.extraClassPath", "file:///D://spark_dependency_jars//commons-pool2-2.8.1.jar:file:///D://spark_dependency_jars//postgresql-42.2.16.jar:file:///D://spark_dependency_jars//spark-sql-kafka-0-10_2.12-3.0.1.jar:file:///D://spark_dependency_jars//kafka-clients-2.6.0.jar:file:///D://spark_dependency_jars//spark-streaming-kafka-0-10-assembly_2.12-3.0.1.jar") \
            .getOrCreate()

    return spark