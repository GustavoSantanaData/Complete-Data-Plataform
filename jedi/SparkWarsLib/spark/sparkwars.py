
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkConf
from delta import *
from delta.tables import DeltaTable

def sparksession():
    """
    Creates a SparkSession with customized configuration.

    This function creates a SparkSession with customized configuration parameters to enable various features
    and connect to specific data sources. It sets configurations related to accessing S3, MinIO, debugging options,
    and package dependencies. Additionally, it configures Spark to use Delta Lake for managing table lifecycle.

    :return: (SparkSession) A SparkSession object with the specified configurations.
    """


    conf = SparkConf()
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.connection.timeout", "600000")
    conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
    conf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    conf.set("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
    conf.set("spark.sql.debug.maxToStringFields", "100")
    conf.set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.sdk.disableCertChecking=true")
    conf.set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.sdk.disableCertChecking=true")
    conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-sharing-spark_2.12:3.1.0,mysql:mysql-connector-java:8.0.27")
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    # Criação da sessão Spark
    spark = SparkSession.builder \
        .appName("Kafka Spark Streaming") \
        .master("local[*]") \
        .config(conf=conf) \
        .getOrCreate()
    
    return spark


def delta_output_path(dataset_name, environment, zone, namespace):
    """
    Generates the output path for Delta Lake tables based on environment, zone, and namespace.

    This function constructs the output path for Lake tables depending on the environment (PRODUCTION or STAGING),
    zone, and namespace. It formats the path according to the specified environment, zone, and namespace, and appends
    the dataset name to the path.

    :param dataset_name: (str) The name of the dataset (table) for which the output path is generated.
    :param environment: (str) The environment (PRODUCTION or STAGING) where the Delta Lake table will be located.
    :param zone: (str) The zone (e.g., raw, trusted) where the Delta Lake table will be located.
    :param namespace: (str) The namespace name for the data lake storage.

    :return: (str) The output path for the Delta Lake table.
    """


    if environment == 'PRODUCTION':
        delta_output_path = f"s3a://prd-lake-{zone}-{namespace}/{dataset_name}"
    elif environment == 'STAGING':
        delta_output_path = f"s3a://stg-lake-{zone}-{namespace}/{dataset_name}"

    return delta_output_path


def write_dataset_to_bucket(dataframe, mode, format, delta_output_path):
    """
    Writes a DataFrame to a specified output path in the specified format.

    This function writes the given DataFrame to the specified output path in the specified format.
    If the format is "delta", it writes the DataFrame in Delta format with the specified retention duration.
    If the format is not "delta", it writes the DataFrame in Parquet format.

    :param dataframe: (DataFrame) The DataFrame to be written.
    :param mode: (str) The mode for saving the DataFrame (e.g., "overwrite", "append").
    :param format: (str) The format in which to save the DataFrame ("delta" or other).
    :param delta_output_path: (str) The output path where the DataFrame will be saved.

    :return: None. The function writes the DataFrame to the specified output path.
    """


    if format == "delta": 
        dataframe.write \
        .format("delta") \
        .mode(mode) \
        .option("deltaRetentionDuration", "3 days") \
        .save(delta_output_path)
    else:
        dataframe.write \
        .mode(mode) \
        .parquet(delta_output_path)


def read_dataset_bucket(format, outputPath):
    """
    Reads a dataset from the specified output path in the specified format.

    This function reads a dataset from the specified output path in the specified format.
    If the format is "delta", it reads the dataset in Delta format.
    If the format is not "delta", it reads the dataset in Parquet format.

    :param format: (str) The format in which the dataset is stored ("delta" or other).
    :param outputPath: (str) The output path from which to read the dataset.

    :return: (DataFrame) The DataFrame containing the read dataset.
    """


    spark = sparksession()


    if format == "delta": 
        read_return = spark.read.format("delta").load(outputPath)
    else:
        read_return = spark.read.parquet(outputPath)


    return read_return


def jediAPI(connection_name):
    """
    Retrieves connection details from an API for a specific connection.

    This function makes a GET request to the API at "http://localhost:8000/connections/"
    and searches for the details of a specific connection identified by the given name.

    Args:
        :pram connection_name (str): The name of the desired connection.

    Returns:
        tuple: A tuple containing:
            - string (str or None): The connection string corresponding to the given name, or None if not found.
            - user (str or None): The username corresponding to the given name, or None if not found.
            - password (str or None): The password corresponding to the given name, or None if not found.
    
    """

    import requests

    api_url = "http://localhost:8000"

    response = requests.get(f"{api_url}/connections/")

    if response.status_code == 200:
        connections = response.json()
        string = None
        user = None
        password = None
        for connection in connections:
            if connection.get('nome') == connection_name:
                string = connection.get('string')
                user = connection.get('usuario')
                password = connection.get('senha')
    else:
        print("Error getting connection:", response.text)

    return string, user, password


def read_jdbc(string, user, password, query):
    """
    Reads data from a JDBC data source.

    This function reads data from a JDBC data source, such as a MySQL database, using the specified URL and optional query.
    If a query is provided, it retrieves data based on the query; otherwise, it reads the entire table specified by the URL.
    The function utilizes SparkSession to create a DataFrame from the JDBC data source.

    :param url: (str) The JDBC URL for connecting to the database.
    :param query: (str, optional) The SQL query to execute for retrieving data. If not provided, reads the entire table.

    :return: (DataFrame) The DataFrame containing the data read from the JDBC data source.
    """


    #tudo aqui devera chamar a jediAPI com a url para preencher os parametros de conexao. depois preciso mudar a docstring

    spark = sparksession()


    if query:
        df_read_jdbc = spark.read \
            .format("jdbc") \
            .option("url", string) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("query", query) \
            .option("user", user) \
            .option("password", password) \
            .load()
    else:
         df_read_jdbc = spark.read \
        .format("jdbc") \
        .option("url", string) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "checkpoints") \
        .option("user", user) \
        .option("password", password) \
        .load()

    
    return df_read_jdbc


def merge_dataset(dataframe, merge_keys, define_output_path, delta_retention_hours):
    """
    Merges a DataFrame with an existing Delta table and performs vacuuming.

    This function merges the provided DataFrame with an existing Delta table located at the specified output path.
    It utilizes the merge keys to determine how to match records between the DataFrame and the Delta table.
    After the merge operation, it performs vacuuming on the Delta table to optimize its size and improve performance.

    :param dataframe: (DataFrame) The DataFrame to be merged with the Delta table.
    :param merge_keys: (str) The merge keys used to match records between the DataFrame and the Delta table.
    :param define_output_path: (str) The output path where the Delta table is located.
    :param delta_retention_hours: (int) The number of hours for retention during vacuuming.

    :return: None. The function merges the DataFrame with the Delta table and performs vacuuming.
    """
    

    spark = sparksession()

    delta_table = DeltaTable.forPath(spark, define_output_path)

    delta_table \
        .alias("df_lake") \
        .merge(
            dataframe.alias("df_parquet"),
            merge_keys
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    retention_hours = delta_retention_hours
    delta_table.vacuum(retention_hours)


