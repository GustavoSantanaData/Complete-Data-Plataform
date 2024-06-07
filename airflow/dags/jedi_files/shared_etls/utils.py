from pyspark.sql.types import *
from datetime import datetime
from decimal import Decimal
from SparkWarsLib.setup import SparkWars
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import mysql.connector
from SparkWarsLib.setup import SparkWars


def define_schema_to_kafka_data(schema_table):
    """
    Defines a schema for Kafka data based on a given table schema.

    This function creates a schema that is used to structure data received from Kafka.
    The schema includes fields for operation type, data before the operation, data after 
    the operation (based on the provided table schema), source information, and a timestamp.

    :param schema_table: (StructType) The schema of the table which will be used for the "after" field.

    :return: (StructType) A schema with the specified structure for Kafka data.
    """

    schema = StructType([
        StructField("operation", StringType(), True),
        StructField("before", StringType(), True),
        StructField("after", schema_table, True),  # Usar schema_table aqui
        StructField("source", StructType([
            StructField("db", StringType(), True),
            StructField("table", StringType(), True)
        ]), True),
        StructField("ts_ms", StringType(), True)
    ])

    return schema


def convert_schema_to_columns(data, schema):
    """
    Converts the 'after' field in the data dictionary to match the specified schema types.

    This function iterates over the fields in the 'after' section of the data dictionary 
    and converts their values to match the corresponding types defined in the provided schema.
    It handles conversion to TimestampType, DateType, and DecimalType.

    :param data: (dict) The data dictionary containing the 'after' field to be converted.
    :param schema: (StructType) The schema that defines the expected types for the 'after' field.

    :return: None. The function modifies the 'after' field of the data dictionary in place.
    """


    after_fields = schema.fields[2].dataType.fields
    for field in after_fields:
        field_name = field.name
        field_type = field.dataType
        if field_name in data['after']:
            if isinstance(field_type, TimestampType):
                data['after'][field_name] = datetime.strptime(data['after'][field_name], '%Y-%m-%dT%H:%M:%S')
            elif isinstance(field_type, DateType):
                data['after'][field_name] = datetime.strptime(data['after'][field_name], '%Y-%m-%d').date()
            elif isinstance(field_type, DecimalType):
                data['after'][field_name] = Decimal(data['after'][field_name])


def write_data_to_the_lake(delta_format, df, namespace_name, dataset):
    """
    Writes data to the data lake in either Delta format or the default format.

    This function writes a DataFrame to the data lake using the specified namespace and dataset names.
    If delta_format is True, the data is written in Delta format. Otherwise, it uses the default format.
    The function prints a message indicating the schema of the DataFrame after writing.

    :param delta_format: (bool) Flag indicating whether to write data in Delta format.
    :param df: (DataFrame) The DataFrame to be written to the data lake.
    :param namespace_name: (str) The namespace name for the data lake storage.
    :param dataset: (str) The dataset name within the namespace.

    :return: None. The function writes data to the data lake and prints a confirmation message.
    """


    if delta_format:
        SparkWars.write(
            df,
            environment="PRODUCTION",
            zone="raw",
            namespace=namespace_name,
            dataset=dataset,
            mode="append",
            format="delta"
        )
    else:
        SparkWars.write(
            df,
            environment="PRODUCTION",
            zone="raw",
            namespace=namespace_name,
            dataset=dataset,
            mode="append"
        )

    print(f"Data saved as Delta Lake in MinIO. Format: {df.schema}")


def get_the_last_execution_checkpoint_of_the_table(df_read_jdbc, key_column_value):
    """
    Retrieves the last execution checkpoint of a table based on the key column value.

    This function filters a DataFrame to find the record with the specified key column value.
    It returns the first matching record, which represents the last execution checkpoint for the table.

    :param df_read_jdbc: (DataFrame) The DataFrame read from JDBC containing the table data.
    :param key_column_value: (str or int) The value of the key column to filter the DataFrame.

    :return: (Row) The first matching record from the DataFrame, representing the last execution checkpoint.
    """


    existing_record = df_read_jdbc.filter(col("key_column") == key_column_value).first()
    
    return existing_record


def extract_last_value_of_checkpoint(df_read_jdbc):
    """
    Extracts the last value of the checkpoint from a DataFrame.

    This function selects the 'last_timestamp' column from the provided DataFrame and retrieves
    the first value, which represents the last checkpoint timestamp. It also prints this value.

    :param df_read_jdbc: (DataFrame) The DataFrame read from JDBC containing the checkpoint data.

    :return: (datetime) The last checkpoint timestamp extracted from the DataFrame.
    """


    last_date = df_read_jdbc.select(col('last_timestamp')).collect()[0][0]

    print(f"checkpoint: {last_date}")

    return last_date


def read_data_from_database(
        connection_name,
        custom_query=None,
        checkpoint=None,
        last_date=None,
        dataset=None,
        delta_format=None,
        namespace_name=None,
        use_query=None
        ):
    """
    Reads data from a database and writes it to the data lake, optionally using a custom query.

    This function reads data from a database using JDBC. If `use_query` is True, it constructs a query
    based on the provided parameters and reads the data accordingly. It then writes the data to the
    raw zone in the data lake, optionally in Delta format. If `use_query` is False, it reads the data
    directly from the database without filtering.

    :param connection_name: (str) The JDBC connection_name to find in api.
    :param custom_query: (str, optional) A custom SQL query to execute.
    :param checkpoint: (str, optional) The checkpoint column name for filtering the query.
    :param last_date: (str, optional) The last checkpoint date to filter the query.
    :param dataset: (str, optional) The name of the dataset to read from.
    :param delta_format: (bool, optional) Flag indicating whether to write data in Delta format.
    :param namespace_name: (str, optional) The namespace name for the data lake storage.
    :param use_query: (bool, optional) Flag indicating whether to use a custom query for reading data.

    :return: (DataFrame) The DataFrame containing the data read from the database.
    """


    if use_query:
        print(f' >>>> Starting in dataset= {dataset}')
        if custom_query:
            query = custom_query
        elif checkpoint:
            query = f"(select * from {dataset} WHERE {checkpoint} >= '{last_date}')"
        else:
            query = f"(select * from {dataset})"

        print(f"query: {query}")

        df_read_jdbc = SparkWars.read_jdbc(connection_name, query)

        print(">>>> writing in the raw zone")

        if delta_format:
            SparkWars.write(
                        df_read_jdbc,
                        environment="PRODUCTION",
                        zone="raw",
                        namespace=namespace_name,
                        dataset=dataset,
                        mode="append",
                        format="delta"
                    )
            print("data saved as Delta Lake in MinIO. ")
        else:
            SparkWars.write(
                        df_read_jdbc,
                        environment="PRODUCTION",
                        zone="raw",
                        namespace=namespace_name,
                        dataset=dataset,
                        mode="append"
                    )
            print("data saved as Delta Lake in MinIO. ")
                    
    else:

        print(f' >>>> Starting get checkpoint')
        df_read_jdbc = SparkWars.read_jdbc(connection_name)

    return df_read_jdbc


def update_checkpoint_in_database(
        checkpoint,
        existing_record,
        custom_query,
        namespace_name,
        dataset,
        key_column_value,
        last_timestamp
        ):
    """
    Updates or inserts checkpoint data in the database.

    This function updates an existing checkpoint record or inserts a new one in the database
    based on the provided parameters. If a custom query is specified, it skips the checkpoint update process.

    :param checkpoint: (str) The checkpoint column name.
    :param existing_record: (Row) The existing record to update, if any.
    :param custom_query: (str, optional) A custom SQL query, if specified.
    :param namespace_name: (str) The namespace name for the checkpoint data.
    :param dataset: (str) The dataset name for the checkpoint data.
    :param key_column_value: (str or int) The key column value for identifying the record.
    :param last_timestamp: (str) The last timestamp value to update or insert.

    :return: None. The function updates or inserts checkpoint data in the database.
    """


    if checkpoint and not existing_record and not custom_query:
        print("Salvando dados de checkpoint")

        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root',
            database='grecee'
        )
        cursor = conn.cursor()

        dados = [
            (namespace_name, dataset, key_column_value, checkpoint, last_timestamp)
        ]

        for checkpoint_data in dados:
            cursor.execute("INSERT INTO checkpoints (namespace, table_name, key_column, "
                           "checkpoint_column, last_timestamp) VALUES (%s, %s, %s, %s, %s)",
                           checkpoint_data)
            
            print("Novo registro inserido.")

        conn.commit()
        conn.close()

    elif checkpoint and existing_record and not custom_query:
        print("Salvando dados de checkpoint")

        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root',
            database='grecee'
        )
        cursor = conn.cursor()

        dados = [
            (checkpoint, last_timestamp)
        ]

        for checkpoint_data in dados:
            cursor.execute('UPDATE checkpoints set checkpoint_column  = (%s), last_timestamp = (%s)',
                           checkpoint_data)
            
            print(f"Registro atualizado.{last_timestamp}")

        conn.commit()
        conn.close()

    else:
        print("No checkpoint save process")


def identifies_type_of_process_for_reading_the_bucket(topic, namespace, tabela):
    """
    Identifies the type of process for reading data from the bucket based on the provided parameters.

    This function determines whether to perform a CDC (Change Data Capture) process or a JDBC (Java Database Connectivity) process
    based on the presence of a topic name. If a topic name is provided, it indicates a CDC process; otherwise, it assumes a JDBC process.
    The function then reads data from the specified dataset in the raw zone of the data lake and selects relevant columns based on the
    type of process.

    :param topic: (str or None) The name of the Kafka topic for CDC process, if available.
    :param namespace: (str) The namespace name for the dataset in the data lake.
    :param tabela: (str) The name of the dataset (table) to read from.

    :return: (DataFrame) The DataFrame containing the data read from the specified dataset, with relevant columns selected
                         based on the type of process.
    """

    
    if topic:
        type_of_process = 'cdc'
    else:
        type_of_process = 'jdbc'

    type_of_read = type_of_process 


    df = SparkWars.read(
                    environment="PRODUCTION",
                    zone="raw",
                    namespace=namespace,
                    dataset=tabela,
                    format="parquet"
                )
    

    if type_of_read == 'cdc':
        df = df.select("after.*")
    else:
        df = df
    
    return df


def adjustment_for_safe_zone(df, ignore_columns):
    """
    Adjusts the DataFrame for the safe zone by dropping specified columns.

    This function creates a new DataFrame by dropping the columns specified in the `ignore_columns` list.
    It is used to prepare the DataFrame for writing to the safe zone, where sensitive or unnecessary columns
    are removed to ensure data security or compliance.

    :param df: (DataFrame) The DataFrame to adjust for the safe zone.
    :param ignore_columns: (list) A list of column names to be ignored (dropped) from the DataFrame.

    :return: (DataFrame) The adjusted DataFrame with specified columns dropped.
    """


    df_parquet_final = df.drop(*ignore_columns)
    return df_parquet_final


def written_in_bucket_safe(ignore_columns, delta_format, namespace, tabela, df, df_parquet_final):
    """
    Writes data to the safe zone in the data lake with optional adjustments.

    This function writes data to the safe zone in the data lake, optionally handling Delta format and column adjustments.
    If `ignore_columns` is provided, it drops the specified columns from the DataFrame before writing.
    If `delta_format` is True, it writes data in Delta format and performs a merge operation if the dataset already exists.
    If `delta_format` is False, it writes data in the default format and overwrites the existing dataset if it exists.

    :param ignore_columns: (list) A list of column names to be ignored (dropped) from the DataFrame before writing.
    :param delta_format: (bool) Flag indicating whether to write data in Delta format.
    :param namespace: (str) The namespace name for the data lake storage.
    :param tabela: (str) The name of the dataset (table) in the safe zone.
    :param df: (DataFrame) The original DataFrame containing the data to be written.
    :param df_parquet_final: (DataFrame) The DataFrame adjusted for safe zone writing, if applicable.

    :return: None. The function writes data to the safe zone and adjusts the DataFrame if necessary.
    """


    if ignore_columns:

        if not delta_format:

            SparkWars.write(
                    df,
                    environment="PRODUCTION",
                    zone="safe",
                    namespace=namespace,
                    dataset=tabela,
                    mode="overwrite"
                )

            
            df_parquet_final = adjustment_for_safe_zone(df_parquet_final, ignore_columns)

        else:

            try:
                # df_safe = spark.read.format("delta").load(outputPath)
                df_safe = SparkWars.read(
                    environment="PRODUCTION",
                    zone="safe",
                    namespace=namespace,
                    dataset=tabela,
                    format="delta"
                )
            except:
                df_safe = None

            

            if df_safe:
                print("starting merge process safe")

                SparkWars.merge(
                    df_parquet_final,
                    environment="PRODUCTION",
                    zone="safe",
                    namespace=namespace,
                    dataset=tabela,
                    merge_keys="df_lake.key = df_parquet.key",
                    delta_retention_hours=72
                )

                
            else:
                print("starting dataset safe")

                SparkWars.write(
                    df_parquet_final,
                    environment="PRODUCTION",
                    zone="safe",
                    namespace=namespace,
                    dataset=tabela,
                    mode="overwrite",
                    format="delta"
                )

            

            df_parquet_final = adjustment_for_safe_zone(df_parquet_final, ignore_columns)


def written_in_bucket_trusted(delta_format, df_parquet_final, namespace, tabela):
    """
    Writes data to the trusted zone in the data lake with optional Delta format and merge operation.

    This function writes data to the trusted zone in the data lake, supporting both Delta and non-Delta formats.
    If `delta_format` is False, it writes data in the default format and overwrites the existing dataset if it exists.
    If `delta_format` is True, it reads the existing dataset in Delta format and performs a merge operation with the
    new data before writing.

    :param delta_format: (bool) Flag indicating whether to write data in Delta format.
    :param df_parquet_final: (DataFrame) The DataFrame to be written to the trusted zone.
    :param namespace: (str) The namespace name for the data lake storage.
    :param tabela: (str) The name of the dataset (table) in the trusted zone.

    :return: None. The function writes data to the trusted zone and performs merge operations if necessary.
    """
    

    if not delta_format:

        print(" starting write dataset trusted")

        SparkWars.write(
                    df_parquet_final,
                    environment="PRODUCTION",
                    zone="trusted",
                    namespace=namespace,
                    dataset=tabela,
                    mode="overwrite"
                )

    else:

        try:

            df_lake = SparkWars.read(
                    environment="PRODUCTION",
                    zone="trusted",
                    namespace=namespace,
                    dataset=tabela,
                    format="delta"
                )
            
        except:

            df_lake = None

        if df_lake:
            print("starting merge process trusted")

            SparkWars.merge(
                    df_parquet_final,
                    environment="PRODUCTION",
                    zone="trusted",
                    namespace=namespace,
                    dataset=tabela,
                    merge_keys="df_lake.key = df_parquet.key",
                    delta_retention_hours=72
                )

            print("Processo finalizado com sucesso")

        else:

            print("starting write dataset trusted")

            SparkWars.write(
                    df_parquet_final,
                    environment="PRODUCTION",
                    zone="trusted",
                    namespace=namespace,
                    dataset=tabela,
                    mode="overwrite",
                    format="delta"
                )
