import argparse
from pyspark.sql.types import *
from delta import *
import datetime as dtm
from datetime import timedelta as tmd
import pyspark.sql.functions as sf
import pyspark.sql.types as st
from pyspark.sql import Window
from SparkWarsLib.setup import SparkWars
from shared_etls.utils import (
    identifies_type_of_process_for_reading_the_bucket,
    written_in_bucket_safe,
    written_in_bucket_trusted
    )


parser = argparse.ArgumentParser(description="Process data. Read raw and write trusted")
parser.add_argument('--namespace', type=str, required=True, help='Namespace of the data')
parser.add_argument('--tabela', type=str, required=True, help='Table name of the data')
parser.add_argument('--schema', type=str, required=True, help='Schema definition as a JSON string')
parser.add_argument('--delta_format', type=bool, required=True, help='Whether to use Delta format')
parser.add_argument('--checkpoint', type=str, required=True, help='Checkpoint column for processing')
parser.add_argument('--table_key', type=str, nargs='+', required=True, help='Key columns for the table')
parser.add_argument('--enable_hyphen_separator', type=bool, required=True, help='Enable hyphen separator in keys')
parser.add_argument('--ignore_columns', type=str, nargs='+', required=True, help='Columns to ignore')
parser.add_argument('--topic', type=str, required=False, help='Kafka topic to process')

args = parser.parse_args()


namespace = args.namespace
tabela = args.tabela
schema_table = eval(args.schema)
delta_format = args.delta_format
checkpoint = args.checkpoint
table_key = args.table_key
ignore_columns = args.ignore_columns
topic = args.topic
enable_hyphen_separator = args.enable_hyphen_separator

spark = SparkWars.sparksession()
schema = StructType(schema_table)


df = identifies_type_of_process_for_reading_the_bucket(topic, namespace, tabela)


def create_artificial_key(ids, df, enableHyphenSeparator=False):
    """
    Creates an artificial key in a DataFrame using the specified columns.

    :param ids: List[str]
        List of column names to be concatenated to form the key.
    :param df: pyspark.sql.dataframe.DataFrame
        DataFrame in which the artificial key will be created.
    :param enableHyphenSeparator: bool, optional
        Boolean indicating whether to concatenate the columns with a hyphen separator.
        Default is False.

    :return: pyspark.sql.dataframe.DataFrame
        DataFrame with the artificial key added.
    """


    if enableHyphenSeparator:
        df = df.withColumn("concatenatedString", sf.concat_ws("-", *ids)) \
            .withColumn("key", sf.md5(sf.col("concatenatedString")
                        .cast(st.StringType()))) \
            .drop("concatenatedString")
        
    else:

        df = df.withColumn(
            "key", sf.concat(*ids)
        ).withColumn(
            "key", sf.md5(sf.col("key").cast(st.StringType()))
        )
    return df


def add_timestamp(df):
    """
    Adds a 'timestamp_kafka' column to a DataFrame with the current timestamp.

    :param df: pyspark.sql.dataframe.DataFrame
        DataFrame to which the 'timestamp_kafka' column will be added.

    :return: pyspark.sql.dataframe.DataFrame
        DataFrame with the 'timestamp_kafka' column added.
    """

    
    print("Adding column 'timestamp_kafka' ")
    df = df.withColumn("timestamp_kafka",
                       sf.lit(dtm.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                       .cast(st.TimestampType()))

    return df


def quality_check_dataformat(df):
    """
    Performs a quality check on the data format in a DataFrame, ensuring 
    that date and timestamp columns have valid values.

    :param df: pyspark.sql.dataframe.DataFrame
        DataFrame to be checked and corrected for data format quality.

    :return: pyspark.sql.dataframe.DataFrame
        DataFrame with invalid dates handled.
    """
    

    try:
        coldates = [cols for cols in df.columns
                    if df.select(cols).dtypes[0][1] in ['timestamp', 'date']]
        for colname in coldates:
            try:
                df = \
                    df.withColumn(colname,
                                  sf.when(sf.year(colname) >= 10,
                                          sf.col(colname)).otherwise(None))
            except Exception as ms:
                print(
                    f'There was a problem while handling invalid '
                    f'date for {colname}: {ms}')
    except Exception as msg:
        print(f'There was a problem while handling invalid dates: {msg}')
    return df


def window_spark(df, table_key, checkpoint):
    """
    Applies windowing and deduplication to a DataFrame based on a table key and checkpoint column.

    :param df: pyspark.sql.dataframe.DataFrame
        DataFrame to be processed.
    :param table_key: List[str]
        List of columns to partition the window function by.
    :param checkpoint: str or None
        Column name used to order the window function for deduplication. If None, deduplication is done by 'key' column.

    :return: pyspark.sql.dataframe.DataFrame
        DataFrame with duplicates removed based on the windowing logic.
    """
    
    
    if checkpoint:
        window_function = Window.partitionBy(*table_key).orderBy(sf.col(checkpoint).desc())
        df_final = df.dropDuplicates().withColumn("distinct", sf.row_number().over(window_function)).filter("distinct = 1")
        df_final = df_final.drop('distinct')
        return df_final
    else:
        df_final = df.dropDuplicates(["key"])
        return df_final
    

table = df


df_parquet_key = create_artificial_key(table_key, table, enable_hyphen_separator)
df_parquet_timestamp = add_timestamp(df_parquet_key)
df_parquet_qs = quality_check_dataformat(df_parquet_timestamp)
df_parquet_final = window_spark(df_parquet_qs, table_key, checkpoint)


written_in_bucket_safe(ignore_columns, delta_format, namespace, tabela, df, df_parquet_final)


written_in_bucket_trusted(delta_format, df_parquet_final, namespace, tabela)


spark.stop()