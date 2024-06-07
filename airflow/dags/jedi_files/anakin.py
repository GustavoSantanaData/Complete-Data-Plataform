import argparse
from pyspark.sql.functions import max
from shared_etls.utils import (
    get_the_last_execution_checkpoint_of_the_table,
    extract_last_value_of_checkpoint,
    read_data_from_database,
    update_checkpoint_in_database
    )
from SparkWarsLib.setup import SparkWars


spark = SparkWars.sparksession()


parser = argparse.ArgumentParser(description='Execute Spark script with provided parameters.')
parser.add_argument('--namespace_name', required=True, help='Namespace name')
parser.add_argument('--dataset', required=True, help='Dataset name')
parser.add_argument('--checkpoint_column', required=True, help='Checkpoint column')
parser.add_argument('--custom_query', default='', help='Custom query')
parser.add_argument('--delta_format', required=True, help='Delta format')
parser.add_argument('--connection_name', required=True, help='Connection name')

args = parser.parse_args()

namespace_name = args.namespace_name
dataset = args.dataset
checkpoint = args.checkpoint_column
key_column_value = f"prd_raw_{namespace_name}_{dataset}"
custom_query = args.custom_query
delta_format = args.delta_format == 'True'
connection_name = args.connection_name


df_read_jdbc = read_data_from_database(connection_name)

existing_record = get_the_last_execution_checkpoint_of_the_table(df_read_jdbc, key_column_value)

last_date = extract_last_value_of_checkpoint(df_read_jdbc)

df = read_data_from_database(
    connection_name,
    custom_query,
    checkpoint,
    last_date,
    dataset,
    delta_format,
    namespace_name,
    use_query=True
    )


if checkpoint:
    last_timestamp = df.select(max(checkpoint)).collect()[0][0]


update_checkpoint_in_database(
    checkpoint,
    existing_record,
    custom_query,
    namespace_name,
    dataset,
    key_column_value,
    last_timestamp
    )


spark.stop()

