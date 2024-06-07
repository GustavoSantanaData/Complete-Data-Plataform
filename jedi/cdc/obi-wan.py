import argparse
from confluent_kafka import Consumer, KafkaError
from pyspark.sql.types import *
import json
import time
from SparkWarsLib.setup import Kafka
from SparkWarsLib.setup import SparkWars
from shared_etls.utils import (
    define_schema_to_kafka_data,
    convert_schema_to_columns,
    write_data_to_the_lake
    )


parser = argparse.ArgumentParser(description='Execute Spark script with provided parameters.')
parser.add_argument('--namespace_name', required=True, help='Namespace name')
parser.add_argument('--dataset', required=True, help='Dataset name')
parser.add_argument('--topic', required=True, help='Kafka topic')
parser.add_argument('--kafka_server', required=True, help='Kafka server')
parser.add_argument('--delta_format', required=True, help='Delta format')
parser.add_argument('--schema', required=True, help='Schema')

args = parser.parse_args()


namespace_name = args.namespace_name
dataset = args.dataset
kafka_topic = args.topic
kafka_server = args.kafka_server
delta_format = args.delta_format == 'True'
schema_table = eval(args.schema)


spark = SparkWars.sparksession()


consumer = Consumer(Kafka.parameters_to_create_kafka_consumer(kafka_server))
consumer.subscribe([kafka_topic])


schema = define_schema_to_kafka_data(schema_table)


try:
    last_message_time = time.time()
    while True:
       
        topic_kafka_message = consumer.poll(timeout=1.0)
        if topic_kafka_message is None:
            current_time = time.time()
            if current_time - last_message_time > 10:
                print("No messages received for 10 seconds. Closing the application.")
                break
            continue
        else:
            last_message_time = time.time()
        
        if topic_kafka_message.error():
            if topic_kafka_message.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(topic_kafka_message.error())
                break

        message = topic_kafka_message.value().decode('utf-8')

        data = json.loads(message)
        
        convert_schema_to_columns(data, schema)

        line_with_the_extracted_data = {}

        for key, value in data.items():

            line_with_the_extracted_data[key] = value
        
        row = tuple(line_with_the_extracted_data.get(col.name, None) for col in schema)
        
        df = spark.createDataFrame([row], schema=schema)

        write_data_to_the_lake(delta_format, df, namespace_name, dataset)

except KeyboardInterrupt:
    pass

finally:
    consumer.close()

