from SparkWarsLib.kafka.kafka_config import parameters_to_create_kafka_consumer
from SparkWarsLib.spark.sparkwars import sparksession, write_dataset_to_bucket, read_dataset_bucket, read_jdbc, delta_output_path, merge_dataset, jediAPI

class Kafka:
    def __init__(self, kafka_server):
        self.kafka_server = kafka_server

    @classmethod
    def parameters_to_create_kafka_consumer(self, kafka_server):
        return parameters_to_create_kafka_consumer(kafka_server)
    
class SparkWars:
    def __init__(self, dataframe, environment, zone, namespace, dataset, mode, format=None, connection_name=None, query=False,  merge_keys=False, delta_retention_hours=False):
        self.dataframe = dataframe
        self.environment = environment
        self.zone = zone
        self.namespace = namespace
        self.dataset = dataset
        self.mode = mode
        self.format = format
        self.connection_name = connection_name
        self.query = query
        self.merge_keys = merge_keys
        self.delta_retention_hours = delta_retention_hours
    

    def sparksession():
        return sparksession()
    
    @classmethod
    def write(self, dataframe, environment, zone, namespace, dataset, mode, format):
        define_output_path = delta_output_path(dataset, environment, zone, namespace)
        write_dataset_to_bucket(dataframe, mode, format, define_output_path)
    

    @classmethod
    def read(self, environment, zone, namespace, dataset, format):
        define_output_path = delta_output_path(dataset, environment, zone, namespace)
        return read_dataset_bucket(format, define_output_path)
    

    @classmethod
    def read_jdbc(self, connection_name, query=False):
        string, user, password = jediAPI(connection_name)
        return read_jdbc(string, user, password, query)
    

    @classmethod
    def merge(self, dataframe, environment, zone, namespace, dataset, merge_keys, delta_retention_hours):
        define_output_path = delta_output_path(dataset, environment, zone, namespace)
        merge_dataset(dataframe, merge_keys, define_output_path, delta_retention_hours)
