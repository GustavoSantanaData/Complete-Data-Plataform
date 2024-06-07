from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType, DecimalType
from minio import Minio
import json
import os
import time
from pyspark import SparkConf
from datetime import datetime
from decimal import Decimal

# Configurações do Kafka
conf_kafka = {
    'bootstrap.servers': 'pkc-4r087.us-west2.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'W2KRRRJ4IYCCJKFC',
    'sasl.password': '2iBBe2PmCFcbFvZDVh8Bmr16p8hDFaggqrYg0D1p9hVuiH27SAVnbpqhlZ0Uy6Xb',
    'group.id': 'my_consumer_groupgusta117',
    'auto.offset.reset': 'earliest'
}

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

# Criação da sessão Spark
spark = SparkSession.builder \
    .appName("Kafka Spark Streaming") \
    .config(conf=conf) \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-sharing-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Cria um consumidor Kafka
consumer = Consumer(conf_kafka)

# Inscreva-se no tópico
topic = "hades"
consumer.subscribe([topic])

# Definir o schema para os dados do Kafka
schema = StructType([
    StructField("operation", StringType(), True),
    StructField("before", StringType(), True),
    StructField("after", StructType([
        StructField("id", IntegerType(), True),
        StructField("phone_number", StringType(), True),
        StructField("rating", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("protocol", StringType(), True),
        StructField("status", StringType(), True),
        StructField("updated_at", TimestampType(), True)
    ]), True),
    StructField("source", StructType([
        StructField("db", StringType(), True),
        StructField("table", StringType(), True)
    ]), True),
    StructField("ts_ms", StringType(), True)
])

# Função para converter colunas conforme o tipo especificado no schema
def convert_columns(data, schema):
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
            # Adicione outros tipos conforme necessário

try:
    last_message_time = time.time()
    while True:
        # Leia mensagens do Kafka
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            current_time = time.time()
            if current_time - last_message_time > 10:
                print("Nenhuma mensagem recebida por 10 segundos. Encerrando a aplicação.")
                break
            continue
        else:
            last_message_time = time.time()
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Final da partição
                continue
            else:
                # Erro irreversível
                print(msg.error())
                break

        # Converter a mensagem JSON em um DataFrame Spark
        message = msg.value().decode('utf-8')
        data = json.loads(message)
        
        # Converter colunas conforme o tipo especificado no schema
        convert_columns(data, schema)

        # Criar uma linha com os dados extraídos
        row_dict = {}
        # Iterar sobre as chaves do dicionário de dados
        for key, value in data.items():
            # Adicionar os valores ao dicionário
            row_dict[key] = value
        
        # Criar uma linha com os dados extraídos
        row = tuple(row_dict.get(col.name, None) for col in schema)
        
        # Criar um DataFrame Spark a partir da linha e do schema
        df = spark.createDataFrame([row], schema=schema)

        # Salvar o DataFrame como arquivo Delta Lake
        delta_output_path = "s3a://prd-lake-raw-hades/hades"
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("append", "true") \
            .save(delta_output_path)

        # Exibir o formato do arquivo antes de ser salvo no MinIO
        print(f"Dados salvos como Delta Lake no MinIO. Formato: {df.schema}")

except KeyboardInterrupt:
    pass

finally:
    # Feche o consumidor Kafka
    consumer.close()
