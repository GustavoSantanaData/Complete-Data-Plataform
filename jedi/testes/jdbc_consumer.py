# Imports
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, max, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import mysql.connector


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



spark = SparkSession.builder \
           .appName('SparkByExamples.com') \
           .master("local[*]") \
           .config(conf=conf) \
           .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-sharing-spark_2.12:3.1.0,mysql:mysql-connector-java:8.0.27") \
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
           .getOrCreate()



namespace_name = "aphrodite" #nome do namespace
dataset = 'aphrodite'

connection_string = "jdbc:mysql://localhost:3306/grecee"
connection_user = "root"
connection_pass = "root"
checkpoint = 'updated_at'
key_column_value = "prd_raw_aphrodite_aphrodite"
custom_query = ''

url = connection_string
user = connection_user
password = connection_pass


print(f' >>>> Starting get checkpoint')
df_checkpoint = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "checkpoints") \
    .option("user", user) \
    .option("password", password) \
    .load()

existing_record = df_checkpoint.filter(col("key_column") == key_column_value).first()
last_date = df_checkpoint.select(col('last_timestamp')).collect()[0][0]



print(last_date)


print(f' >>>> Starting in dataset= {dataset}')
if custom_query:
    query = custom_query
elif checkpoint:
    query = f"(select * from {dataset} WHERE {checkpoint} >= '{last_date}')"
else:
    query = f"(select * from {dataset})"

print(f"query: {query}")

df = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("query", query) \
    .option("user", user) \
    .option("password", password) \
    .load()



delta_output_path = "s3a://prd-lake-raw-aphrodite/aphrodite"
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("append", "true") \
    .save(delta_output_path)

print(f"Dados salvos como Delta Lake no MinIO. Formato: {df.schema}")








if checkpoint:
    last_timestamp = df.select(max(checkpoint)).collect()[0][0]






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
        cursor.execute('INSERT INTO checkpoints (namespace, table_name, key_column, checkpoint_column, last_timestamp) VALUES (%s, %s, %s, %s, %s)', checkpoint_data)
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
        cursor.execute('UPDATE checkpoints set checkpoint_column  = (%s), last_timestamp = (%s)', checkpoint_data)
        print(f"Registro atualizado.{last_timestamp}")

    conn.commit()
    conn.close()

else:
    print("No checkpoint save process")


spark.stop()


