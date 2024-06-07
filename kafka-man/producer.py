import json 
import datetime 
import time
from confluent_kafka import Producer
import mysql.connector
from mysql.connector import Error


def get_kafka_config():
    """
    Load Kafka configuration from a JSON file.

    Returns:
        dict: A dictionary containing Kafka configurations.
    """

    with open('cluster/kafka_config.json', 'r') as file:
        kafka_config = json.load(file)
    return kafka_config


def get_mysql_properties():
    """
    Load MySQL connection properties from a JSON file.

    Returns:
        dict: A dictionary containing MySQL connection properties.
    """

    with open('mysql/properties.json', 'r') as file:
        mysql_properties = json.load(file)
    return mysql_properties


def serialize_datetime(obj): 
    """
    Serialize datetime objects to ISO 8601 format.

    Args:
        obj: The object to be serialized.

    Returns:
        str: The ISO 8601 string representation of the datetime object.

    Raises:
        TypeError: If the object is not a datetime instance.
    """

    if isinstance(obj, datetime.datetime): 
        return obj.isoformat() 
    raise TypeError("Type not serializable") 


def produce_debezium_to_kafka(bootstrap_servers, topic, message):
    """
    Produce a message to a Kafka topic using Kafka configuration.

    Args:
        bootstrap_servers (str): Kafka bootstrap servers.
        topic (str): The topic to which the message will be sent.
        message (str): The message to be sent in JSON format.
    """

    kafka_config = get_kafka_config()
    conf = {
        'bootstrap.servers': kafka_config['bootstrap.servers'],
        'security.protocol': kafka_config['security.protocol'],
        'sasl.mechanisms': kafka_config['sasl.mechanisms'],
        'sasl.username': kafka_config['sasl.username'],
        'sasl.password': kafka_config['sasl.password']
    }
    producer = Producer(conf)
    

    timestamp_ms = int(time.time() * 1000)
    message_dict = json.loads(message)
    message_dict['ts_ms'] = timestamp_ms
    

    message_with_timestamp = json.dumps(message_dict, default=serialize_datetime)
    

    producer.produce(topic=topic, value=message_with_timestamp)
    producer.flush()


def main():
    """
    Main function that manages the connection to MySQL and the production of messages to Kafka.
    Continuously queries a MySQL database and sends the changes to a Kafka topic.
    """

    mysql_properties = get_mysql_properties()
    kafka_config = get_kafka_config()


    while True:
        try:
            for db_config in mysql_properties:

                host = db_config['host']
                port = db_config['port']
                user = db_config['user']
                password = db_config['password']
                database = db_config['database']
                topic = db_config['topic']
                columns = db_config['columns']
                table_name = db_config['name']
                last_processed_id = db_config.get('last_processed_id', 0)

                connection = mysql.connector.connect(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    database=database
                )

                if connection.is_connected():
                    cursor = connection.cursor()

                    query = f"SELECT * FROM {table_name} WHERE id > {last_processed_id};"
                    cursor.execute(query)
                    records = cursor.fetchall()


                    for row in records:
                        debezium_message = {
                            "operation": "insert",
                            "before": {},
                            "after": dict(zip(cursor.column_names, row)),
                            "source": {
                                "db": database,
                                "table": table_name
                            }
                        }

                        produce_debezium_to_kafka(
                            kafka_config['bootstrap.servers'],
                            topic,
                            json.dumps(debezium_message, default=serialize_datetime)
                        )

                    if records:
                        last_id = max(row[0] for row in records)
                        update_last_processed_id(mysql_properties, table_name, last_id)

                    cursor.close()
                    connection.close()

        except Error as e:
            print("Error to connect at MySQL:", e)


def update_last_processed_id(properties, table_name, last_processed_id):
    """
    Update the last processed ID for a specific table in the MySQL properties file.

    Args:
        properties (list): List of dictionaries containing MySQL properties.
        table_name (str): The name of the table whose last processed ID will be updated.
        last_processed_id (int): The ID of the last processed record.
    """

    for prop in properties:
        if prop['name'] == table_name:
            prop['last_processed_id'] = last_processed_id

            with open('mysql/properties.json', 'w') as file:
                json.dump(properties, file, indent=4)
            break

if __name__ == "__main__":
    main()
