def parameters_to_create_kafka_consumer(kafka_server):
    """
    Creates parameters to configure a Kafka consumer.

    This function creates a dictionary containing the configuration parameters required to create a Kafka consumer.
    It includes parameters such as bootstrap servers, security protocol, SASL mechanisms, SASL username and password,
    consumer group ID, and auto offset reset.

    :param kafka_server: (str) The address of the Kafka server.

    :return: (dict) A dictionary containing Kafka consumer configuration parameters.
    """

    
    conf_kafka = {
        'bootstrap.servers': f'{kafka_server}',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'W2KRRRJ4IYCCJKFC',
        'sasl.password': '2iBBe2PmCFcbFvZDVh8Bmr16p8hDFaggqrYg0D1p9hVuiH27SAVnbpqhlZ0Uy6Xb',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    return conf_kafka