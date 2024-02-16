from kafka.admin import KafkaAdminClient, NewTopic


def create_kafka_topic(
    topic_name,
    num_partitions=1,
    replication_factor=1,
    broker={"bootstrap_servers": "localhost:9092"},
):
    """
    Creates topic if it doesn't exist
    :param topic_name: The topic name
    :param num_partitions: The number partitions
    :param replication_factor: The replication factor
    :param broker: The broker
    """
    # Create KafkaAdminClient
    admin_client = KafkaAdminClient(**broker)

    # Define topic configuration
    topic_config = {
        "num_partitions": num_partitions,
        "replication_factor": replication_factor,
    }

    # Create NewTopic instance
    topic = NewTopic(topic_name, **topic_config)

    # Create topic
    try:
        admin_client.create_topics([topic])
        print(f"Topic '{topic_name}' created.")
    except Exception as e:
        print(f"Failed to create topic '{topic_name}': {e}")
