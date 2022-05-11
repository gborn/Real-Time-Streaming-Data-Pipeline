from kafka import KafkaConsumer
from json import loads
import os

HOST_IP_ADDRESS = os.environ.get('HOST_IP_ADDRESS')
KAFKA_CONSUMER_GROUP_NAME_CONS = "test_consumer_group"
KAFKA_TOPIC_NAME_CONS = "server-live-status"
KAFKA_BOOTSTRAP_SERVERS_CONS = f'{HOST_IP_ADDRESS}:9092'

if __name__ == "__main__":
    """
    Test driver program to test above Kafka Producer
    """

    print("Kafka Consumer Application Started ... ")

    try:
        # auto_offset_reset='latest'
        # auto_offset_reset='earliest'
        consumer = KafkaConsumer(
                        KAFKA_TOPIC_NAME_CONS,
                        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS_CONS],
                        auto_offset_reset='earliest',
                        enable_auto_commit=True,
                        group_id=KAFKA_CONSUMER_GROUP_NAME_CONS,
                        value_deserializer=lambda x: loads(x.decode('utf-8'))
                    )


        for message in consumer:
            print("Key: ", message.key)
            message = message.value
            print("Message received: ", message)

    except Exception as ex:
        print("Failed to read kafka message.")
        print(ex)

    print("Kafka Consumer Application Completed. ")