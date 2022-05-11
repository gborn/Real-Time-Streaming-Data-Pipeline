
from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random
import requests
from restcountries import RestCountryApiV2 as rapi
from typing import List
import os


HOST_IP_ADDRESS = os.environ.get('HOST_IP_ADDRESS')
TOPIC_NAME_CONS = "server-live-status"
BOOTSTRAP_SERVERS_CONS = f'{HOST_IP_ADDRESS}:9092'
RANDOM_USER_API_URL = "https://randomuser.me/api/0.8"


def publish_event(event_id: int, 
                  event_server_status_color_name_severity_level_types: List[str], 
                  event_server_types: List[str], 
                  producer: KafkaProducer) -> bool:

        """
        Method to generate an event and send it to Kafka Producer listening at BOOTSTRAP_SERVERS_CONS
        """
        try:

            # generate an event and use user's nationality to get country name and capital city
            response = requests.get(RANDOM_USER_API_URL)
            event_country_code = response.json()['nationality']

            country_obj =  rapi.get_country_by_country_code(alpha=event_country_code)
            print(country_obj.name)
            print(country_obj.capital)

            # build event message
            event_country_name = country_obj.name
            event_city_name = country_obj.capital

            event_message = {}
            event_datetime = datetime.now()

            event_message["event_server_status_color_name_severity_level"] = random.choice(event_server_status_color_name_severity_level_types)
            event_message["event_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")
            event_message["event_server_type"] = random.choice(event_server_types)
            event_message["event_country_code"] = event_country_code
            event_message["event_country_name"] = event_country_name
            event_message["event_city_name"] = event_city_name
            event_message["event_estimated_issue_resolution_time"] = round(random.uniform(1.5, 10.5))

            # other params
            event_message["event_server_status_other_param_1"] = ""
            event_message["event_server_status_other_param_2"] = ""
            event_message["event_server_status_other_param_3"] = ""
            event_message["event_server_status_other_param_4"] = ""
            event_message["event_server_status_other_param_5"] = ""

            event_message["event_server_config_other_param_1"] = ""
            event_message["event_server_config_other_param_2"] = ""
            event_message["event_server_config_other_param_3"] = ""
            event_message["event_server_config_other_param_4"] = ""
            event_message["event_server_config_other_param_5"] = ""

            # send to kafka consumer with topic name TOPIC_NAME_CONS
            print("Printing message id: " + str(event_id))
            event_message["event_id"] = str(event_id)
            print("Sending message to Kafka topic: " + TOPIC_NAME_CONS)
            print("Message to be sent: ", event_message)
            print()
            producer.send(TOPIC_NAME_CONS, event_message)

        except Exception as ex:
            print("Event Message Construction Failed. ")
            print(ex)
            return False

        time.sleep(1)
        return True

if __name__ == "__main__":
    print("Data Center Server Live Status Simulator | Kafka Producer Application Started ... ")

    # kafka producer to send events
    kafka_producer_obj = None
    kafka_producer_obj = KafkaProducer(
                            bootstrap_servers=BOOTSTRAP_SERVERS_CONS,
                            value_serializer=lambda x: dumps(x).encode('utf-8'),
                        )

    # types of events
    event_server_status_color_name_severity_level_list = ["Red|Severity 1", "Orange|Severity 2", "Green|Severity 3"]

    # types of event server
    event_server_type_list = ["Application Servers", "Client Servers", "Collaboration Servers", "FTP Servers", "List Servers",
                        "Mail Servers", "Open Source Servers", "Proxy Servers", "Real-Time Communication Servers", "Server Platforms",
                        "Telnet Servers", "Virtual Servers", "Web Servers"]

    # send event to message queue
    NUM_EVENTS = 10
    results = [publish_event(
                        event_id, 
                        event_server_status_color_name_severity_level_list, 
                        event_server_type_list, 
                        kafka_producer_obj
                    )  for event_id in range(1, NUM_EVENTS + 1)]

    print(f'Generated {sum(result for result in results if type(result) == bool)} events successfully')
    print("Data Center Server Live Status Simulator | Kafka Producer Application Completed. ")