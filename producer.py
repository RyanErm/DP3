# Ryan Ermovick
# Dev Aswani

#import packages
import os
import json
from datetime import datetime
import requests
from quixstreams import Application
from google.transit import gtfs_realtime_pb2
from prefect import task, flow
import time
#set up kafka broker and metro API address
KAFKA_BROKER = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092"
METRO_URL_UPDATES = "https://api.wmata.com/gtfs/bus-gtfsrt-tripupdates.pb"
METRO_URL_POSITIONS = "https://api.wmata.com/gtfs/bus-gtfsrt-vehiclepositions.pb"

API_KEY = "4d7182489af74b9f82486ea11265d85d"

headers = {"api_key": API_KEY}

@task(retries=100, retry_delay_seconds=10)
def publish_to_kafka_updates(app, topic):
    print("test")
    response = requests.get(METRO_URL_UPDATES, headers=headers)
    data = response.content
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)
    with app.get_producer() as producer:
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue
            event_key = entity.id
            event_dict = {
                "trip_update": {
                    "trip": {
                        "trip_id": entity.trip_update.trip.trip_id,
                        "route_id": entity.trip_update.trip.route_id,
                        "start_time": entity.trip_update.trip.start_time,
                        "start_date": entity.trip_update.trip.start_date,
                        "schedule_relationship": entity.trip_update.trip.schedule_relationship,
                        "direction_id": entity.trip_update.trip.direction_id,
                    },
                    "vehicle_id": entity.trip_update.vehicle.id,
                    "timestamp": entity.trip_update.timestamp,
                    "delay": entity.trip_update.delay,
                    "stop_time_updates": [
                        {
                            "stop_sequence": stu.stop_sequence,
                            "stop_id": stu.stop_id,
                            "schedule_relationship": stu.schedule_relationship,
                            "arrival": {
                                "time": stu.arrival.time
                            } if stu.HasField("arrival") else None,
                            "departure": {
                                "time": stu.departure.time
                            } if stu.HasField("departure") else None,
                        }
                        for stu in entity.trip_update.stop_time_update
                    ],
                }
            }

            serialized = topic.serialize(
                key=event_key,
                value=event_dict
            )

            producer.produce(
                topic=topic.name,
                key=serialized.key,   
                value=serialized.value
            )



@task(retries=100, retry_delay_seconds=10)
def publish_to_kafka_positions(app, topic):
    print("test")
    response = requests.get(METRO_URL_POSITIONS, headers=headers)
    data = response.content
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)

    ######################################## Update here
    with app.get_producer() as producer:
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue
            event_key = entity.id
            event_dict = {
                "trip_update": {
                    "trip": {
                        "trip_id": entity.trip_update.trip.trip_id,
                        "route_id": entity.trip_update.trip.route_id,
                        "start_time": entity.trip_update.trip.start_time,
                        "start_date": entity.trip_update.trip.start_date,
                        "schedule_relationship": entity.trip_update.trip.schedule_relationship,
                        "direction_id": entity.trip_update.trip.direction_id,
                    },
                    "vehicle_id": entity.trip_update.vehicle.id,
                    "timestamp": entity.trip_update.timestamp,
                    "delay": entity.trip_update.delay,
                    "stop_time_updates": [
                        {
                            "stop_sequence": stu.stop_sequence,
                            "stop_id": stu.stop_id,
                            "schedule_relationship": stu.schedule_relationship,
                            "arrival": {
                                "time": stu.arrival.time
                            } if stu.HasField("arrival") else None,
                            "departure": {
                                "time": stu.departure.time
                            } if stu.HasField("departure") else None,
                        }
                        for stu in entity.trip_update.stop_time_update
                    ],
                }
            }

            serialized = topic.serialize(
                key=event_key,
                value=event_dict
            )

            producer.produce(
                topic=topic.name,
                key=serialized.key,   
                value=serialized.value
            )

@flow(name = "Metro-flow")
def my_flow():
    app = Application( #initialize app
    broker_address=KAFKA_BROKER,
    producer_extra_config={
        # Resolve localhost to 127.0.0.1 to avoid IPv6 issues
        "broker.address.family": "v4",
    }
    )

    print(f"Connected to Kafka broker at {KAFKA_BROKER}")
    topic1 = app.topic(
        name="metro-changes",
        value_serializer="json",
    )
    
    print(f"Producing to topic: {topic1.name}")

    topic2 = app.topic(
        name="metro-positions",
        value_serializer="json",
    )

    print(f"Producing to topic: {topic2.name}")
    min = 0
    while min<=60:
        publish_to_kafka_updates(app, topic1)
        publish_to_kafka_positions(app, topic2)
        min+=1
        time.sleep(60)

        


if __name__ == "__main__":
    my_flow()


