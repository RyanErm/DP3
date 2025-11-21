# Ryan Ermovick
# Dev Aswani

#import packages
import os
import json
from datetime import datetime
import requests
from quixstreams import Application
from google.transit import gtfs_realtime_pb2
#set up kafka broker and metro API address
KAFKA_BROKER = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092"
METRO_URL = "https://api.wmata.com/gtfs/bus-gtfsrt-tripupdates.pb"

API_KEY = "4d7182489af74b9f82486ea11265d85d"

headers = {"api_key": API_KEY}


class MetroStreamer: #writing own class
    def __init__(self):
        """Initialize the Metro streamer with Kafka producer."""
        self.app = Application( #initialize app
            broker_address=KAFKA_BROKER,
            producer_extra_config={
                # Resolve localhost to 127.0.0.1 to avoid IPv6 issues
                "broker.address.family": "v4",
            }
        )
        self.topic = self.app.topic(
            name="metro-changes",
            value_serializer="json",
        )
        print(f"Connected to Kafka broker at {KAFKA_BROKER}")
        print(f"Producing to topic: {self.topic.name}")

    def publish_to_kafka(self):
        print("test")
        response = requests.get(METRO_URL, headers=headers)
        data = response.content
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(data)

        with self.app.get_producer() as producer:
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
                        },
                        "vehicle": {
                            "id": entity.trip_update.vehicle.id,
                        },
                        "stop_time_updates": [
                            {
                                "stop_sequence": stu.stop_sequence,
                                "stop_id": stu.stop_id,
                                "arrival": {
                                    "time": stu.arrival.time
                                } if stu.HasField("arrival") else None,
                                "departure": {
                                    "time": stu.departure.time
                                } if stu.HasField("departure") else None,
                            }
                            for stu in entity.trip_update.stop_time_update
                        ],
                        "timestamp": entity.trip_update.timestamp,
                        "delay": entity.trip_update.delay,
                    }
                }

                serialized = self.topic.serialize(
                    key=event_key,
                    value=event_dict
                )

                producer.produce(
                    topic=self.topic.name,
                    key=serialized.key,   
                    value=serialized.value
                )
        


if __name__ == "__main__":
    streamer = MetroStreamer()
    streamer.publish_to_kafka()
