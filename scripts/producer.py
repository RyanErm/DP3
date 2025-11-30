# Ryan Ermovick
# Dev Aswani

#import packages
import json
from datetime import datetime
import requests
from quixstreams import Application
from google.transit import gtfs_realtime_pb2
from prefect import task, flow
import time

#set up kafka broker, metro API addresses, and API information
KAFKA_BROKER = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092"
METRO_URL_UPDATES = "https://api.wmata.com/gtfs/bus-gtfsrt-tripupdates.pb"
METRO_URL_POSITIONS = "https://api.wmata.com/gtfs/bus-gtfsrt-vehiclepositions.pb"
API_KEY = "4d7182489af74b9f82486ea11265d85d"
headers = {"api_key": API_KEY}

#task to produce bus updates to kafka
@task(retries=100, retry_delay_seconds=10)
def publish_to_kafka_updates(app, topic):
    #graceful error handling
    try:
        #Connect to the metro updates AIP
        response = requests.get(METRO_URL_UPDATES, headers=headers)
        print("Connecting to the Metro Bus Update API")
        #Convert the data from gtfs 
        data = response.content
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(data)
        print(f"{len(feed.entity)} messages have been collected from the updates API")
        #Connect to Kafka as a Producer        
        with app.get_producer() as producer:
            #Loop through each metro bus found
            for entity in feed.entity:
                #if it does not have the fields required, skip
                if not entity.HasField("trip_update"):
                    continue
                #create the id out of the bus id
                event_key = entity.id
                #set up the entity dictionary
                #grab all the data
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
                #serializing the data to fit kafkas format
                serialized = topic.serialize(
                    key=event_key,
                    value=event_dict
                )
                #produce the data
                producer.produce(
                    topic=topic.name,
                    key=serialized.key,   
                    value=serialized.value
                )
                print(f"The bus with the id {event_key} has been inputted into Kafka")

    except Exception as e:
        print(f"e")
        

#task to produce bus positions to kafka
@task(retries=100, retry_delay_seconds=10)
def publish_to_kafka_positions(app, topic):
    #graceful error handling
    try:
        #connect to the metro positions api
        response = requests.get(METRO_URL_POSITIONS, headers=headers)
        print("Connecting to the Metro Bus Position API")
        #parse the data
        data = response.content
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(data)
        print(f"{len(feed.entity)} messages have been collected from the position API")
        #Connect to kafka as a producer
        with app.get_producer() as producer:
            #loop through each bus
            for entity in feed.entity:
                #ensure the proper data is present
                if not entity.HasField("vehicle"):
                    continue
                #grab the bus id as the key
                event_key = entity.id
                #gather all necessary information
                event_dict = {
                    "trip_id": entity.vehicle.trip.trip_id,
                    "start_time": entity.vehicle.trip.start_time, 
                    "start_date": entity.vehicle.trip.start_date,
                    "schedule_relationship": entity.vehicle.trip.schedule_relationship,
                    "route_id": entity.vehicle.trip.route_id,
                    "position": {
                        "latitude": entity.vehicle.position.latitude,
                        "longitude": entity.vehicle.position.longitude,
                        "speed": entity.vehicle.position.speed,
                        "bearing": entity.vehicle.position.bearing,
                    },
                    "vehicle":{
                        "vehicle": entity.vehicle.vehicle.id,
                        "label": entity.vehicle.vehicle.label,
                    },
                    "current_status": entity.vehicle.current_status, 
                    "timestamp": entity.vehicle.timestamp
                }
                #serialize the data
                serialized = topic.serialize(
                    key=event_key,
                    value=event_dict
                )
                #produce the data
                producer.produce(
                    topic=topic.name,
                    key=serialized.key,   
                    value=serialized.value
                )
                print(f"The bus with the id {event_key} has been inputted into Kafka")
    except Exception as e:
        print(f"e")


#flow that runs all the tasks
@flow(name = "Metro-flow", log_prints = True)
def consumer_flow():
    #graceful error handling
    try:
        #set up the app
        app = Application(
        broker_address=KAFKA_BROKER,
        producer_extra_config={
            # Resolve localhost to 127.0.0.1 to avoid IPv6 issues
            "broker.address.family": "v4",
        }
        )
        print(f"Connected to Kafka broker at {KAFKA_BROKER}")

        #choose the metro changes topic
        topic1 = app.topic(
            name="metro-changes",
            value_serializer="json",
        )
        print(f"Producing to topic: {topic1.name}")

        #choose the metro positions topic
        topic2 = app.topic(
            name="metro-positions",
            value_serializer="json",
        )
        print(f"Producing to topic: {topic2.name}")

        #min is the counter for how long data should be collected
        min = 0
        while min<1: 
            #publish both bus updates and positions
            publish_to_kafka_updates(app, topic1)
            publish_to_kafka_positions(app, topic2)
            min+=1
            print(f"Data has been collected for {min} minutes so far")
            #wait at least 45 seconds for new data
            time.sleep(45)
    except Exception as e:
        print(f"e")


#run the whole code
if __name__ == "__main__":
    #graceful error handling
    try:
        consumer_flow()
    except Exception as e:
        print(f"e")
