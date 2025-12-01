#Ryan Ermovick
#Dev Aswani

#import necessary packages
from quixstreams import Application
import json
import time
import requests
import os
from datetime import datetime
import duckdb
from prefect import task, flow
from prefect.cache_policies import NO_CACHE


#set up kafka broker address
KAFKA_BROKER = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092"

#tas to set up the application connection and duckdb tables
@task(retries = 3, retry_delay_seconds=10)
def setup():
    #graceful error handling
    try:

        #connect to Duckdb
        con = duckdb.connect(database='metro.duckdb', read_only=False)
        print("Connected to Duckdb")

        #create duckdb table with appropriate fields
        con.execute(""" 
            DROP TABLE IF EXISTS positions;
            CREATE TABLE positions(
                    trip_id VARCHAR, 
                    route_id VARCHAR, 
                    start_time TIME, 
                    start_date DATE, 
                    vehicle_id VARCHAR,
                    vehicle_label VARCHAR,
                    timestamp DATETIME, 
                    latitude DECIMAL(7, 5), 
                    longitude DECIMAL(8, 5), 
                    speed DECIMAL(9, 5), 
                    bearing BIGINT, 
                    current_status VARCHAR, 
                    schedule_relationship VARCHAR);
        """)
        print("Positions table has been created")
        return True
        
    except Exception as e:
        print(e)


#function for position data entries
@task(retries=1000, retry_delay_seconds=10, cache_key_fn=None, cache_policy=NO_CACHE)
def insert_position_record(kafka_key, offset, value, con:duckdb.DuckDBPyConnection):
    #Insert an update record into the database
    #graceful error handling
    try:
        # Extract properties from the nested structure
        trip_id = value["trip_id"]
        route_id = value["route_id"]
        start_time = value["start_time"] 
        start_date = value["start_date"] 
        #properly format to SQL acceptable date format
        formatted_start_date = str(start_date[:4])+"-"+str(start_date[4:6])+"-"+str(start_date[6:])
        schedule_relationship = value["schedule_relationship"]
        #map out the numerical value to actual value
        SCHEDULE_REL_MAP = {
            0: "SCHEDULED",
            1: "ADDED",
            2: "UNSCHEDULED",
            3: "CANCELED"
        }
        sr_text = SCHEDULE_REL_MAP.get(schedule_relationship, "UNKNOWN")    
        timestamp = value["timestamp"] 
        #convert from unix time
        new_timestamp = datetime.fromtimestamp(timestamp)
        latitude = value["position"]["latitude"]
        longitude = value["position"]["longitude"]
        speed = value["position"]["speed"]
        bearing = value["position"]["bearing"]
        vehicle_id = value["vehicle"]["vehicle"]
        vehicle_label = value["vehicle"]["label"]
        cur_status = value["current_status"]
        #map out the numerical value to actual value
        status_map = {
            0: "INCOMING_AT",
            1: "STOPPED_AT",
            2: "IN_TRANSIT_TO"
        }
        current_status = status_map.get(cur_status, "UNKNOWN")
        #insert into duckdb
        con.execute("INSERT INTO positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", [trip_id, route_id, start_time, formatted_start_date, vehicle_id, vehicle_label, new_timestamp, latitude, longitude, speed, bearing, current_status, sr_text])
        print(f"The bus with the id {trip_id} has been inputted into the positions table")
        return True
    except Exception as e:
        print(f"Error inserting record: {e}")
        return False

#task to insert all positions into duckdb
@task(retries=1000, retry_delay_seconds=10, cache_key_fn=None, cache_policy=NO_CACHE)
def positions_duckdb():
    #graceful error handling
    try:
        app = Application(
            broker_address=KAFKA_BROKER,
            consumer_group="metro_reader_v2",
            auto_offset_reset="earliest",
        )
        print("Connected to Kafka")
        #connect as a consumer
        with app.get_consumer() as consumer:
            #subscribe to the positions topic
            consumer.subscribe(["metro-positions"])
            print("Subscribed to the metro-positions topic")
            
            with duckdb.connect('metro.duckdb', read_only = False) as con:
                while True:
                    #poll for messages, wait 5 seconds
                    msg = consumer.poll(5)
                    #if there are no messages, continue
                    if msg is None:
                        print("No new messages for now")
                        continue
                    #error handling
                    elif msg.error() is not None:
                        #reset conditional 
                        quiet_polls = 0
                        raise Exception(msg.error())
                    else:
                        #reset conditional
                        quiet_polls = 0
                        print("Got a message!")
                        #get key
                        key = msg.key().decode("utf8")
                        #get dictionary
                        value = json.loads(msg.value())
                        #get data position
                        offset = msg.offset()
                        #print out
                        print(f"Here is the offset: {offset}, key: {key}, and value: {value}")
                        # Insert into Duckdb
                        if insert_position_record(key, offset, value, con):
                            print(f"✓ Inserted record {offset} into DuckDB")
                        else:
                            print(f"✗ Failed to insert record {offset}")
                        consumer.store_offsets(msg)
                    
    except Exception as e:
        print(f"{e}")

#flow to run everything
@flow(name = "Metro-flow", log_prints = True)
def consumer_flow():
    #graceful error handling
    try:    
        #setup 
        t1 = setup()
        print("Fully set up!")
        #consume update data
        t2 = positions_duckdb()
    except Exception as e:
        print(f"{e}")

if __name__ == "__main__":
    #graceful error handling
    try:
        #run the flow
        consumer_flow()
    except KeyboardInterrupt:
        pass