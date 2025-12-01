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
            DROP TABLE IF EXISTS updates;
            CREATE TABLE updates(
                    trip_id VARCHAR, 
                    route_id VARCHAR,
                    start_time TIME, 
                    start_date DATE, 
                    schedule_relationship VARCHAR,
                    vehicle_id VARCHAR, 
                    timestamp DATETIME,
                    delay BIGINT,
                    num_delays BIGINT);
        """)
        print("Updates table has been created")
        return True
        
    except Exception as e:
        print(e)

#task for update data entries
@task(retries=1000, retry_delay_seconds=10, cache_key_fn=None)
def insert_update_record(kafka_key, offset, value, con: duckdb.DuckDBPyConnection):
    #Insert an update record into the database
    #graceful error handling
    try:
        # Extract properties from the nested structure
        trip_id = value["trip_update"]["trip"]["trip_id"]
        route_id = value["trip_update"]["trip"]["route_id"]
        start_time = value["trip_update"]["trip"]["start_time"]
        start_date = value["trip_update"]["trip"]["start_date"]
        #properly format to SQL acceptable date format
        formatted_start_date = str(start_date[:4])+"-"+str(start_date[4:6])+"-"+str(start_date[6:])
        schedule_relationship = value["trip_update"]["trip"]["schedule_relationship"]
        #map out the numerical value to actual value
        SCHEDULE_REL_MAP = {
            0: "SCHEDULED",
            1: "ADDED",
            2: "UNSCHEDULED",
            3: "CANCELED"
        }
        sr_text = SCHEDULE_REL_MAP.get(schedule_relationship, "UNKNOWN")    
        vehicle_id = value["trip_update"]["vehicle_id"]
        timestamp = value["trip_update"]["timestamp"]
        #convert from unix time
        new_timestamp = datetime.fromtimestamp(timestamp)
        delay = value["trip_update"]["delay"]    
        num_delays = len(value["trip_update"]["stop_time_updates"])  
        #insert into duckdb
        con.execute("INSERT INTO updates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", [trip_id, route_id, start_time, formatted_start_date, sr_text, vehicle_id, new_timestamp, delay, num_delays])
        print(f"The bus with the id {trip_id} has been inputted into the updates table")
        return True
    except Exception as e:
        print(f"Error inserting record: {e}")
        return False
    
#task to insert all updates into duckdb
@task(retries=1000, retry_delay_seconds=10, cache_key_fn=None)
def updates_duckdb():
    #graceful error handling
    try:
        app = Application(
            broker_address=KAFKA_BROKER,
            consumer_group="metro_reader_v2",
            auto_offset_reset="earliest",
        )
        print("Connected to Kafka")
        #act as a consumer from kafka
        with app.get_consumer() as consumer:
            #subscribe to metro changes
            consumer.subscribe(["metro-changes"])
            print("Subscribed to the metro-changes topic.")
            #run as long as the variable is true
            with duckdb.connect('metro.duckdb', read_only = False) as con:
                while True:
                    #poll kafka, wait 5 seconds
                    msg = consumer.poll(5)
                    #if there are no messages, continue
                    if msg is None:
                        print("No new messages")
                        continue
                    #error handling
                    elif msg.error() is not None:
                        #reset quiet_polls
                        quiet_polls = 0
                        raise Exception(msg.error())
                        
                    else:
                        #reset quiet_polls
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
                        if insert_update_record(key, offset, value, con):
                            print(f"✓ Inserted record {offset} into DuckDB")
                        else:
                            print(f"✗ Failed to insert record {offset}")
                        #let kafka know that this message has been consumed
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
        t2 = updates_duckdb()
    except Exception as e:
        print(f"{e}")

if __name__ == "__main__":
    #graceful error handling
    try:
        #run the flow
        consumer_flow()
    except KeyboardInterrupt:
        pass