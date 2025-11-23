#Ryan Ermovick
#Dev Aswani
from quixstreams import Application
import json
import time
import requests
import os
from datetime import datetime
import duckdb
from prefect import task, flow

KAFKA_BROKER = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092"

@task(retries = 3, retry_delay_seconds=10)
def setup():
    app = Application(
        broker_address=KAFKA_BROKER,
        consumer_group="metro_reader",
        auto_offset_reset="earliest",
    )
    con = duckdb.connect(database='metro.duckdb', read_only=False)
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
                current_status VARCHAR);
    """)
    return app



@task(retries=100, retry_delay_seconds=10, cache_key_fn=None)
def insert_update_record(kafka_key, offset, value):
    """Insert an update record into the database"""
    try:
        #connecting
        con = duckdb.connect(database='metro.duckdb', read_only=False)
        # Extract properties from the nested structure
        trip_id = value["trip_update"]["trip"]["trip_id"]
        route_id = value["trip_update"]["trip"]["route_id"]
        start_time = value["trip_update"]["trip"]["start_time"]
        start_date = value["trip_update"]["trip"]["start_date"]
        formatted_start_date = str(start_date[:4])+"-"+str(start_date[4:6])+"-"+str(start_date[6:])
        schedule_relationship = value["trip_update"]["trip"]["schedule_relationship"]
        SCHEDULE_REL_MAP = {
            0: "SCHEDULED",
            1: "ADDED",
            2: "UNSCHEDULED",
            3: "CANCELED"
        }
        sr_text = SCHEDULE_REL_MAP.get(schedule_relationship, "UNKNOWN")    
        vehicle_id = value["trip_update"]["vehicle_id"]
        timestamp = value["trip_update"]["timestamp"] #FIX
        new_timestamp = datetime.fromtimestamp(timestamp)
        delay = value["trip_update"]["delay"]    
        num_delays = len(value["trip_update"]["stop_time_updates"])  
        con.execute("INSERT INTO updates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", [trip_id, route_id, start_time, formatted_start_date, sr_text, vehicle_id, new_timestamp, delay, num_delays])
        

        return True
    except Exception as e:
        print(f"Error inserting record: {e}")
        return False
    


@task(retries=100, retry_delay_seconds=10, cache_key_fn=None)
def insert_position_record(kafka_key, offset, value):
    """Insert an update record into the database"""
    try:
        #connecting
        con = duckdb.connect(database='metro.duckdb', read_only=False)
        # Extract properties from the nested structure
        trip_id = value["trip_id"]
        route_id = value["route_id"]
        start_time = value["start_time"] #UPDATE THIS
        start_date = value["start_date"] #UPDATE THIS
        formatted_start_date = str(start_date[:4])+"-"+str(start_date[4:6])+"-"+str(start_date[6:])
        schedule_relationship = value["schedule_relationship"]
        SCHEDULE_REL_MAP = {
            0: "SCHEDULED",
            1: "ADDED",
            2: "UNSCHEDULED",
            3: "CANCELED"
        }
        sr_text = SCHEDULE_REL_MAP.get(schedule_relationship, "UNKNOWN")    
        timestamp = value["timestamp"] #FIX
        new_timestamp = datetime.fromtimestamp(timestamp)
        latitude = value["position"]["latitude"]
        longitude = value["position"]["longitude"]
        speed = value["position"]["speed"]
        bearing = value["position"]["bearing"]
        vehicle_id = value["vehicle"]["vehicle"]
        vehicle_label = value["vehicle"]["label"]
        cur_status = value["current_status"]
        status_map = {
            0: "INCOMING_AT",
            1: "STOPPED_AT",
            2: "IN_TRANSIT_TO"
        }
        current_status = status_map.get(cur_status, "UNKNOWN")
        con.execute("INSERT INTO positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", [trip_id, route_id, start_time, formatted_start_date, vehicle_id, vehicle_label, new_timestamp, latitude, longitude, speed, bearing, current_status])
        return True
    except Exception as e:
        print(f"Error inserting record: {e}")
        return False
    
@task(retries=10, retry_delay_seconds=10, cache_key_fn=None)
def updates_duckdb(app):
    con = duckdb.connect(database='metro.duckdb', read_only=False)
    with app.get_consumer() as consumer:
        consumer.subscribe(["metro-changes"])
        state_variable = True
        while state_variable == True:

            msg = consumer.poll(5)
            if msg is None:
                print("Good for now")
                state_variable = False
            elif msg.error() is not None:
                raise Exception(msg.error())
            else:
                key = msg.key().decode("utf8")
                value = json.loads(msg.value())
                offset = msg.offset()
                print(f"{offset} {key} {value}")
                # Insert into Duckdb
                if insert_update_record(key, offset, value):
                    print(f"✓ Inserted record {offset} into DuckDB")
                else:
                    print(f"✗ Failed to insert record {offset}")
                consumer.store_offsets(msg)

@task(cache_key_fn=None)
def positions_duckdb(app):
    con = duckdb.connect(database='metro.duckdb', read_only=False)
    with app.get_consumer() as consumer:
        consumer.subscribe(["metro-positions"])
        state_variable = True
        while state_variable == True:

            msg = consumer.poll(5)
            if msg is None:
                print("Good for now")
                state_variable = False
            elif msg.error() is not None:
                raise Exception(msg.error())
            else:
                key = msg.key().decode("utf8")
                value = json.loads(msg.value())
                offset = msg.offset()
                print(f"{offset} {key} {value}")
                # Insert into Duckdb
                if insert_position_record(key, offset, value):
                    print(f"✓ Inserted record {offset} into DuckDB")
                else:
                    print(f"✗ Failed to insert record {offset}")
                consumer.store_offsets(msg)

@flow(name = "Metro-flow", log_prints = True)
def consumer_flow():
    app= setup()
    updates_duckdb(app)
    print("HI")
    positions_duckdb(app)
    print("HI")


if __name__ == "__main__":
    try:
        consumer_flow()
    except KeyboardInterrupt:
        pass