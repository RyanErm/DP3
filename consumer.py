#Ryan Ermovick
#Dev Aswani


from quixstreams import Application
import json
import time
import requests
import os
from datetime import datetime
import duckdb

KAFKA_BROKER = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092"


def insert_seismic_record(con, kafka_key, offset, value):
    """Insert an update record into the database"""
    try:
        #connecting
        

        ##############################################################################################

        
        # Extract properties from the nested structure
        trip_id = value["trip_update"]["trip"]["trip_id"]
        route_id = value["trip_update"]["trip"]["route_id"]
        start_time = value["trip_update"]["trip"]["start_time"]
        start_date = value["trip_update"]["trip"]["start_date"]
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
        print(num_delays)

        con.execute("INSERT INTO update_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", [trip_id, route_id, start_time, start_date, sr_text, vehicle_id, new_timestamp, delay, num_delays])
        

        ##############################################################################################################
        return True
    except Exception as e:
        print(f"Error inserting record: {e}")
        return False

def main():
    
    app = Application(
        broker_address=KAFKA_BROKER,
        consumer_group="metro_reader",
        auto_offset_reset="earliest",
    )
    con = duckdb.connect(database='metro.duckdb', read_only=False)
    con.execute(""" 
        DROP TABLE IF EXISTS update_data;
        CREATE TABLE update_data(
                trip_id VARCHAR, 
                route_id VARCHAR,
                start_time VARCHAR, 
                start_date BIGINT, 
                schedule_relationship VARCHAR,
                vehicle_id VARCHAR, 
                timestamp DATETIME,
                delay BIGINT,
                num_delays BIGINT);
    """)

    with app.get_consumer() as consumer:
        consumer.subscribe(["metro-changes"])

        while True:
            msg = consumer.poll(5)

            if msg is None:
                print("Waiting...")
            elif msg.error() is not None:
                raise Exception(msg.error())
            else:
                key = msg.key().decode("utf8")
                value = json.loads(msg.value())
                offset = msg.offset()

                print(f"{offset} {key} {value}")
                
                # Insert into MySQL
                if insert_seismic_record(con, key, offset, value):
                    print(f"✓ Inserted record {offset} into DuckDB")
                else:
                    print(f"✗ Failed to insert record {offset}")
                
                consumer.store_offsets(msg)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass