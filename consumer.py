# Ryan Ermovick
# Dev Aswani

#import packages
from quixstreams import Application
import os
import json
from requests_sse import EventSource
from datetime import datetime
#set up kafka broker and metro API address
KAFKA_BROKER = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092"
METRO_URL = "https://api.wmata.com/gtfs/bus-gtfsrt-tripupdates.pb"


class MetroStreamer: #writing own class
    def __init__(self):
        """Initialize the Metro streamer with Kafka producer."""
        self.app = Application( #initialize app
            broker_address="metro-producer",
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

    def publish_to_kafka(self, change_event: dict) -> bool:
        """
        Publish a metro delay to Kafka.
        
        Args:
            change_event: Dictionary containing the change event data
            
        Returns:
            True if published successfully, False otherwise
        """
        try:
            # Use the event ID as the key for partitioning
            event_id = str(change_event.get('id', ''))
            
            # Serialize the event
            serialized = self.topic.serialize(key=event_id, value=change_event)
            
            # Produce to Kafka
            with self.app.get_producer() as producer:
                producer.produce(
                    topic=self.topic.name,
                    key=serialized.key,
                    value=serialized.value
                )
            return True
            
        except Exception as e:
            print(f"Error publishing to Kafka: {e}")
            return False

    def process_change(self, change_event: dict) -> None:
        """
        Process a single Wikipedia change event.
        
        Args:
            change_event: Dictionary containing the change event data
        """
        # Extract key information
        event_type = change_event.get('type', 'unknown')
        title = change_event.get('title', 'N/A')
        user = change_event.get('user', 'anonymous')
        wiki = change_event.get('server_name', 'unknown')
        timestamp = change_event.get('timestamp')
        
        # Print the change
        #PRINT SOMETHING
        
        # Publish to Kafka
        if self.publish_to_kafka(change_event):
            print(f"  ✓ Published to Kafka (ID: {change_event.get('id')})")



    def run(self):
        """
        Connect to Metro API and continuously process events.
        """
        print(f"Connecting to Wikipedia SSE stream: {METRO_URL_URL}")
        print("Press Ctrl+C to stop\n")
        
        try:

            #FIX THIS
            # Set up headers with proper User-Agent (required by Wikimedia)
            headers = {
                'User-Agent': 'WikipediaStreamer/1.0 (Educational Project; Python/requests-sse)',
                'Accept': 'text/event-stream' #this entry is needed, user agent is needed. Most APIs dont just want anonynmous
            }
            
            # Connect to the SSE stream with headers
            with EventSource(METRO_URL, headers=headers) as source:
                # EDIT
                for event in source:
                    # The message data is in event.data as a JSON string
                    if event.data:
                        try:
                            change_event = json.loads(event.data)
                            self.process_change(change_event)
                        except json.JSONDecodeError as e:
                            print(f"Error parsing event: {e}")
                        except Exception as e:
                            print(f"Error processing event: {e}")
                            
        except KeyboardInterrupt:
            print("\n\nStopping stream...")
        except Exception as e:
            print(f"Error connecting to stream: {e}")
            raise


if __name__ == "__main__":
    streamer = MetroStreamer()
    streamer.run()

