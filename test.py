import requests
from google.transit import gtfs_realtime_pb2

WMATA_VEHICLE_URL = "https://api.wmata.com/gtfs/bus-gtfsrt-vehiclepositions.pb"
API_KEY = "4d7182489af74b9f82486ea11265d85d"

headers = {"api_key": API_KEY}

response = requests.get(WMATA_VEHICLE_URL, headers=headers)
data = response.content

feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(data)

buses = []


print(len(feed.entity))







for entity in feed.entity:


    print(entity)
    break




