from google.transit import gtfs_realtime_pb2
import os

PB_PATH = os.path.expanduser("~/Downloads/realtime-free.pb")

def parse_pb():
    feed = gtfs_realtime_pb2.FeedMessage()
    with open(PB_PATH, "rb") as f:
        feed.ParseFromString(f.read())

    print(f"--- Decoded GTFS-RT Message ---")
    print(f"Header: {feed.header}")
    print(f"Total Entities: {len(feed.entity)}")

    # Look at the first 5 entities
    for i, entity in enumerate(feed.entity[:5]):
        print(f"\n[Entity {i}] ID: {entity.id}")
        if entity.HasField('trip_update'):
            print(f"  Type: TripUpdate")
            print(f"  Trip ID: {entity.trip_update.trip.trip_id}")
            print(f"  Route ID: {entity.trip_update.trip.route_id}")
            for stop_time_update in entity.trip_update.stop_time_update[:2]:
                print(f"    - Stop Update: {stop_time_update.stop_id} (Delay: {stop_time_update.arrival.delay}s)")
        
        if entity.HasField('vehicle'):
            print(f"  Type: VehiclePosition")
            print(f"  Trip ID: {entity.vehicle.trip.trip_id}")
            print(f"  Position: Lat {entity.vehicle.position.latitude}, Lon {entity.vehicle.position.longitude}")
            print(f"  Stop ID: {entity.vehicle.stop_id}")

if __name__ == "__main__":
    parse_pb()
