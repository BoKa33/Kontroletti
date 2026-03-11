import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
import time

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r * 1000 # Return in meters

def normalize_name(name):
    """Simple normalization: lowercase, remove special chars"""
    if not isinstance(name, str): return ""
    return "".join(e for e in name.lower() if e.isalnum())

def run_experiment():
    print("Loading datasets...")
    # Read Berlin-specific stops for a better geographical overlap
    df_delfi = pd.read_csv("/tmp/berlin_delfi.txt", names=["stop_id","stop_code","stop_name","stop_desc","stop_lat","stop_lon","location_type","parent_station","wheelchair_boarding","platform_code","level_id"])
    df_gtfs_de = pd.read_csv("/tmp/berlin_gtfs_de.txt", names=["stop_name","parent_station","stop_id","stop_lat","stop_lon","location_type","platform_code"])

    # DELFI Cols: stop_id, stop_name, stop_lat, stop_lon
    # GTFS.DE Cols: stop_name, stop_id, stop_lat, stop_lon
    
    print(f"Comparing {len(df_delfi)} DELFI stops against {len(df_gtfs_de)} GTFS.DE stops...")
    
    matches = 0
    start_time = time.time()

    for idx, d_row in df_delfi.iterrows():
        d_lat, d_lon = d_row['stop_lat'], d_row['stop_lon']
        d_name_norm = normalize_name(d_row['stop_name'])
        
        # 1. Broad spatial filter: Find all GTFS.DE stops within ~50 meters
        # (Approx 0.0005 degrees latitude is ~55 meters)
        nearby = df_gtfs_de[
            (df_gtfs_de['stop_lat'] > d_lat - 0.0005) & 
            (df_gtfs_de['stop_lat'] < d_lat + 0.0005) &
            (df_gtfs_de['stop_lon'] > d_lon - 0.0005) & 
            (df_gtfs_de['stop_lon'] < d_lon + 0.0005)
        ]
        
        for _, g_row in nearby.iterrows():
            # 2. Refined distance check
            dist = haversine(d_lon, d_lat, g_row['stop_lon'], g_row['stop_lat'])
            if dist < 25: # Within 25 meters
                # 3. Name check
                g_name_norm = normalize_name(g_row['stop_name'])
                if d_name_norm == g_name_norm or d_name_norm in g_name_norm or g_name_norm in d_name_norm:
                    matches += 1
                    # print(f"MATCH: {d_row['stop_name']} <-> {g_row['stop_name']} ({dist:.1f}m)")
                    break # Stop looking for this DELFI stop
    
    end_time = time.time()
    success_rate = (matches / len(df_delfi)) * 100
    print(f"\n--- Experiment Results ---")
    print(f"Time Taken: {end_time - start_time:.2f}s")
    print(f"Total DELFI Stops: {len(df_delfi)}")
    print(f"Matches Found: {matches}")
    print(f"Success Rate: {success_rate:.2f}%")

if __name__ == "__main__":
    run_experiment()
