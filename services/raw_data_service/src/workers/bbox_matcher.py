import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
import time

# Berlin Bounding Box
BBOX = {
    'min_lat': 52.33, 'max_lat': 52.68,
    'min_lon': 13.08, 'max_lon': 13.76
}

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # km
    return c * r * 1000 # meters

def normalize_name(name):
    if not isinstance(name, str): return ""
    return "".join(e for e in name.lower() if e.isalnum())

def run():
    print("Loading datasets and filtering by BBox...")
    # Read the first 200,000 stops from both to get a large overlap in Berlin
    df_delfi_all = pd.read_csv("/tmp/stops_delfi.txt", nrows=200000)
    df_gtfs_de_all = pd.read_csv("/tmp/stops_gtfs_de.txt", nrows=200000)

    # Filter by BBox
    df_d = df_delfi_all[
        (df_delfi_all['stop_lat'] >= BBOX['min_lat']) & (df_delfi_all['stop_lat'] <= BBOX['max_lat']) &
        (df_delfi_all['stop_lon'] >= BBOX['min_lon']) & (df_delfi_all['stop_lon'] <= BBOX['max_lon'])
    ]
    
    df_g = df_gtfs_de_all[
        (df_gtfs_de_all['stop_lat'] >= BBOX['min_lat']) & (df_gtfs_de_all['stop_lat'] <= BBOX['max_lat']) &
        (df_gtfs_de_all['stop_lon'] >= BBOX['min_lon']) & (df_gtfs_de_all['stop_lon'] <= BBOX['max_lon'])
    ]

    print(f"DELFI stops in BBox: {len(df_d)}")
    print(f"GTFS.DE stops in BBox: {len(df_g)}")
    
    if len(df_d) == 0 or len(df_g) == 0:
        print("Error: No stops found in BBox. Try a different range or bounding box.")
        return

    matches = 0
    start_time = time.time()

    for idx, d_row in df_d.iterrows():
        d_lat, d_lon = d_row['stop_lat'], d_row['stop_lon']
        d_name_norm = normalize_name(d_row['stop_name'])
        
        nearby = df_g[
            (df_g['stop_lat'] > d_lat - 0.0003) & (df_g['stop_lat'] < d_lat + 0.0003) &
            (df_g['stop_lon'] > d_lon - 0.0005) & (df_g['stop_lon'] < d_lon + 0.0005)
        ]
        
        for _, g_row in nearby.iterrows():
            dist = haversine(d_lon, d_lat, g_row['stop_lon'], g_row['stop_lat'])
            if dist < 30: # 30 meters
                g_name_norm = normalize_name(g_row['stop_name'])
                # Fuzzy match: name overlap
                if d_name_norm == g_name_norm or d_name_norm in g_name_norm or g_name_norm in d_name_norm:
                    matches += 1
                    break
    
    print(f"\n--- Bounding Box Match Results ---")
    print(f"Matches: {matches} / {len(df_d)}")
    print(f"Success Rate: {(matches/len(df_d))*100:.2f}%")
    print(f"Time: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    run()
