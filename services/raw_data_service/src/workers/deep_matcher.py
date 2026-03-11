import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
import re
import time

def normalize_name(name):
    if not isinstance(name, str): return ""
    # Remove things like "S+U", "U-Bhf", "(Berlin)", "Halt", etc.
    name = re.sub(r'\(.*?\)', '', name)
    name = re.sub(r'[SU]\+?[U]?-?Bhf', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+', ' ', name).strip()
    return "".join(e for e in name.lower() if e.isalnum())

def run_deep_match():
    print("Loading DELFI stops...")
    df_d = pd.read_csv("/tmp/stops_delfi.txt")
    print("Loading GTFS.DE stops...")
    df_g = pd.read_csv("/tmp/stops_gtfs_de.txt")

    print(f"DELFI: {len(df_d)} stops | GTFS.DE: {len(df_g)} stops")

    # Build KDTree for DELFI
    print("Building spatial index for DELFI...")
    coords_d = df_d[['stop_lat', 'stop_lon']].values
    tree_d = cKDTree(coords_d)

    print("Matching stops...")
    bridge = []
    start_time = time.time()
    
    # Pre-normalize DELFI names
    df_d['norm_name'] = df_d['stop_name'].apply(normalize_name)
    df_g['norm_name'] = df_g['stop_name'].apply(normalize_name)

    # To speed up, we'll process GTFS.DE stops
    for idx, g_row in df_g.iterrows():
        g_lat, g_lon = g_row['stop_lat'], g_row['stop_lon']
        g_name_norm = g_row['norm_name']
        
        # Find all DELFI stops within ~150 meters (roughly 0.0013 degrees)
        indices = tree_d.query_ball_point([g_lat, g_lon], 0.0015)
        
        if not indices:
            continue
            
        # Among nearby DELFI stops, find the best name match
        for d_idx in indices:
            d_row = df_d.iloc[d_idx]
            d_name_norm = d_row['norm_name']
            
            if g_name_norm == d_name_norm or g_name_norm in d_name_norm or d_name_norm in g_name_norm:
                bridge.append({
                    'gtfs_de_id': g_row['stop_id'],
                    'delfi_id': d_row['stop_id'],
                    'stop_name': g_row['stop_name'],
                    'dist_deg': np.sqrt((g_lat-d_row['stop_lat'])**2 + (g_lon-d_row['stop_lon'])**2)
                })
                break # Match found for this GTFS.DE stop
        
        if idx % 50000 == 0 and idx > 0:
            print(f"Processed {idx} stops... {len(bridge)} matches found so far.")

    df_bridge = pd.DataFrame(bridge)
    df_bridge.to_csv("stop_bridge.csv", index=False)
    
    print(f"\n--- Deep Match Results ---")
    print(f"Total Matches: {len(bridge)}")
    print(f"Success Rate: {(len(bridge)/len(df_g))*100:.2f}% of GTFS.DE stops mapped to DELFI.")
    print(f"Time Taken: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    run_deep_match()
