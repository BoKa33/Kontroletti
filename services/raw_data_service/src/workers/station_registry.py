import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
from rapidfuzz import fuzz
import re
import time
import hashlib

# --- Pre-processing: The "German Transit Dictionary" ---
TRANSIT_MAP = {
    r'\bstr\b\.?': 'straße',
    r'\bbhf\b\.?': 'bahnhof',
    r'\bpl\b\.?': 'platz',
    r'\ba\.-bebel\b': 'august-bebel',
    r'\bk\.-marx\b': 'karl-marx',
    r'\bg\.-hauptmann\b': 'gerhart-hauptmann',
    r'\bs\+u\b': '',
    r'\bu-bhf\b': '',
    r'\bs-bhf\b': '',
    r'\b(h)\b': '', # (H) for Halt
}

def clean_name(name):
    if not isinstance(name, str): return ""
    name = name.lower()
    for pattern, replacement in TRANSIT_MAP.items():
        name = re.sub(pattern, replacement, name)
    name = re.sub(r'\(.*?\)', '', name) # Remove (Berlin) etc.
    name = re.sub(r'[^a-z0-9]', '', name) # Only alphanumeric
    return name.strip()

def get_lat_lon_hash(lat, lon, name):
    """Generate a stable ID based on 10m precision coords + cleaned name"""
    # Round to 4 decimal places (~11 meters)
    lat_r, lon_r = round(lat, 4), round(lon, 4)
    key = f"{lat_r}_{lon_r}_{clean_name(name)}"
    return hashlib.md5(key.encode()).hexdigest()[:12]

def run_registry_gen():
    print("Loading datasets for Station Registry v1...")
    df_d = pd.read_csv("/tmp/stops_delfi.txt")
    df_g = pd.read_csv("/tmp/stops_gtfs_de.txt")
    
    # 1. Prepare DELFI (The Primary Skeleton)
    print("Preparing DELFI references...")
    df_d['clean_name'] = df_d['stop_name'].apply(clean_name)
    coords_d = df_d[['stop_lat', 'stop_lon']].values
    tree_d = cKDTree(coords_d)

    registry = []
    unmatched = []
    
    print(f"Matching {len(df_g)} GTFS.DE stops...")
    start_time = time.time()
    
    # Pre-normalize GTFS.DE names
    df_g['clean_name'] = df_g['stop_name'].apply(clean_name)

    for idx, g_row in df_g.iterrows():
        g_lat, g_lon = g_row['stop_lat'], g_row['stop_lon']
        g_clean = g_row['clean_name']
        
        # Spatial search (within 50 meters)
        indices = tree_d.query_ball_point([g_lat, g_lon], 0.0005)
        
        match_found = False
        if indices:
            for d_idx in indices:
                d_row = df_d.iloc[d_idx]
                # High-fidelity Fuzzy Match (Levenshtein)
                score = fuzz.ratio(g_clean, d_row['clean_name'])
                if score > 85 or g_clean in d_row['clean_name'] or d_row['clean_name'] in g_clean:
                    registry.append({
                        'canonical_id': d_row['stop_id'], # Use DELFI zHV as Canonical
                        'source': 'delfi',
                        'gtfs_de_id': g_row['stop_id'],
                        'delfi_id': d_row['stop_id'],
                        'name': d_row['stop_name'],
                        'lat': d_row['stop_lat'], 'lon': d_row['stop_lon'],
                        'match_score': score,
                        'is_synthetic': False
                    })
                    match_found = True
                    break
        
        if not match_found:
            # CREATE SYNTHETIC ID for unmatched GTFS.DE stops
            synth_id = f"synth:{get_lat_lon_hash(g_lat, g_lon, g_row['stop_name'])}"
            registry.append({
                'canonical_id': synth_id,
                'source': 'gtfs_de',
                'gtfs_de_id': g_row['stop_id'],
                'delfi_id': None,
                'name': g_row['stop_name'],
                'lat': g_lat, 'lon': g_lon,
                'match_score': 0,
                'is_synthetic': True
            })

        if idx % 100000 == 0 and idx > 0:
            print(f"Processed {idx} stops...")

    df_reg = pd.DataFrame(registry)
    df_reg.to_csv("station_registry_v1.csv", index=False)
    
    # Quality Report
    total = len(df_g)
    matched = df_reg[df_reg['is_synthetic'] == False]
    synthetic = df_reg[df_reg['is_synthetic'] == True]
    
    print(f"\n--- Station Registry v1 Report ---")
    print(f"Total Registry Entries: {len(df_reg)}")
    print(f"Successfully Matched: {len(matched)} ({len(matched)/total*100:.2f}%)")
    print(f"Synthetic (Unmatched): {len(synthetic)} ({len(synthetic)/total*100:.2f}%)")
    print(f"Time Taken: {time.time() - start_time:.2f}s")
    print("\nRegistry saved to station_registry_v1.csv")

if __name__ == "__main__":
    run_registry_gen()
