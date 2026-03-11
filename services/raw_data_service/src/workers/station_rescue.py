import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
from rapidfuzz import fuzz
import re
import time

# --- Expanded Forensic Dictionary ---
FORENSIC_MAP = {
    r'\bf\b\.?\s+': 'frankfurt ',
    r'\bm\b\.?\s+': 'münchen ',
    r'\bb\b\.?\s+': 'berlin ',
    r'\bh\b\.?\s+': 'hamburg ',
    r'\bri\.\s+': 'richtung ',
    r'\bgh\b\.?\s+': 'gasthof ',
    r'\bmhl\b\.?\s+': 'mühlhausen ',
    r'\bstr\b\.?': 'straße',
    r'\bbhf\b\.?': 'bahnhof',
    r'\bpl\b\.?': 'platz',
    r'\ba\.-bebel\b': 'august-bebel',
    r'\bk\.-marx\b': 'karl-marx',
    r'\bg\.-hauptmann\b': 'gerhart-hauptmann',
    r'\bs\+u\b': '',
    r'\bu-bhf\b': '',
    r'\bs-bhf\b': '',
    r'\b(h)\b': '',
}

def forensic_clean(name):
    if not isinstance(name, str): return ""
    name = name.lower()
    for pattern, replacement in FORENSIC_MAP.items():
        name = re.sub(pattern, replacement, name)
    name = re.sub(r'\(.*?\)', '', name)
    name = re.sub(r'[^a-z0-9]', '', name)
    return name.strip()

def run_rescue():
    print("Loading Registry v1 and raw datasets...")
    df_reg = pd.read_csv("station_registry_v1.csv")
    df_d = pd.read_csv("/tmp/stops_delfi.txt")
    df_g = pd.read_csv("/tmp/stops_gtfs_de.txt")

    # 1. Identify "Lonely" stops
    df_lonely = df_reg[df_reg['is_synthetic'] == True].copy()
    print(f"Starting Rescue Mission for {len(df_lonely)} synthetic stops...")

    # 2. Build Spatial Index for DELFI
    df_d['clean_name'] = df_d['stop_name'].apply(forensic_clean)
    coords_d = df_d[['stop_lat', 'stop_lon']].values
    tree_d = cKDTree(coords_d)

    rescued = []
    log_samples = []
    
    start_time = time.time()
    
    # We only process the lonely ones
    for idx, row in df_lonely.iterrows():
        g_lat, g_lon = row['lat'], row['lon']
        g_name = row['name']
        g_clean = forensic_clean(g_name)
        
        # Wider search for rescue (150 meters)
        indices = tree_d.query_ball_point([g_lat, g_lon], 0.0015)
        
        best_match = None
        best_score = 0
        
        if indices:
            for d_idx in indices:
                d_row = df_d.iloc[d_idx]
                d_clean = d_row['clean_name']
                
                # Forensic Matching
                score = fuzz.token_set_ratio(g_clean, d_clean)
                
                # If they are VERY close (<20m), we accept a lower name score
                dist_deg = np.sqrt((g_lat-d_row['stop_lat'])**2 + (g_lon-d_row['stop_lon'])**2)
                
                if (score > 80) or (dist_deg < 0.0002 and score > 60):
                    if score > best_score:
                        best_score = score
                        best_match = d_row
            
            if best_match is not None:
                # SUCCESS: Rescue complete
                rescue_data = {
                    'canonical_id': best_match['stop_id'],
                    'gtfs_de_id': row['gtfs_de_id'],
                    'delfi_id': best_match['stop_id'],
                    'name': best_match['stop_name'],
                    'old_name': g_name,
                    'lat': best_match['stop_lat'],
                    'lon': best_match['stop_lon'],
                    'match_score': best_score,
                    'is_synthetic': False,
                    'rescue_method': 'forensic_spatial'
                }
                rescued.append(rescue_data)
                
                if len(log_samples) < 50:
                    log_samples.append(rescue_data)

    # 3. Merge rescued back into registry
    df_rescued = pd.DataFrame(rescued)
    
    # Update the original registry: Replace synthetic rows with rescued rows where possible
    # We use gtfs_de_id as the key
    df_reg_v2 = df_reg.set_index('gtfs_de_id')
    df_rescued_indexed = df_rescued.set_index('gtfs_de_id')
    
    # Update only the columns that changed
    df_reg_v2.update(df_rescued_indexed)
    df_reg_v2 = df_reg_v2.reset_index()

    # Save Registry v2
    df_reg_v2.to_csv("station_registry_v2.csv", index=False)
    
    # Save Rescue Log for verification
    pd.DataFrame(log_samples).to_csv("rescue_log.csv", index=False)

    print(f"\n--- Rescue Mission Results ---")
    print(f"Stops Rescued: {len(rescued)}")
    total = len(df_g)
    matched_total = len(df_reg_v2[df_reg_v2['is_synthetic'] == False])
    print(f"New Total Matched: {matched_total} ({matched_total/total*100:.2f}%)")
    print(f"Remaining Synthetic: {total - matched_total}")
    print(f"Time Taken: {time.time() - start_time:.2f}s")
    print("\nCheck rescue_log.csv for samples of new matches.")

if __name__ == "__main__":
    run_rescue()
