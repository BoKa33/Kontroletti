import pandas as pd
import numpy as np
import time
from zipfile import ZipFile

# Frankfurt Bounding Box
BBOX = {
    'min_lat': 50.0, 'max_lat': 50.2,
    'min_lon': 8.5, 'max_lon': 8.8
}

def match_geo_aligned():
    print("Loading Registry v2...")
    df_reg = pd.read_csv("station_registry_v2.csv")
    
    print("Filtering stops geographically (Frankfurt)...")
    df_d = pd.read_csv("/tmp/stops_delfi.txt")
    df_g = pd.read_csv("/tmp/stops_gtfs_de.txt")
    
    # Filter by BBox
    df_d_geo = df_d[(df_d['stop_lat'] >= BBOX['min_lat']) & (df_d['stop_lat'] <= BBOX['max_lat']) &
                    (df_d['stop_lon'] >= BBOX['min_lon']) & (df_d['stop_lon'] <= BBOX['max_lon'])]
    
    df_g_geo = df_g[(df_g['stop_lat'] >= BBOX['min_lat']) & (df_g['stop_lat'] <= BBOX['max_lat']) &
                    (df_g['stop_lon'] >= BBOX['min_lon']) & (df_g['stop_lon'] <= BBOX['max_lon'])]

    print(f"Frankfurt stops: DELFI={len(df_d_geo)}, GTFS.DE={len(df_g_geo)}")
    
    # 1. Load stop_times from GTFS.DE ZIP
    print("Loading 2M stop times from GTFS.DE ZIP...")
    with ZipFile("/home/tuxi/Downloads/latest.zip") as z:
        with z.open("stop_times.txt") as f:
            df_st_g = pd.read_csv(f, nrows=2000000, usecols=['trip_id', 'stop_id', 'departure_time', 'stop_sequence'])
    
    # 2. Translate these to Canonical
    df_st_g = df_st_g.merge(df_reg[['gtfs_de_id', 'canonical_id']], left_on='stop_id', right_on='gtfs_de_id', how='left')
    
    # 3. Pick a trip from GTFS.DE that actually stays in Frankfurt
    frankfurt_stops = set(df_g_geo['stop_id'])
    trips_in_ffm = df_st_g[df_st_g['stop_id'].isin(frankfurt_stops)]['trip_id'].unique()
    
    if len(trips_in_ffm) == 0:
        print("No trips in the first 2M lines visit Frankfurt. Try another sample.")
        return

    test_trip_id = trips_in_ffm[len(trips_in_ffm)//2] # Pick a middle one
    print(f"\nTarget Trip (GTFS.DE): {test_trip_id}")
    
    # 4. Get the DNA
    dna_g = df_st_g[df_st_g['trip_id'] == test_trip_id].sort_values('stop_sequence')
    mapped_dna = dna_g.dropna(subset=['canonical_id'])
    print(f"DNA length: {len(dna_g)} stops ({len(mapped_dna)} mapped to Canonical)")

    if len(mapped_dna) < 3:
        print("Error: Too few mapped stops to match DNA.")
        return

    # 5. Look for this DNA in DELFI (Full Dataset Search)
    # We'll use the FIRST mapped stop and its departure time
    search_stop_id = mapped_dna.iloc[0]['canonical_id']
    search_time = mapped_dna.iloc[0]['departure_time']
    
    print(f"Searching for Stop: {search_stop_id} at Time: {search_time} in DELFI archive...")
    
    import subprocess
    # Use grep -F for literal matching (much faster)
    cmd = f"unzip -p ~/Downloads/20260309_fahrplaene_gesamtdeutschland_gtfs.zip stop_times.txt | grep -F '{search_stop_id}' | grep -F '{search_time}'"
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if res.stdout:
        print("\n--- [DNA MATCH POTENTIAL SUCCESS] ---")
        print("Raw lines found in DELFI:")
        # Show first 5 lines
        for line in res.stdout.splitlines()[:5]:
            print(f"  {line}")
    else:
        print("\n--- [DNA MATCH FAILED] ---")
        print("No identical stop+time found in DELFI.")

if __name__ == "__main__":
    match_geo_aligned()
