import pandas as pd

def analyze_gaps():
    print("Loading datasets for gap analysis...")
    df_d = pd.read_csv("/tmp/stops_delfi.txt")
    df_g = pd.read_csv("/tmp/stops_gtfs_de.txt")
    df_bridge = pd.read_csv("stop_bridge.csv")

    # Find GTFS.DE IDs that are NOT in the bridge
    matched_ids = set(df_bridge['gtfs_de_id'])
    df_lonely = df_g[~df_g['stop_id'].isin(matched_ids)]

    print(f"\n--- Gap Analysis ---")
    print(f"Total GTFS.DE Stops: {len(df_g)}")
    print(f"Unmatched Stops: {len(df_lonely)} ({len(df_lonely)/len(df_g)*100:.2f}%)")

    # 1. Location Type Distribution
    print("\nLocation Type in Unmatched Stops (0=Stop, 1=Station):")
    print(df_lonely['location_type'].value_counts())

    # 2. Are they outside Germany? (Approx Bounding Box for Germany)
    # Lat: 47.2 to 55.1 | Lon: 5.8 to 15.1
    df_outside = df_lonely[
        (df_lonely['stop_lat'] < 47.2) | (df_lonely['stop_lat'] > 55.1) |
        (df_lonely['stop_lon'] < 5.8) | (df_lonely['stop_lon'] > 15.1)
    ]
    print(f"\nStops outside Germany BBox: {len(df_outside)} ({len(df_outside)/len(df_lonely)*100:.2f}% of unmatched)")

    # 3. Sample of unmatched stop names
    print("\nSample of Unmatched Stop Names:")
    print(df_lonely['stop_name'].head(20).tolist())

    # 4. Do they have a parent?
    print("\nUnmatched stops with a parent_station:")
    print(df_lonely['parent_station'].notna().value_counts())

if __name__ == "__main__":
    analyze_gaps()
