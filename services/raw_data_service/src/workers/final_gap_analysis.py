import pandas as pd
import numpy as np

def analyze_final_gaps():
    print("Loading Registry v2 for final gap analysis...")
    df_reg = pd.read_csv("station_registry_v2.csv")
    
    # 1. Filter for the remaining Synthetic stops
    df_lonely = df_reg[df_reg['is_synthetic'] == True].copy()
    print(f"Analyzing the final {len(df_lonely)} synthetic stops...")

    # 2. Regional Analysis (State-level using Bounding Boxes approx)
    # Berlin: 52.3-52.7N, 13.0-13.8E
    # Munich: 48.0-48.3N, 11.3-11.8E
    # Hamburg: 53.3-53.8N, 9.7-10.3E
    
    regions = {
        'Berlin/Brandenburg': ((52.3, 53.0), (13.0, 14.5)),
        'Munich/Bavaria': ((47.5, 49.0), (10.0, 13.0)),
        'Hamburg/North': ((53.3, 54.5), (9.0, 11.0)),
        'Cologne/NRW': ((50.5, 52.0), (6.0, 8.0)),
    }

    print("\n--- Geographical Distribution ---")
    for name, bounds in regions.items():
        (lat_min, lat_max), (lon_min, lon_max) = bounds
        count = len(df_lonely[
            (df_lonely['lat'] >= lat_min) & (df_lonely['lat'] <= lat_max) &
            (df_lonely['lon'] >= lon_min) & (df_lonely['lon'] <= lon_max)
        ])
        print(f" - {name}: {count} stops ({count/len(df_lonely)*100:.2f}%)")

    # 3. Coordinate outliers (Border regions or outside Germany)
    outside = df_lonely[
        (df_lonely['lat'] < 47.0) | (df_lonely['lat'] > 55.0) |
        (df_lonely['lon'] < 5.0) | (df_lonely['lon'] > 16.0)
    ]
    print(f"\nStops technically outside Germany: {len(outside)}")

    # 4. Keyword Analysis (Are they special types?)
    keywords = {
        'Test/Admin': r'test|admin|internal|dummy',
        'Construction/Temp': r'baustelle|ersatz|temp|provisorisch|bau',
        'Private/Company': r'werk|firma|gmbh|ag|privat',
        'Platform info': r'gleis|steig|bstg|bahnsteig|pl\b',
    }
    
    print("\n--- Keyword Analysis ---")
    for key, pattern in keywords.items():
        count = len(df_lonely[df_lonely['name'].str.contains(pattern, case=False, na=False, regex=True)])
        print(f" - {key}: {count} stops ({count/len(df_lonely)*100:.2f}%)")

    # 5. Raw Sample for visual inspection
    print("\n--- Final Gap Samples (Visual Inspection) ---")
    sample = df_lonely.sample(min(len(df_lonely), 30))
    for _, row in sample.iterrows():
        print(f" - [{row['gtfs_de_id']}] {row['name']} ({row['lat']:.4f}, {row['lon']:.4f})")

if __name__ == "__main__":
    analyze_final_gaps()
