import pandas as pd
from rapidfuzz import fuzz

def audit_registry():
    print("Loading Registry for Quality Audit...")
    df_reg = pd.read_csv("station_registry_v1.csv")
    df_d = pd.read_csv("/tmp/stops_delfi.txt")
    
    # 1. Sample High-Quality Matches (Plausibility Check)
    print("\n--- [Audit] High-Score Matches (Should be identical) ---")
    high_score = df_reg[df_reg['match_score'] > 98].sample(10)
    for _, row in high_score.iterrows():
        print(f"Match: {row['name']} (Score: {row['match_score']})")

    # 2. Sample Borderline Matches (Risk Check)
    print("\n--- [Audit] Borderline Matches (Risk of False Positives) ---")
    borderline = df_reg[(df_reg['match_score'] > 85) & (df_reg['match_score'] < 90)].sample(10)
    for _, row in borderline.iterrows():
        # Get the original DELFI name for comparison
        d_name = df_d[df_d['stop_id'] == row['delfi_id']]['stop_name'].values[0]
        print(f"GTFS.DE: {row['name']} <-> DELFI: {d_name} (Score: {row['match_score']})")

    # 3. Analyze Synthetic (The Unmatched 9%)
    print("\n--- [Audit] The Unmatched 'Lonely' 9% ---")
    synthetic = df_reg[df_reg['is_synthetic'] == True].sample(20)
    print("Sample of names we missed:")
    for _, row in synthetic.iterrows():
        print(f" - {row['name']} (Lat: {row['lat']:.4f}, Lon: {row['lon']:.4f})")

if __name__ == "__main__":
    audit_registry()
