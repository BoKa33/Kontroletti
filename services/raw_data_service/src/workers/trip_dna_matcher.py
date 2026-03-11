import pandas as pd
import time

def match_trip_dna():
    print("Loading Registry v2 and Sample Data...")
    df_reg = pd.read_csv("station_registry_v2.csv")
    df_st_g = pd.read_csv("/tmp/stop_times_gtfs_de.txt")
    df_st_d = pd.read_csv("/tmp/stop_times_delfi.txt")
    
    # 1. Pick a random Trip from GTFS.DE
    # Let's find a trip that has at least 5 stops to have a meaningful DNA
    trip_counts = df_st_g['trip_id'].value_counts()
    test_trip_id = trip_counts[trip_counts > 5].index[20]
    print(f"\nTarget Trip (GTFS.DE): {test_trip_id}")
    
    # 2. Get the "DNA"
    dna_g = df_st_g[df_st_g['trip_id'] == test_trip_id].sort_values('stop_sequence')
    dna_g = dna_g.merge(df_reg[['gtfs_de_id', 'canonical_id']], left_on='stop_id', right_on='gtfs_de_id', how='left')
    
    # Filter for stops that have a canonical mapping
    mapped_dna = dna_g.dropna(subset=['canonical_id'])
    print(f"DNA length: {len(dna_g)} stops ({len(mapped_dna)} mapped to Canonical)")
    
    if len(mapped_dna) < 3:
        print("Error: Too few mapped stops to match DNA.")
        return

    # 3. Search for this DNA in DELFI (Universal Search)
    print("\nSearching for DNA 'Twins' in DELFI...")
    start_time = time.time()
    
    # Merge DELFI stop times with Registry
    df_st_d = df_st_d.merge(df_reg[['delfi_id', 'canonical_id']].dropna(), 
                            left_on='stop_id', right_on='delfi_id', how='left')
    
    # Find ALL trips that visit ANY of our mapped stops at the SAME time
    # This is much more robust than just the first stop
    potential_matches = df_st_d[
        (df_st_d['canonical_id'].isin(mapped_dna['canonical_id'])) &
        (df_st_d['departure_time'].isin(mapped_dna['departure_time']))
    ]
    
    potential_twins = potential_matches['trip_id'].value_counts()
    print(f"Found {len(potential_twins)} trips with at least one overlapping stop/time.")
    
    # Filter for those with high overlap
    top_twins = potential_twins[potential_twins >= 3].index.tolist()
    
    if not top_twins:
        print("No high-overlap twins found.")
        return

    print(f"\n--- [DNA MATCH SUCCESS] ---")
    for twin_id in top_twins[:5]: # Show top 5
        overlap_count = potential_twins[twin_id]
        print(f"Twin Found: {twin_id} ({overlap_count} overlapping stops)")
        
        # Verify the actual stop sequence
        twin_dna = df_st_d[df_st_d['trip_id'] == twin_id].sort_values('stop_sequence')
        print(f"  Sample stops: {twin_dna['canonical_id'].tolist()[:3]}...")

    print(f"\nTime Taken: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    match_trip_dna()
