# ingest/src/fetch_owid.py
import pandas as pd
import requests
import json
from pathlib import Path
from datetime import datetime

# OWID datasets (they're all on GitHub as CSVs)
OWID_DATASETS = {
    'life_expectancy': 'https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/Life%20expectancy%20at%20birth%20(historical)/Life%20expectancy%20at%20birth%20(historical).csv',
    'child_mortality': 'https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/Child%20mortality/Child%20mortality.csv',
    'literacy': 'https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/Literacy%20rates/Literacy%20rates.csv',
    'extreme_poverty': 'https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/Extreme%20poverty/Extreme%20poverty.csv'
}

# Country centroids (ISO3 code -> lat/lng)
COUNTRY_CENTROIDS = {
    'USA': (37.0902, -95.7129),
    'GBR': (55.3781, -3.4360),
    'FRA': (46.2276, 2.2137),
    'DEU': (51.1657, 10.4515),
    'CHN': (35.8617, 104.1954),
    'IND': (20.5937, 78.9629),
    'BRA': (-14.2350, -51.9253),
    'AUS': (-25.2744, 133.7751),
    'RUS': (61.5240, 105.3188),
    'JPN': (36.2048, 138.2529),
    'CAN': (56.1304, -106.3468),
    'MEX': (23.6345, -102.5528),
    'ZAF': (-30.5595, 22.9375),
    'NGA': (9.0820, 8.6753),
    'EGY': (26.8206, 30.8025),
    # Add more as needed, or use a complete ISO3 centroid dataset
}

def fetch_owid_indicator(name, url):
    """Fetch a single OWID dataset"""
    print(f"📥 Fetching {name}...")
    
    try:
        df = pd.read_csv(url)
        
        # OWID format: Entity, Year, [Indicator columns]
        # Get the most recent year for each country
        latest = df.sort_values('Year', ascending=False).groupby('Entity').first().reset_index()
        
        print(f"   ✅ Got data for {len(latest)} countries (year range: {df['Year'].min()}-{df['Year'].max()})")
        return latest
        
    except Exception as e:
        print(f"   ❌ Failed to fetch {name}: {e}")
        return None

def normalize_to_100(value, min_val, max_val):
    """Normalize a value to 0-100 scale (higher is better)"""
    if pd.isna(value):
        return 50.0  # Neutral for missing data
    
    normalized = ((value - min_val) / (max_val - min_val)) * 100
    return max(0, min(100, normalized))

def calculate_progress_score(row):
    """
    Calculate a composite progress score from multiple indicators
    Higher score = more progress
    """
    scores = []
    
    # Life expectancy (30-85 years range)
    if 'life_expectancy' in row and not pd.isna(row['life_expectancy']):
        scores.append(normalize_to_100(row['life_expectancy'], 30, 85))
    
    # Child mortality (200-0 per 1000 range, INVERTED: lower is better)
    if 'child_mortality' in row and not pd.isna(row['child_mortality']):
        scores.append(100 - normalize_to_100(row['child_mortality'], 0, 200))
    
    # Literacy (0-100% range)
    if 'literacy' in row and not pd.isna(row['literacy']):
        scores.append(row['literacy'])
    
    # Extreme poverty (50-0% range, INVERTED: lower is better)
    if 'extreme_poverty' in row and not pd.isna(row['extreme_poverty']):
        scores.append(100 - normalize_to_100(row['extreme_poverty'], 0, 50))
    
    # Return average of available indicators
    return sum(scores) / len(scores) if scores else 50.0

def fetch_owid_data():
    """Fetch all OWID datasets and combine them"""
    all_data = {}
    
    for name, url in OWID_DATASETS.items():
        df = fetch_owid_indicator(name, url)
        if df is not None:
            all_data[name] = df
    
    if not all_data:
        print("❌ No OWID data fetched!")
        return []
    
    # Merge all indicators on Entity (country name)
    base_df = all_data['life_expectancy'][['Entity', 'Year']].copy()
    base_df.rename(columns={'Year': 'latest_year'}, inplace=True)
    
    for name, df in all_data.items():
        # Get the indicator column (usually the 3rd column after Entity and Year)
        indicator_col = df.columns[2]
        merge_df = df[['Entity', indicator_col]].copy()
        merge_df.rename(columns={indicator_col: name}, inplace=True)
        base_df = base_df.merge(merge_df, on='Entity', how='left')
    
    # Calculate progress score
    base_df['progress_score'] = base_df.apply(calculate_progress_score, axis=1)
    
    # Add country codes and centroids
    # For now, use a simple mapping (in production, use a proper ISO3 lookup)
    base_df['iso3'] = base_df['Entity'].map({
        'United States': 'USA',
        'United Kingdom': 'GBR',
        'France': 'FRA',
        'Germany': 'DEU',
        'China': 'CHN',
        'India': 'IND',
        'Brazil': 'BRA',
        'Australia': 'AUS',
        'Russia': 'RUS',
        'Japan': 'JPN',
        'Canada': 'CAN',
        'Mexico': 'MEX',
        'South Africa': 'ZAF',
        'Nigeria': 'NGA',
        'Egypt': 'EGY',
        # Add more mappings as needed
    })
    
    # Add centroids
    base_df['lat'] = base_df['iso3'].map(lambda x: COUNTRY_CENTROIDS.get(x, (None, None))[0] if pd.notna(x) else None)
    base_df['lng'] = base_df['iso3'].map(lambda x: COUNTRY_CENTROIDS.get(x, (None, None))[1] if pd.notna(x) else None)
    
    # Filter to only countries with coordinates
    base_df = base_df[base_df['lat'].notna()].copy()
    
    # Convert to list of dicts
    progress_data = base_df.to_dict('records')
    
    # Save to JSON
    output_file = Path("../data/owid_progress.json")
    with open(output_file, "w") as f:
        json.dump(progress_data, f, indent=2)
    
    print(f"\n✅ Saved progress data for {len(progress_data)} countries to {output_file}")
    print(f"📊 Average global progress score: {base_df['progress_score'].mean():.1f}/100")
    
    return progress_data

if __name__ == "__main__":
    data = fetch_owid_data()
    
    if data:
        print("\nExample country:")
        print(json.dumps(data[0], indent=2))