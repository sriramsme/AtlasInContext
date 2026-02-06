import requests
import zipfile
import io
import h3
import pandas as pd
from datetime import datetime
import json
from pathlib import Path
from typing import List, Dict, Optional
from schema.models import RawEvent
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from utils import DATA_DIR

# High-impact themes for Progress (Green)
PROGRESS_WEIGHTS = {
    'WB_AID_AND_DEVELOPMENT': 2.0,
    'WB_HEALTH_NUTRITION_AND_POPULATION': 2.0,
    'SOC_POINTSOF_LIGHT': 1.5,
    'ECON_DEVELOPMENT': 1.5,
    'PEACEKEEPING': 1.5,
    'ENV_GREEN': 1.2
}

# High-impact themes for Noise (Red)
NOISE_WEIGHTS = {
    'TAX_FNCACT_PROTEST': 1.5,
    'KILL': 2.0,
    'REBEL': 1.5,
    'TORTURE': 2.5,
    'TERROR': 2.5,
    'ARMEDCONFL': 2.0
}

# H3 Resolution (lower = fewer, larger hexagons)
H3_RESOLUTION = 4

# Configuration
MAX_RETRIES = 3
RETRY_BACKOFF = 2
DOWNLOAD_TIMEOUT = 60
BATCH_SAVE_SIZE = 1000


def create_robust_session() -> requests.Session:
    """Create a requests session with retry logic and connection pooling"""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session


def get_latest_gkg_url(num_of_blocks: int = 16, session: Optional[requests.Session] = None) -> List[str]:
    """Fetch the latest GKG 2.0 file URLs from the master list"""
    if session is None:
        session = create_robust_session()
    
    master_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"📡 Fetching master list (attempt {attempt + 1}/{MAX_RETRIES})...")
            response = session.get(master_url, timeout=30)
            response.raise_for_status()
            
            lines = response.text.strip().split('\n')
            gkg_files = [line.split()[2] for line in lines if '.gkg.csv.zip' in line]
            
            if not gkg_files:
                print("⚠️  No GKG files found in master list")
                return []
            
            print(f"✅ Found {len(gkg_files)} total GKG files, selecting last {num_of_blocks}")
            return gkg_files[-num_of_blocks:]
            
        except Exception as e:
            print(f"❌ Attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_BACKOFF ** (attempt + 1)
                print(f"⏳ Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            else:
                print(f"❌ All attempts failed for master list")
                return []
    return []

def parse_themes(themes_str: Optional[str]) -> set:
    """Parse V2Themes field: THEME;THEME;THEME;"""
    if not themes_str:
        return set()
    return {theme.strip() for theme in themes_str.split(';') if theme.strip()}


def parse_organizations(orgs_str: Optional[str]) -> set:
    """Parse V2Organizations field: org,CHAROFFSET;org,CHAROFFSET;"""
    if not orgs_str:
        return set()
    orgs = set()
    for item in orgs_str.split(';'):
        if item.strip():
            # Split on comma and take first part (ignore offset)
            parts = item.split(',')
            if parts:
                orgs.add(parts[0].strip())
    return orgs


def parse_tone(tone_str: Optional[str]) -> tuple[float, float]:
    """
    Parse V2Tone field: AvgTone,PosScore,NegScore,Polarity,ActivityDensity,SelfDensity,WordCount
    Returns: (avg_tone, polarity)
    """
    if not tone_str or not tone_str.strip():
        return (0.0, 0.0)
    
    try:
        parts = tone_str.split(',')
        if len(parts) >= 4:
            avg_tone = float(parts[0])
            polarity = float(parts[3])
            return (avg_tone, polarity)
    except (ValueError, IndexError):
        pass
    
    return (0.0, 0.0)


def parse_location(location_str: Optional[str]) -> Optional[tuple]:
    """
    Parse V2Locations field: TYPE#NAME#COUNTRY#ADM1#LAT#LON#FEATUREID;
    Returns: (lat, lng, name) or None
    """
    if not location_str or not location_str.strip():
        return None
    
    # Take first location (primary)
    locations = location_str.split(';')
    if not locations or not locations[0]:
        return None
    
    parts = locations[0].split('#')
    if len(parts) < 6:
        return None
    
    try:
        lat = float(parts[4])
        lng = float(parts[5])
        name = parts[1]
        
        # Validate coordinates
        if not (-90 <= lat <= 90 and -180 <= lng <= 180):
            return None
        
        return (lat, lng, name)
    except (ValueError, IndexError):
        return None


def extract_headline(xml_str: Optional[str]) -> str:
    """Extract PAGE_TITLE from ExtrasXML field"""
    if not xml_str or "<PAGE_TITLE>" not in xml_str:
        return ""
    
    try:
        start = xml_str.find("<PAGE_TITLE>") + len("<PAGE_TITLE>")
        end = xml_str.find("</PAGE_TITLE>")
        if start > 0 and end > start:
            return xml_str[start:end].strip()
    except:
        pass
    
    return ""


def parse_gkg_record(fields: List[str]) -> Optional[RawEvent]:
    """
    Parse a single GKG 2.1 record into RawEvent
    
    GKG 2.1 has 27 fields (0-26):
    [0] GKGRECORDID
    [1] DATE
    [2] SourceCollectionIdentifier
    [3] SourceCommonName
    [4] DocumentIdentifier (URL)
    [7] V2Themes
    [9] V2Locations
    [12] V2Organizations
    [14] V2Tone
    [17] SharingImage
    [26] ExtrasXML
    """
    
    # Validate minimum field count
    if len(fields) < 27:
        return None
    
    # 1. Document URL (required)
    doc_url = fields[4].strip() if fields[4] else None
    if not doc_url:
        return None
    
    # 2. Themes
    themes = parse_themes(fields[7])
    
    # 3. Organizations
    orgs = parse_organizations(fields[12])
    
    # 4. Categorization
    p_score = sum(PROGRESS_WEIGHTS.get(t, 0) for t in themes)
    n_score = sum(NOISE_WEIGHTS.get(t, 0) for t in themes)
    
    if p_score > n_score:
        category = "progress"
    elif n_score > p_score:
        category = "noise"
    else:
        category = "neutral"
    
    # 5. Tone
    avg_tone, polarity = parse_tone(fields[14])
    
    # 6. Location (required)
    location_data = parse_location(fields[9])
    if not location_data:
        return None
    
    lat, lng, location_name = location_data
    
    # 7. H3 Index
    try:
        h3_index = h3.latlng_to_cell(lat, lng, H3_RESOLUTION)
    except Exception:
        return None
    
    # 8. Headline
    headline = extract_headline(fields[26])
    
    
    # Create RawEvent
    try:
        return RawEvent(
            id=doc_url,
            category=category,
            source_type='gdelt',
            headline=headline,
            tone=avg_tone,
            polarity=polarity,
            p_weight=p_score,
            n_weight=n_score,
            lat=lat,
            lng=lng,
            h3_index=h3_index,
            location_name=location_name,
            timestamp=datetime.now()
        )
    except Exception as e:
        return None


def parse_gkg_file(file_content: io.BytesIO, filename: str, max_events: Optional[int] = None) -> List[RawEvent]:
    """Parse a single GKG CSV file from zip content"""
    events: List[RawEvent] = []
    
    # Debug counters
    total_lines = 0
    too_few_fields = 0
    no_url = 0
    no_location = 0
    parse_errors = 0
    
    try:
        with zipfile.ZipFile(file_content) as z:
            csv_filename = z.namelist()[0]
            print(f"   📦 Parsing: {csv_filename}")
            
            with z.open(csv_filename) as f:
                for line_num, line in enumerate(f, 1):
                    total_lines += 1
                    
                    try:
                        # Tab-delimited
                        fields = line.decode('utf-8', errors='ignore').split('\t')
                        
                        # Track filtering reasons
                        if len(fields) < 27:
                            too_few_fields += 1
                            continue
                        
                        if not fields[4] or not fields[4].strip():
                            no_url += 1
                            continue
                        
                        # Try to parse
                        event = parse_gkg_record(fields)
                        
                        if event is None:
                            # Most likely filtered due to no location
                            if not fields[9] or not fields[9].strip():
                                no_location += 1
                            else:
                                parse_errors += 1
                            continue
                        
                        events.append(event)
                        
                        # Max events check
                        if max_events and len(events) >= max_events:
                            print(f"   ⚠️  Reached max events ({max_events}), stopping parse")
                            break
                            
                    except Exception as e:
                        parse_errors += 1
                        continue
                        
    except zipfile.BadZipFile as e:
        print(f"   ❌ Invalid ZIP file: {e}")
        raise
    except Exception as e:
        print(f"   ❌ Unexpected parsing error: {e}")
        raise
    
    # Print statistics
    print(f"   📊 Parse Statistics:")
    print(f"      Total lines: {total_lines}")
    print(f"      Too few fields (<27): {too_few_fields}")
    print(f"      No URL: {no_url}")
    print(f"      No location: {no_location}")
    print(f"      Parse errors: {parse_errors}")
    print(f"   ✅ Extracted {len(events)} valid events")
    
    return events


def fetch_single_file(url: str, session: requests.Session, max_events: Optional[int] = None) -> Optional[List[RawEvent]]:
    """Download and parse a single GKG file with retry logic"""
    filename = url.split('/')[-1]
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"📥 Downloading: {filename} (attempt {attempt + 1}/{MAX_RETRIES})")
            
            response = session.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT)
            response.raise_for_status()
            
            file_content = io.BytesIO(response.content)
            events = parse_gkg_file(file_content, filename, max_events)
            return events
            
        except requests.exceptions.Timeout:
            print(f"   ⏱️  Timeout on attempt {attempt + 1}")
        except requests.exceptions.ConnectionError as e:
            print(f"   🔌 Connection error on attempt {attempt + 1}: {e}")
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Request error on attempt {attempt + 1}: {e}")
        except Exception as e:
            print(f"   ❌ Parsing error on attempt {attempt + 1}: {e}")
        
        if attempt < MAX_RETRIES - 1:
            wait_time = RETRY_BACKOFF ** (attempt + 1)
            print(f"   ⏳ Waiting {wait_time}s before retry...")
            time.sleep(wait_time)
    
    print(f"   ❌ Failed to download {filename} after {MAX_RETRIES} attempts")
    return None


def fetch_gkg_streaming(max_events: Optional[int] = None, num_of_blocks: int = 16) -> List[RawEvent]:
    """Download, unzip, and parse GKG data for AIC with robust error handling"""
    session = create_robust_session()
    
    urls = get_latest_gkg_url(num_of_blocks, session)
    if not urls:
        print("❌ Could not fetch GKG URLs")
        return []
    
    print(f"\n🎯 Will attempt to download {len(urls)} files")
    
    all_events: List[RawEvent] = []
    successful_downloads = 0
    failed_downloads = 0
    
    for i, url in enumerate(urls, 1):
        print(f"\n{'='*60}")
        print(f"File {i}/{len(urls)}")
        print(f"{'='*60}")
        
        remaining_events = None
        if max_events:
            remaining_events = max_events - len(all_events)
            if remaining_events <= 0:
                print(f"✅ Reached global max_events limit ({max_events})")
                break
        
        events = fetch_single_file(url, session, remaining_events)
        
        if events is not None:
            all_events.extend(events)
            successful_downloads += 1
            print(f"✅ Total events so far: {len(all_events)}")
        else:
            failed_downloads += 1
            print(f"⚠️  Skipping this file, continuing with others...")
        
        # Auto-save progress
        if len(all_events) > 0 and len(all_events) % BATCH_SAVE_SIZE == 0:
            print(f"\n💾 Auto-saving progress ({len(all_events)} events)...")
            save_raw_events(all_events, suffix="_partial")
    
    print(f"\n{'='*60}")
    print(f"📊 Download Summary:")
    print(f"   ✅ Successful: {successful_downloads}/{len(urls)}")
    print(f"   ❌ Failed: {failed_downloads}/{len(urls)}")
    print(f"   📦 Total events: {len(all_events)}")
    print(f"{'='*60}")
    
    return all_events


def save_raw_events(events: List[RawEvent], suffix: str = "") -> None:
    """Save raw events to JSON"""
    output_dir = DATA_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / f"gdelt_events{suffix}.json"
    
    # Convert events to dict
    events_dict = [e.model_dump(mode='json') if hasattr(e, 'model_dump') else e for e in events]
    
    output_data = {
        "generated_at": datetime.now().isoformat(),
        "source": "GDELT GKG 2.1",
        "h3_resolution": H3_RESOLUTION,
        "total_events": len(events),
        "events": events_dict
    }
    
    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2)
    
    print(f"💾 Saved {len(events)} events to {output_file}")
    print(f"📦 File size: {output_file.stat().st_size / (1024*1024):.2f} MB")


def run(num_of_blocks: int = 3, max_events: Optional[int] = None):
    print("🚀 AIC GDELT Ingestion Pipeline (PRODUCTION)")
    print("=" * 60)
    
    start_time = time.time()
    
    try:
        data = fetch_gkg_streaming(max_events=max_events, num_of_blocks=num_of_blocks)
        
        if data:
            print(f"\n✅ Successfully extracted {len(data)} events")
            
            save_raw_events(data)
            
            # Statistics
            print(f"\n📊 Category Distribution:")
            categories = pd.Series([e.category for e in data]).value_counts()
            print(categories)
            
            avg_tone = sum(e.tone for e in data) / len(data)
            print(f"\n🎭 Average Tone: {avg_tone:.2f}")
            
            unique_cells = len(set(e.h3_index for e in data))
            print(f"🗺️  Unique H3 Cells: {unique_cells}")
            print(f"📍 Events per Cell: {len(data) / unique_cells:.1f}")
            
            # Sample
            sample = data[0]
            print(f"\n📋 Sample Event:")
            print(f"   URL: {sample.id[:80]}...")
            print(f"   Headline: {sample.headline[:80] if sample.headline else 'N/A'}...")
            print(f"   Category: {sample.category}")
            print(f"   Tone: {sample.tone:.2f}")
            print(f"   Location: {sample.location_name}")
            
        else:
            print("❌ No data extracted.")
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        if 'all_events' in locals() and all_events:  # ty:ignore[unresolved-reference]
            save_raw_events(all_events, suffix="_interrupted")  # ty:ignore[unresolved-reference]
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    
    elapsed = time.time() - start_time
    print(f"\n⏱️  Total time: {elapsed:.1f}s")

if __name__ == "__main__":
    run()