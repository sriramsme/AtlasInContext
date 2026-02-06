import fetch_gdelt
import calculate_vibe
import export_geojson
import sys
import gzip
import shutil
from pathlib import Path
from utils import DATA_DIR

def compress_file(file_path: Path):
    """Compress a file using gzip"""
    with open(file_path, 'rb') as f_in:
        with gzip.open(f"{file_path}.gz", 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"   📦 Compressed {file_path.name} -> {file_path.name}.gz")

def main():
    print("🌍 STARTING SIGNAL ATLAS INGESTION PIPELINE")
    print("="*60)
    
    # 1. Fetch Data
    print("\n[STEP 1/3] Fetching GDELT Data...")
    import os
    max_events = os.getenv("MAX_EVENTS")
    max_events = int(max_events) if max_events and max_events.isdigit() else None
    
    try:
        fetch_gdelt.run(num_of_blocks=32, max_events=max_events)
    except Exception as e:
        print(f"❌ Step 1 failed: {e}")
        sys.exit(1)
        
    # 2. Calculate Vibe
    print("\n[STEP 2/3] Calculating Vibe Scores...")
    try:
        calculate_vibe.run()
    except Exception as e:
        print(f"❌ Step 2 failed: {e}")
        sys.exit(1)
        
    # 3. Export & Cleanup
    print("\n[STEP 3/3] Exporting and Cleaning Up...")
    try:
        export_geojson.run()
    except Exception as e:
        print(f"❌ Step 3 failed: {e}")
        sys.exit(1)

    # 4. Cleanup intermediate files
    print("\n🧹 Cleaning up intermediate files...")
    data_dir = DATA_DIR
    files_to_clean = [
        data_dir / "gdelt_events.json",
        data_dir / "h3_cells.json"
    ]
    
    for f in files_to_clean:
        if f.exists():
            try:
                f.unlink()
                print(f"   ✅ Deleted {f.name}")
            except Exception as e:
                print(f"   ❌ Failed to delete {f.name}: {e}")
        else:
            print(f"   ⚠️  {f.name} not found")
            
    # 5. Compress remaining files and cleanup originals
    print("\n📦 Compressing final data files and cleaning up originals...")
    for f in data_dir.glob("*.json"):
        if f.suffix == ".json":
            compress_file(f)
            f.unlink()
            print(f"   🗑️ Removed original {f.name}")
        
    print("\n✅ PIPELINE COMPLETE")

if __name__ == "__main__":
    main()
