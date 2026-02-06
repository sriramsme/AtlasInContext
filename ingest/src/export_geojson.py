import json
import h3
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from schema.models import H3Cell, GlobalPulse, GlobalInsight, AggregateResult
from utils import DATA_DIR

def load_aggregated_cells() -> Tuple[List[H3Cell], Dict, Optional[GlobalPulse], List[GlobalInsight]]:
    """Load aggregated data including the new Pulse and Insights"""
    input_file = DATA_DIR / "h3_cells.json"
    
    if not input_file.exists():
        return [], {}, None, []
    
    with open(input_file) as f:
        data = json.load(f)
    
    cells_data = data.get('cells', [])
    cells = [H3Cell(**c) for c in cells_data]
    
    metadata = data.get('metadata', {})
    
    pulse_data = data.get('pulse', None)
    pulse = GlobalPulse(**pulse_data) if pulse_data else None
    
    insights_data = data.get('insights', [])
    insights = [GlobalInsight(**i) for i in insights_data]
    
    return cells, metadata, pulse, insights

def h3_to_geojson_polygon(h3_index: str) -> List[List[float]]:
    """Convert H3 cell to GeoJSON polygon coordinates"""
    boundary = h3.cell_to_boundary(h3_index)
    # GeoJSON format: [lng, lat] and close the polygon
    coords = [[lng, lat] for lat, lng in boundary]
    coords.append(coords[0])  # Close the polygon
    return coords

def export_core_grid(cells: List[H3Cell]) -> None:
    """
    Export H3 grid geometry ONLY (no data attributes)
    This file is loaded once and cached forever
    """
    print("📐 Generating core H3 grid geometry...")
    
    features = []
    for cell in cells:
        polygon_coords = h3_to_geojson_polygon(cell.h3_index)
        
        feature = {
            "type": "Feature",
            "id": cell.h3_index,  # ID for easy lookup
            "geometry": {
                "type": "Polygon",
                "coordinates": [polygon_coords]
            },
            "properties": {}  # Empty - just geometry
        }
        features.append(feature)
    
    # Get H3 resolution from first cell
    h3_resolution = h3.get_resolution(cells[0].h3_index)
    
    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "resolution": h3_resolution,
            "total_cells": len(cells),
            "coverage": "global",
            "generated_at": datetime.now().isoformat()
        },
        "features": features
    }
    
    # Save to core directory
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_file = DATA_DIR / f"h3_grid_res{h3_resolution}.json"
    
    with open(output_file, "w") as f:
        json.dump(geojson, f, separators=(',', ':'))  # No indentation = smaller file
    
    print(f"✅ Core grid: {output_file}")
    print(f"   Size: {output_file.stat().st_size / 1024:.1f} KB")

def export_vibe_scores(cells: List[H3Cell], metadata: Dict, pulse: Optional[GlobalPulse], insights: List[GlobalInsight]) -> None:
    """
    Enhanced: Includes ALL cell data for frontend display
    """
    print("📊 Generating vibe scores with complete cell data...")
    
    scores = {}
    for cell in cells:
        # Include ALL fields the frontend needs
        scores[cell.h3_index] = {
            # Core metrics
            "vibe": cell.vibe,
            "p_int": cell.p_intensity,
            "n_int": cell.n_intensity,
            "tone": cell.avg_tone,
            "polarity": cell.avg_polarity,
            "count": cell.total_events,
            
            # Event breakdown
            "noise_count": cell.noise_count,
            "progress_count": cell.progress_count,
            "neutral_count": cell.neutral_count,
            
            # Headlines for popups
            "top_progress_headline": cell.top_progress_headline,
            "top_noise_headline": cell.top_noise_headline,
            "headline_sample": cell.headline_sample,
            
            # Location data
            "location_sample": cell.location_sample,
            "centroid_lat": cell.centroid_lat,
            "centroid_lng": cell.centroid_lng,
            
            # Metadata
            "last_updated": cell.last_updated
        }
    
    output_data = {
        "generated_at": datetime.now().isoformat(),
        "pulse": pulse.model_dump(mode='json') if pulse else None,
        "insights": [i.model_dump(mode='json') for i in insights] if insights else [],
        "cells": scores
    }
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_file = DATA_DIR / "vibe_scores.json"
    
    with open(output_file, "w") as f:
        json.dump(output_data, f, separators=(',', ':'))
    
    print(f"✅ Vibe scores: {output_file}")
    print(f"   Size: {output_file.stat().st_size / 1024:.1f} KB")

def export_events_sample(cells: List[H3Cell]) -> None:
    """
    Export top events for detail panel (top 100 most active cells)
    """
    print("📰 Generating events sample...")
    
    # Get top 100 cells by event count
    top_cells = sorted(cells, key=lambda x: x.total_events, reverse=True)[:100]
    
    samples = []
    for cell in top_cells:
        samples.append({
            "h3_index": cell.h3_index,
            "location": cell.location_sample,
            "vibe": cell.vibe,
            "total_events": cell.total_events,
            "noise_count": cell.noise_count,
            "progress_count": cell.progress_count
        })
    
    output_data = {
        "generated_at": datetime.now().isoformat(),
        "total_samples": len(samples),
        "samples": samples
    }
    
    output_file = DATA_DIR / "events_sample.json"
    
    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2)
    
    print(f"✅ Events sample: {output_file}")
    print(f"   Size: {output_file.stat().st_size / 1024:.1f} KB")

def export_metadata(cells: List[H3Cell], metadata: Dict, pulse: Optional[GlobalPulse]) -> None:
    """Export metadata file for frontend to know what's available"""
    h3_resolution = h3.get_resolution(cells[0].h3_index)
    
    meta = {
        "schema_version": "2.0",
        "generated_at": datetime.now().isoformat(),
        "h3_resolution": h3_resolution,
        "total_cells": len(cells),
        "total_events": metadata.get('total_events', 0),
        "global_avg_vibe": metadata.get('global_avg_vibe', 0),
        "vibe_range": metadata.get('vibe_range', {}),
        "available_files": {
            "core_grid": f"h3_grid_res{h3_resolution}.json",
            "vibe_scores": "vibe_scores.json",
            "events_sample": "events_sample.json"
        },
        "update_frequency": "Every 8 hours",
        "next_update": "TBD"  # Can be calculated from GitHub Actions schedule
    }
    
    output_file = DATA_DIR / "metadata.json"
    
    with open(output_file, "w") as f:
        json.dump(meta, f, indent=2)
    
    print(f"✅ Metadata: {output_file}")

def run():
    print("🚀 AIC Export Pipeline")
    print("=" * 50)
    
    # Load processed cells
    cells, metadata, pulse, insights = load_aggregated_cells()
    
    if not cells:
        print("❌ No cells to export. Run calculate_vibe.py first.")
        # When running as part of a pipeline, we might want to return early
        return
    
    print(f"\n📦 Exporting {len(cells)} cells to optimized format...")
    print()
    
    # Export layered files
    export_core_grid(cells)          # Load once, cache forever
    export_vibe_scores(cells, metadata, pulse, insights)  # Refresh every 8h - NOW WITH FULL DATA
    export_events_sample(cells)      # Top 100 for detail panel
    export_metadata(cells, metadata, pulse)  # Schema info
    
    print("\n" + "=" * 50)
    print("✅ Export complete!")
    print("\nGenerated files:")
    print("  📐 h3_grid_res*.json    (geometry, cache forever)")
    print("  📊 vibe_scores.json     (data, refresh 8h)")
    print("  📰 events_sample.json   (top events)")
    print("  ℹ️  metadata.json        (schema info)")
    
    # Calculate total size
    total_size = sum(f.stat().st_size for f in DATA_DIR.rglob('*.json') if f.is_file())
    total_size += sum(f.stat().st_size for f in DATA_DIR.rglob('*.geojson') if f.is_file())
    
    print(f"\n📦 Total data size: {total_size / 1024:.1f} KB")
    print(f"   Estimated gzipped: {total_size / 1024 * 0.2:.1f} KB (80% compression)")

if __name__ == "__main__":
    run()