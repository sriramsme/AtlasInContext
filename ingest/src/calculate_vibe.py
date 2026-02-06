import json
import pandas as pd
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from pydantic import BaseModel
from schema.models import RawEvent, H3Cell, GlobalPulse, GlobalInsight, AggregateResult
from utils import DATA_DIR

def load_raw_events() -> List[RawEvent]:
    """Load raw GDELT events"""
    input_file = DATA_DIR / "gdelt_events.json"
    
    if not input_file.exists():
        print(f"❌ No raw events found at {input_file}")
        return []
    
    with open(input_file) as f:
        data = json.load(f)
    
    events_data = data.get('events', [])
    events = [RawEvent(**e) for e in events_data]
    print(f"📥 Loaded {len(events)} raw events")
    return events

def aggregate_to_h3(events: List[RawEvent]) -> AggregateResult:
    """Aggregate raw events into H3 hexagons with vibe calculation"""
    
    # Group by H3 index
    h3_groups = defaultdict(list)
    for event in events:
        h3_groups[event.h3_index].append(event)
    
    print(f"🔄 Aggregating {len(events)} events into {len(h3_groups)} H3 cells...")
    
    # Calculate vibe per hexagon
    cells: List[H3Cell] = []
    # These will track the global "Pulse"
    total_p_weight = 0
    total_n_weight = 0

    for h3_index, group in h3_groups.items():
        # Averages
        avg_tone = sum(e.tone for e in group) / len(group)
        avg_p = sum(e.p_weight for e in group) / len(group)
        avg_n = sum(e.n_weight for e in group) / len(group)
        avg_polarity = sum(e.polarity for e in group) / len(group)
        
        # --- THE VIBE FORMULA ---
        # We want to balance the "What" (Themes) with the "How" (Tone)
        # Final Score V = 0.6 * (Theme Balance) + 0.4 * (Normalized Tone)
        
        # Count categories
        noise_count = sum(1 for e in group if e.category == 'noise')
        progress_count = sum(1 for e in group if e.category == 'progress')
        neutral_count = sum(1 for e in group if e.category == 'neutral')
        
        # Get centroid from first event (for reference only)
        first_event = group[0]
        
        # VIBE FORMULA
        # α = News sentiment weight (GDELT tone)
        # β = Progress weight (OWID, not yet implemented)
        # γ = Category balance modifier
        
        theme_balance = (avg_p - avg_n) / (avg_p + avg_n + 0.1) # Range ~ -1 to 1
        normalized_tone = max(-1, min(1, avg_tone / 10.0))    # GDELT tone is usually -10 to 10
        
        vibe_score = (0.6 * theme_balance) + (0.4 * normalized_tone)

        # Track global weights for the Pulse ticker
        cell_p = sum(e.p_weight for e in group)
        cell_n = sum(e.n_weight for e in group)
        total_p_weight += cell_p
        total_n_weight += cell_n

        # --- Insight Selection ---
        # Sort group to find the most "thematically heavy" stories
        best_progress_story = max(group, key=lambda x: x.p_weight)
        best_noise_story = max(group, key=lambda x: x.n_weight)
        
        cells.append(H3Cell(
            h3_index=h3_index,
            centroid_lat=round(first_event.lat, 4),
            centroid_lng=round(first_event.lng, 4),
            vibe=round(vibe_score, 3),
            top_progress_headline=best_progress_story.headline,
            top_noise_headline=best_noise_story.headline,
            p_intensity=round(cell_p, 2),
            n_intensity=round(cell_n, 2),
            avg_tone=round(avg_tone, 2),
            avg_polarity=round(avg_polarity, 2),
            noise_count=noise_count,
            progress_count=progress_count,
            neutral_count=neutral_count,
            total_events=len(group),
            headline_sample=next((e.headline for e in group if e.headline), "N/A"),
            location_sample=first_event.location_name or 'Unknown',
            last_updated=datetime.now().isoformat()
        ))
    
    # Sort by total events (most active cells first)
    cells.sort(key=lambda x: x.total_events, reverse=True)

    # Prepare the Global Pulse
    global_pulse = GlobalPulse(
        progress_signal=round(total_p_weight, 0),
        noise_signal=round(total_n_weight, 0),
        humanity_ratio=round(total_p_weight / (total_n_weight + 1), 2)
    )

    # Prepare Global Insights (The 5 strongest stories worldwide)
    top_global_progress = sorted(events, key=lambda x: x.p_weight, reverse=True)[:5]
    
    return AggregateResult(
        pulse=global_pulse,
        insights=[GlobalInsight(headline=e.headline, url=e.id) for e in top_global_progress],
        cells=cells
    )

def save_aggregated_cells(result: AggregateResult) -> None:
    """Save the full AggregateResult to JSON for CDN delivery"""
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    output_file = DATA_DIR / "h3_cells.json"
    
    # Calculate summary statistics for metadata
    # Access properties directly from the object
    total_events = sum(c.total_events for c in result.cells)
    avg_vibe = sum(c.vibe for c in result.cells) / len(result.cells) if result.cells else 0
    
    # Serialize to dicts
    result_dict = result.model_dump(mode='json')

    # Combine everything into the final static JSON structure
    output_data = {
        "generated_at": datetime.now().isoformat(),
        "metadata": {
            "total_cells": len(result.cells),
            "total_events": total_events,
            "global_avg_vibe": round(avg_vibe, 3),
        },
        "pulse": result_dict['pulse'],        # The "Ticker" data
        "insights": result_dict['insights'],  # The "Sidebar" stories
        "cells": result_dict['cells']         # The "Map" data
    }
    
    with open(output_file, "w") as f:
        # Use separators to minimize file size if this gets huge
        json.dump(output_data, f, indent=2)
    
    print(f"\n💾 Saved AggregateResult to {output_file}")
    print(f"📦 File size: {output_file.stat().st_size / 1024:.1f} KB")

def print_statistics(result: AggregateResult) -> None:
    """Print detailed statistics including Pulse and Insights"""
    if not result.cells:
        print("⚠️ No data to summarize.")
        return
    
    print(f"\n🌍 AIC GLOBAL PULSE")
    print(f"{'='*50}")
    print(f"Humanity Ratio:   {result.pulse.humanity_ratio}x")
    print(f"Progress Signal:  {result.pulse.progress_signal}")
    print(f"Noise Signal:     {result.pulse.noise_signal}")
    
    print(f"\n💡 TOP PROGRESS INSIGHTS")
    for i, insight in enumerate(result.insights, 1):
        print(f"  {i}. {insight.headline[:80]}...")

    print(f"\n📊 GRID STATISTICS")
    print(f"{'-'*50}")
    total_events = sum(c.total_events for c in result.cells)
    vibes = [c.vibe for c in result.cells]
    
    print(f"Total H3 Cells:   {len(result.cells)}")
    print(f"Total Events:     {total_events}")
    print(f"Avg Vibe Score:   {sum(vibes) / len(vibes):.3f}")
    
    # Category breakdown from cells
    total_noise = sum(c.noise_count for c in result.cells)
    total_progress = sum(c.progress_count for c in result.cells)
    
    print(f"\n📈 EVENT MIX")
    print(f"  Noise Stories:    {total_noise} ({total_noise/total_events*100:.1f}%)")
    print(f"  Progress Stories: {total_progress} ({total_progress/total_events*100:.1f}%)")
    
    print(f"\n🔥 HOTSPOT REGIONS")
    for i, cell in enumerate(result.cells[:5], 1):
        # Determine color/emoji based on vibe
        emoji = "🟢" if cell.vibe > 0.1 else "🔴" if cell.vibe < -0.1 else "🟡"
        print(f"  {i}. {emoji} {cell.location_sample}: {cell.total_events} events (Vibe: {cell.vibe:.2f})")


def run():
    print("🚀 AIC Aggregation Pipeline")
    print("=" * 50)
    
    events = load_raw_events()
    
    if not events:
        print("❌ No events to process. Run fetch_gdelt.py first.")
        # When running as part of a pipeline, we might want to raise an error
        # but for now we'll just return
        return
    
    # Now returns an AggregateResult object instead of a list
    result = aggregate_to_h3(events)
    
    if result.cells:
        print_statistics(result)
        save_aggregated_cells(result)
        print(f"\n✅ Pipeline complete. Ready for CDN deployment.")
    else:
        print("❌ Aggregation produced no results.")

if __name__ == "__main__":
    run()