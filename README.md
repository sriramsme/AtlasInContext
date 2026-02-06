# Atlas In Context - Optimized Data Pipeline

A performant geospatial application that visualizes the tension between headline noise and global progress using an optimized static-file architecture.

### Data Flow

```
GDELT API (Live Global Events)
         ↓
[fetch_gdelt.py] → Extract & Categorize (Noise vs Progress)
         ↓
[calculate_vibe.py] → Aggregate into H3 Grids
         ↓
[export_geojson.py] → Generate GeoJSON & Vibe Scores
         ↓
[main.py Cleanup] → Auto-delete raw/intermediate JSONs
         ↓
[main.py Compress] → Gzip final assets (*.json.gz)
         ↓
GitHub Actions commit + Cloudflare Pages Deploy
         ↓
Frontend (Client-side decompression via DecompressionStream)
```

## Quick Start

### 1. Install Dependencies

```bash
cd ingest
pip install requests h3 pandas pydantic
```

### 2. Run Integrated Data Pipeline

```bash
# Runs fetch -> calculate -> export -> cleanup -> compress
uv run ingest/src/main.py
```

### 3. Verify Generated Assets

```bash
# Check the clean data directory
ls -lh data/

# You should see only .gz files (raw events are auto-deleted)
# h3_grid_res3.json.gz
# vibe_scores.json.gz
# ...
```

### 4. Serve Locally

```bash
# Simple HTTP server for testing
python -m http.server 8000

# Or use Vite for frontend
cd frontend
npm install
npm run dev
```

## File Structure

```
data/
├── h3_grid_res3.json.gz     # H3 geometry (Compressed)
├── vibe_scores.json.gz      # Vibe data, Pulse, & Insights (Compressed)
├── events_sample.json.gz    # Top 100 sample events (Compressed)
├── metadata.json.gz         # Deployment & Schema Metadata (Compressed)
└── manifest.json            # Deployment timestamp for cache busting
```

### Compression & Performance

- **Automatic Extraction**: All final assets are gzipped at birth in the pipeline.
- **Client-Side Decompression**: The frontend uses the browser's native `DecompressionStream` for transparent, high-performance loading.
- **Payload Efficiency**: This reduces payload sizes by **~80%**..

## Automated Pipeline (GitHub Actions)

The workflow runs **every 8 hours** automatically:

- `0:00 UTC` - Fetch GDELT + aggregate + export
- `8:00 UTC` - Refresh
- `16:00 UTC` - Refresh

### Manual Trigger

```bash
# Via GitHub UI
Actions → Atlas In Context Data Ingestion → Run workflow

# Or via gh CLI
gh workflow run ingest.yml
```

### Monitor Status

Check the Actions tab in your GitHub repo to see:

- Ingestion logs
- File sizes
- Error messages
- Generated data summary

## Frontend Integration

### 1. Map Initialization

The map initializes using the H3 geometry grid. Because we use H3 resolution indexes as Feature IDs, all vibe data is joined on the GPU for instant rendering.

### 2. Smart Data Fetching

We use a custom `fetchWithDecompress` helper in `main.js` that:

- Detects the Gzip magic header.
- Decompresses data using `DecompressionStream`.
- Falls back to plain JSON if required (useful for local development).

### 3. Multi-CDN Strategy

- **Development**: Fetches from `/data`.
- **Production**: Routes through **jsDelivr CDN** for sub-100ms global latency.

```javascript
// Example production URL
const CDN_URL = 'https://cdn.jsdelivr.net/gh/sriramsme/AtlasInContext@main/data/vibe_scores.json.gz';
```

## Configuration

### H3 Resolution

Change in `fetch_gdelt.py`:

```python
H3_RESOLUTION = 3  # Global: ~4k cells (100-300km per hex)
# H3_RESOLUTION = 4  # Regional: ~29k cells (50-150km per hex)
# H3_RESOLUTION = 5  # City: ~208k cells (10-50km per hex)
```

**Trade-offs:**

- Lower resolution = Fewer cells, larger hexagons, smaller files
- Higher resolution = More detail, more cells, larger files

### Vibe Formula

Modify in `calculate_vibe.py`:

```python
α = 0.7  # News sentiment weight (GDELT tone)
β = 0.3  # Progress weight (OWID, when implemented)

final_vibe = (α * normalized_tone) + (β * progress_score)
```

### Event Limit

Change in `fetch_gdelt.py`:

```python
max_events = 2000  # Increase for more data
```

## 📈 Performance Optimization

### Current Performance

| Metric | Target | Status |
|--------|--------|--------|
| Initial Load | <3s on 3G | ✅ 1.3MB total |
| Core Grid | <1s | ✅ 800KB |
| Vibe Scores | <500ms | ✅ 500KB |
| Memory Usage | <150MB | ✅ ~80MB |

### Future Optimizations (when needed)

#### Phase 2: Binary Format (30% smaller)

```bash
pip install msgpack

# In export_optimized.py
import msgpack
msgpack.pack(scores, open('vibe_scores.msgpack', 'wb'))
```

#### Phase 3: Vector Tiles (5x compression)

```bash
npm install -g tippecanoe

tippecanoe -o AIC.pmtiles \
  -z12 -Z0 \
  --drop-densest-as-needed \
  data/core/h3_grid_res3.json
```

## Troubleshooting

### Pipeline fails in GitHub Actions

**Check:**

1. GDELT API is accessible (sometimes down)
2. Python dependencies installed correctly
3. `data/` directory has write permissions

**Debug locally:**

```bash
cd ingest/src
python fetch_gdelt.py  # Test GDELT connection
```

### Frontend not loading data

**Check:**

1. Files exist in `data/` directory
2. CORS headers if serving from different domain
3. Browser console for errors

**Quick fix:**

```bash
# Serve with CORS enabled
python -m http.server 8000 --bind 0.0.0.0
```

### Large file sizes

**Solutions:**

1. Lower H3 resolution (3 instead of 4)
2. Reduce `max_events` in fetch script
3. Enable gzip compression (automatic on GitHub Pages)

## License

MIT

## Credits

- **GDELT Project** - Global news data
- **Uber H3** - Hexagonal indexing
- **MapLibre GL JS** - Map rendering
- **Our World in Data** - Progress metrics (future)
