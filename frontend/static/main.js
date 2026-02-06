// frontend/src/main.js
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import './style.css';

// ============================================================================
// CONFIGURATION
// ============================================================================

const IS_PROD = window.location.hostname !== 'localhost' && window.location.hostname !== '127.0.0.1';
const REPO_OWNER = 'sriramsme'; // Based on project attribution
const REPO_NAME = 'AtlasInContext';

const CONFIG = {
    // In dev, use local /data. In prod, use jsDelivr CDN for speed and compression.
    dataBaseUrl: IS_PROD
        ? `https://cdn.jsdelivr.net/gh/${REPO_OWNER}/${REPO_NAME}@main/data`
        : '/data',
    useGzip: true, // Set to true to fetch .gz files (mirrors production compression)
    mapStyle: {
        version: 8,
        sources: {
            'carto-dark': {
                type: 'raster',
                tiles: ['https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png'],
                tileSize: 256,
                attribution: '©OpenStreetMap, ©CartoDB'
            }
        },
        layers: [{
            id: 'carto-dark-layer',
            type: 'raster',
            source: 'carto-dark',
            minzoom: 0,
            maxzoom: 22
        }]
    },
    mapOptions: {
        center: [0, 20],
        zoom: 2,
        maxZoom: 12,
        minZoom: 1.5,
    },
    viewModes: {
        balanced: { mode: 'vibe', label: 'Balanced Vibe' },
        progress: { mode: 'progress', label: 'Progress Intensity' },
        noise: { mode: 'noise', label: 'Noise Intensity' }
    }
};

// ============================================================================
// STATE MANAGEMENT
// ============================================================================

const state = {
    map: null,
    coreGrid: null,
    vibeScores: null,
    metadata: null,
    pulse: null,
    insights: null,
    currentViewMode: 'balanced', // 'balanced', 'progress', or 'noise'
    isLoading: false,
    currentPopup: null,
    hoverTooltip: null,
    sidePanelOpen: false
};

// ============================================================================
// UI PANEL CONTROLS
// ============================================================================

/**
 * Toggle side panel open/close
 */
function toggleSidePanel() {
    const panel = document.getElementById('sidePanel');
    const backdrop = document.getElementById('panelBackdrop');

    if (!panel) return;

    state.sidePanelOpen = !state.sidePanelOpen;

    if (state.sidePanelOpen) {
        panel.classList.add('open');
        if (backdrop && window.innerWidth < 768) {
            backdrop.classList.remove('hidden');
        }
    } else {
        panel.classList.remove('open');
        if (backdrop) {
            backdrop.classList.add('hidden');
        }
    }
}

/**
 * Close side panel
 */
function closeSidePanel() {
    const panel = document.getElementById('sidePanel');
    const backdrop = document.getElementById('panelBackdrop');

    if (!panel) return;

    state.sidePanelOpen = false;
    panel.classList.remove('open');

    if (backdrop) {
        backdrop.classList.add('hidden');
    }
}

// ============================================================================
// DATA LOADING
// ============================================================================

/**
 * Enhanced fetch that handles automatic Gzip decompression only if the data is compressed.
 * This handles cases where dev servers (like Vite) or browsers might already decompress 
 * based on Content-Encoding headers.
 */
async function fetchWithDecompress(url, options = {}) {
    const fetchUrl = CONFIG.useGzip ? `${url}.gz` : url;
    const response = await fetch(fetchUrl, options);

    if (!response.ok) {
        if (CONFIG.useGzip) {
            console.warn(`⚠️ File not found at ${fetchUrl}, falling back to ${url}...`);
            const fallbackResponse = await fetch(url, options);
            if (!fallbackResponse.ok) throw new Error(`HTTP ${fallbackResponse.status}`);
            return await fallbackResponse.json();
        }
        throw new Error(`HTTP ${response.status}`);
    }

    // Peek at the first chunk of data to see if it's actually Gzipped
    // Gzip files always start with 0x1f 0x8b
    const reader = response.body.getReader();
    const { value, done } = await reader.read();

    if (done) return null;

    // Check for GZIP magic number 1f 8b
    const isEncrypted = value[0] === 0x1f && value[1] === 0x8b;

    // Reconstruct the stream since we've already consumed the first chunk
    const combinedStream = new ReadableStream({
        start(controller) {
            controller.enqueue(value);
            function push() {
                reader.read().then(({ done, value }) => {
                    if (done) {
                        controller.close();
                        return;
                    }
                    controller.enqueue(value);
                    push();
                });
            }
            push();
        }
    });

    if (isEncrypted) {
        try {
            const ds = new DecompressionStream('gzip');
            const decompressedStream = combinedStream.pipeThrough(ds);
            return await new Response(decompressedStream).json();
        } catch (e) {
            console.error('❌ Manual decompression failed:', e);
            throw new Error('CORRUPTED_GZIP: Data header matched but decompression failed.');
        }
    }

    // Already decompressed (likely by browser or dev server)
    return await new Response(combinedStream).json();
}

/**
 * Load metadata to know what files are available
 */
async function loadMetadata() {
    try {
        const metadata = await fetchWithDecompress(`${CONFIG.dataBaseUrl}/metadata.json`);
        console.log('✅ Metadata loaded:', metadata);
        return metadata;
    } catch (error) {
        console.error('❌ Failed to load metadata:', error);
        throw new Error('Could not load metadata file. Ensure data pipeline has run.');
    }
}

/**
 * Load core H3 grid (geometry only, cached forever)
 */
async function loadCoreGrid(metadata) {
    updateLoadingText('Loading grid geometry...');

    try {
        const gridFile = metadata.available_files.core_grid;
        // In our new flat structure, the core_grid path in metadata might still have 'core/' prefix
        // Let's strip it if it exists since we moved everything to root /data
        const cleanPath = gridFile.replace(/^core\//, '').replace(/^current\//, '');

        const grid = await fetchWithDecompress(`${CONFIG.dataBaseUrl}/${cleanPath}`, {
            cache: 'force-cache'
        });

        // Ensure each feature has its H3 index as a property
        grid.features = grid.features.map(feature => ({
            ...feature,
            properties: {
                ...feature.properties,
                h3_index: feature.id
            }
        }));

        console.log(`✅ Core grid loaded: ${grid.features.length} H3 cells (res ${grid.metadata.resolution})`);

        return grid;
    } catch (error) {
        console.error('❌ Failed to load core grid:', error);
        throw new Error('Could not load grid geometry. Check data files.');
    }
}

/**
 * Load current vibe scores with pulse and insights
 */
async function loadVibeScores() {
    updateLoadingText('Loading vibe scores...');

    try {
        // Strip any 'current/' prefix from the path if needed, though we fetch directly here
        const data = await fetchWithDecompress(`${CONFIG.dataBaseUrl}/vibe_scores.json`, {
            cache: 'no-cache'
        });

        // Extract pulse and insights from the new structure
        state.pulse = data.pulse || null;
        state.insights = data.insights || [];

        console.log(`✅ Vibe scores loaded: ${Object.keys(data.cells).length} cells`);
        if (state.pulse) {
            console.log(`   Global Pulse: Progress ${state.pulse.progress_signal} | Noise ${state.pulse.noise_signal} | Ratio ${state.pulse.humanity_ratio}`);
        }
        console.log(`   Insights: ${state.insights.length} stories`);

        return data;
    } catch (error) {
        console.error('❌ Failed to load vibe scores:', error);
        throw new Error('Could not load vibe scores. Data may be processing.');
    }
}

/**
 * Load all data sequentially
 */
async function loadAllData() {
    try {
        state.isLoading = true;

        state.metadata = await loadMetadata();
        state.coreGrid = await loadCoreGrid(state.metadata);
        state.vibeScores = await loadVibeScores();

        state.isLoading = false;
        return true;

    } catch (error) {
        state.isLoading = false;
        throw error;
    }
}

// ============================================================================
// VISUALIZATION MODE LOGIC
// ============================================================================

/**
 * Get the value to visualize based on current view mode
 */
function getVisualizationValue(h3Index) {
    const cellData = state.vibeScores.cells[h3Index];
    if (!cellData) return 0;

    switch (state.currentViewMode) {
        case 'progress':
            return Math.min(1, cellData.p_int / 10);
        case 'noise':
            return Math.min(1, cellData.n_int / 10);
        case 'balanced':
        default:
            return (cellData.vibe + 1) / 2;
    }
}

/**
 * Create lookup object for MapLibre expressions
 */
function createVisualizationLookup() {
    const lookup = {};

    for (const h3Index of Object.keys(state.vibeScores.cells)) {
        lookup[h3Index] = getVisualizationValue(h3Index);
    }

    console.log('🎨 Created visualization lookup with', Object.keys(lookup).length, 'cells');
    console.log('🎨 Mode:', state.currentViewMode);

    return lookup;
}

/**
 * Get color scheme based on view mode
 */
function getColorScheme() {
    switch (state.currentViewMode) {
        case 'progress':
            return [
                0.0, '#1e293b',
                0.2, '#22c55e',
                0.4, '#10b981',
                0.6, '#14b8a6',
                0.8, '#06b6d4',
                1.0, '#0ea5e9'
            ];
        case 'noise':
            return [
                0.0, '#1e293b',
                0.2, '#fbbf24',
                0.4, '#f97316',
                0.6, '#ef4444',
                0.8, '#dc2626',
                1.0, '#991b1b'
            ];
        case 'balanced':
        default:
            return [
                0.0, '#ef4444',
                0.25, '#f97316',
                0.5, '#eab308',
                0.65, '#84cc16',
                0.8, '#22c55e',
                1.0, '#10b981'
            ];
    }
}

// ============================================================================
// MAP INITIALIZATION
// ============================================================================

/**
 * Initialize MapLibre map
 */
async function initializeMap() {
    updateLoadingText('Initializing map...');

    const container = document.getElementById('map');
    if (!container) throw new Error('Map container not found');

    const checkDimensions = () => {
        const rect = container.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
    };

    let retryCount = 0;
    while (!checkDimensions() && retryCount < 20) {
        console.warn(`⚠️ Map container has no size (retry ${retryCount}/20), waiting...`);
        await new Promise(resolve => setTimeout(resolve, 100));
        retryCount++;
    }

    if (!checkDimensions()) {
        const rect = container.getBoundingClientRect();
        throw new Error(`Map container still has invalid dimensions after waiting: ${rect.width}x${rect.height}`);
    }

    try {
        console.log('🗺️  Initializing map...');
        state.map = new maplibregl.Map({
            container: 'map',
            style: CONFIG.mapStyle,
            ...CONFIG.mapOptions
        });

        console.log('🗺️  Map initialized');

        state.map.addControl(new maplibregl.NavigationControl(), 'top-left');
        state.map.addControl(new maplibregl.ScaleControl(), 'bottom-left');

        console.log('🗺️  Map controls added');
        return state.map;

    } catch (error) {
        console.error('❌ Map constructor failed:', error);

        if ((error.message.includes('null') || error.message.includes('property 0')) && !window._mapRetry) {
            console.log('🔄 Attempting automatic retry for map initialization...');
            window._mapRetry = true;
            await new Promise(resolve => setTimeout(resolve, 500));
            return initializeMap();
        }

        throw new Error(`Map initialization failed: ${error.message}`);
    }
}

/**
 * Add data sources to map
 */
function addMapSources() {
    updateLoadingText('Adding data sources...');

    state.map.addSource('h3-grid', {
        type: 'geojson',
        data: state.coreGrid,
        promoteId: 'h3_index'
    });

    console.log('✅ Added h3-grid source');
}

/**
 * Add map layers with data-driven styling
 */
function addMapLayers() {
    updateLoadingText('Rendering visualization...');

    const vizLookup = createVisualizationLookup();
    const colorScheme = getColorScheme();

    state.map.addLayer({
        id: 'vibe-fill',
        type: 'fill',
        source: 'h3-grid',
        paint: {
            'fill-color': [
                'case',
                ['has', ['get', 'h3_index'], ['literal', vizLookup]],
                [
                    'interpolate',
                    ['linear'],
                    ['get', ['get', 'h3_index'], ['literal', vizLookup]],
                    ...colorScheme
                ],
                '#1e293b'
            ],
            'fill-opacity': [
                'interpolate',
                ['linear'],
                ['zoom'],
                1.5, 0.7,
                5, 0.85,
                10, 0.9
            ]
        }
    });

    state.map.addLayer({
        id: 'vibe-outline',
        type: 'line',
        source: 'h3-grid',
        paint: {
            'line-color': '#ffffff',
            'line-width': [
                'interpolate',
                ['linear'],
                ['zoom'],
                1.5, 0.3,
                5, 0.8,
                10, 1.5
            ],
            'line-opacity': 0.2
        }
    });

    state.map.addLayer({
        id: 'vibe-hover',
        type: 'line',
        source: 'h3-grid',
        paint: {
            'line-color': '#ffffff',
            'line-width': 3,
            'line-opacity': [
                'case',
                ['boolean', ['feature-state', 'hover'], false],
                1,
                0
            ]
        }
    });

    console.log('✅ Added visualization layers');
}

/**
 * Add map interactions (click, hover)
 */
function addMapInteractions() {
    let hoveredId = null;

    state.map.on('mousemove', 'vibe-fill', (e) => {
        if (e.features.length > 0) {
            const feature = e.features[0];
            const h3Index = feature.properties.h3_index;
            const cellData = state.vibeScores.cells[h3Index];

            if (hoveredId !== null) {
                state.map.setFeatureState(
                    { source: 'h3-grid', id: hoveredId },
                    { hover: false }
                );
            }

            hoveredId = h3Index;

            state.map.setFeatureState(
                { source: 'h3-grid', id: hoveredId },
                { hover: true }
            );

            state.map.getCanvas().style.cursor = 'pointer';

            if (cellData) {
                showHoverTooltip(e.lngLat, cellData, h3Index);
            }
        }
    });

    state.map.on('mouseleave', 'vibe-fill', () => {
        if (hoveredId !== null) {
            state.map.setFeatureState(
                { source: 'h3-grid', id: hoveredId },
                { hover: false }
            );
        }
        hoveredId = null;
        state.map.getCanvas().style.cursor = '';

        if (state.hoverTooltip) {
            state.hoverTooltip.remove();
            state.hoverTooltip = null;
        }
    });

    state.map.on('click', 'vibe-fill', (e) => {
        const feature = e.features[0];
        const h3Index = feature.properties.h3_index;
        const cellData = state.vibeScores.cells[h3Index];

        if (!cellData) return;

        if (state.hoverTooltip) {
            state.hoverTooltip.remove();
            state.hoverTooltip = null;
        }

        if (state.currentPopup) {
            state.currentPopup.remove();
        }

        showDetailPopup(e.lngLat, cellData, h3Index);
    });

    console.log('✅ Map interactions enabled');
}

/**
 * Show hover tooltip with enhanced information
 */
function showHoverTooltip(lngLat, cellData, h3Index) {
    if (state.hoverTooltip) {
        state.hoverTooltip.remove();
    }

    const vibeColor = getVibeColor(cellData.vibe);
    const vibeLabel = getVibeLabel(cellData.vibe);

    const totalEvents = cellData.count || 0;
    const noiseCount = cellData.noise_count || 0;
    const progressCount = cellData.progress_count || 0;

    const location = cellData.location_sample || 'Unknown Location';
    const headline = cellData.headline_sample;

    const truncatedHeadline = headline && headline !== 'N/A'
        ? (headline.length > 80 ? headline.substring(0, 77) + '...' : headline)
        : null;

    state.hoverTooltip = new maplibregl.Popup({
        closeButton: false,
        closeOnClick: false,
        className: 'hover-tooltip',
        offset: 10,
        maxWidth: '300px'
    })
        .setLngLat(lngLat)
        .setHTML(`
            <div class="px-3 py-2 bg-slate-900/95 backdrop-blur-sm text-white rounded-lg shadow-xl border border-slate-700 text-xs">
                <div class="font-semibold text-slate-200 mb-2">${location}</div>
                
                <div class="flex items-center gap-2 mb-2">
                    <div class="w-2 h-2 rounded-full" style="background-color: ${vibeColor}"></div>
                    <span class="font-bold text-white">${vibeLabel}</span>
                    <span class="font-mono text-slate-400">${cellData.vibe.toFixed(2)}</span>
                </div>
                
                <div class="text-slate-300 mb-2">
                    <span class="font-semibold">${totalEvents}</span> events • 
                    <span class="text-red-400">${noiseCount}</span> noise / 
                    <span class="text-green-400">${progressCount}</span> progress
                </div>
                
                ${truncatedHeadline ? `
                    <div class="text-slate-400 italic text-xs pt-2 border-t border-slate-700 line-clamp-2">
                        "${truncatedHeadline}"
                    </div>
                ` : ''}
                
                <div class="text-slate-500 text-xs mt-2 pt-2 border-t border-slate-700">
                    Click for details
                </div>
            </div>
        `)
        .addTo(state.map);
}

/**
 * Show detailed popup with all available information
 */
function showDetailPopup(lngLat, cellData, h3Index) {
    const vibeColor = getVibeColor(cellData.vibe);
    const vibeLabel = getVibeLabel(cellData.vibe);

    const totalEvents = cellData.count || 0;
    const noiseCount = cellData.noise_count || 0;
    const progressCount = cellData.progress_count || 0;
    const neutralCount = cellData.neutral_count || 0;

    const noisePercent = totalEvents > 0
        ? (noiseCount / totalEvents * 100).toFixed(1)
        : 0;
    const progressPercent = totalEvents > 0
        ? (progressCount / totalEvents * 100).toFixed(1)
        : 0;

    state.currentPopup = new maplibregl.Popup({
        closeButton: true,
        closeOnClick: true,
        maxWidth: '400px',
        className: 'detail-modal'
    })
        .setLngLat(lngLat)
        .setHTML(`
            <div class="bg-slate-900 text-white rounded-xl overflow-hidden max-h-[80vh] overflow-y-auto">
                <div class="bg-gradient-to-r from-slate-800 to-slate-900 px-4 py-3 border-b border-slate-700 sticky top-0 z-10">
                    <div class="flex items-center justify-between">
                        <div>
                            <h3 class="text-base font-bold">${cellData.location_sample || 'Region Details'}</h3>
                            <div class="text-xs text-slate-400 mt-1">
                                ${cellData.centroid_lat?.toFixed(2) || '—'}, ${cellData.centroid_lng?.toFixed(2) || '—'}
                            </div>
                        </div>
                        <div class="flex items-center gap-2">
                            <div class="w-3 h-3 rounded-full shadow-lg" style="background-color: ${vibeColor}; box-shadow: 0 0 10px ${vibeColor}40"></div>
                            <span class="text-sm font-mono font-bold" style="color: ${vibeColor}">${cellData.vibe.toFixed(3)}</span>
                        </div>
                    </div>
                    <div class="mt-2">
                        <span class="text-xs px-2 py-1 rounded-full font-semibold" style="background-color: ${vibeColor}20; color: ${vibeColor}">
                            ${vibeLabel}
                        </span>
                    </div>
                </div>
                    
                    <div class="space-y-2">
                        ${cellData.top_progress_headline ? `
                            <div class="bg-green-500/10 border border-green-500/30 rounded-lg p-3">
                                <div class="text-xs text-green-400 font-semibold mb-1">🟢 Top Progress Story</div>
                                <div class="text-sm text-slate-200 line-clamp-3">${cellData.top_progress_headline}</div>
                            </div>
                        ` : ''}
                        
                        ${cellData.top_noise_headline ? `
                            <div class="bg-red-500/10 border border-red-500/30 rounded-lg p-3">
                                <div class="text-xs text-red-400 font-semibold mb-1">🔴 Top Noise Story</div>
                                <div class="text-sm text-slate-200 line-clamp-3">${cellData.top_noise_headline}</div>
                            </div>
                        ` : ''}
                    </div>
                    
                    <div class="grid grid-cols-3 gap-2">
                        <div class="bg-slate-800/50 rounded-lg p-3 text-center">
                            <div class="text-xs text-slate-400 mb-1">Avg Tone</div>
                            <div class="text-lg font-bold">${cellData.tone?.toFixed(1) || '—'}</div>
                        </div>
                        <div class="bg-slate-800/50 rounded-lg p-3 text-center">
                            <div class="text-xs text-slate-400 mb-1">Polarity</div>
                            <div class="text-lg font-bold">${cellData.polarity?.toFixed(1) || '—'}</div>
                        </div>
                        <div class="bg-slate-800/50 rounded-lg p-3 text-center">
                            <div class="text-xs text-slate-400 mb-1">Events</div>
                            <div class="text-lg font-bold">${totalEvents}</div>
                        </div>
                    </div>
                    
                    <div class="grid grid-cols-2 gap-3">
                        <div class="bg-green-500/10 border border-green-500/30 rounded-lg p-3">
                            <div class="text-xs text-green-400 mb-1">Progress Intensity</div>
                            <div class="text-2xl font-bold text-green-400">${cellData.p_int?.toFixed(1) || '0.0'}</div>
                            <div class="text-xs text-slate-400 mt-1">${progressCount} events (${progressPercent}%)</div>
                        </div>
                        <div class="bg-red-500/10 border border-red-500/30 rounded-lg p-3">
                            <div class="text-xs text-red-400 mb-1">Noise Intensity</div>
                            <div class="text-2xl font-bold text-red-400">${cellData.n_int?.toFixed(1) || '0.0'}</div>
                            <div class="text-xs text-slate-400 mt-1">${noiseCount} events (${noisePercent}%)</div>
                        </div>
                    </div>
                    
                    <div class="bg-slate-800/50 rounded-lg p-3">
                        <div class="text-xs text-slate-400 mb-2">Event Distribution</div>
                        
                        <div class="h-2 bg-slate-700 rounded-full overflow-hidden flex mb-2">
                            <div class="bg-red-500" style="width: ${noisePercent}%"></div>
                            <div class="bg-green-500" style="width: ${progressPercent}%"></div>
                            <div class="bg-slate-600" style="width: ${100 - parseFloat(noisePercent) - parseFloat(progressPercent)}%"></div>
                        </div>
                        
                        <div class="grid grid-cols-3 gap-2 text-xs">
                            <div class="text-center">
                                <div class="text-red-400 font-bold">${noiseCount}</div>
                                <div class="text-slate-500">Noise</div>
                            </div>
                            <div class="text-center">
                                <div class="text-green-400 font-bold">${progressCount}</div>
                                <div class="text-slate-500">Progress</div>
                            </div>
                            <div class="text-center">
                                <div class="text-slate-400 font-bold">${neutralCount}</div>
                                <div class="text-slate-500">Neutral</div>
                            </div>
                        </div>
                    </div>
                    
                    <div class="text-xs text-slate-500 text-center pt-2 border-t border-slate-800">
                        <div>Cell ID: <span class="font-mono">${h3Index}</span></div>
                        <div class="mt-1">Updated: ${cellData.last_updated ? new Date(cellData.last_updated).toLocaleString() : '—'}</div>
                    </div>
                </div>
            </div>
        `)
        .addTo(state.map);
}

/**
 * Fit map to data bounds
 */
function fitMapToBounds() {
    if (!state.coreGrid || !state.coreGrid.features.length) return;

    const bounds = new maplibregl.LngLatBounds();

    state.coreGrid.features.forEach(feature => {
        feature.geometry.coordinates[0].forEach(coord => {
            bounds.extend(coord);
        });
    });

    state.map.fitBounds(bounds, {
        padding: { top: 100, bottom: 100, left: 100, right: 100 },
        duration: 1500,
        maxZoom: 3
    });

    console.log('🎯 Fitted map to bounds');
}

// ============================================================================
// MAP UPDATES
// ============================================================================

/**
 * Update map colors based on current view mode
 */
function updateMapColors() {
    if (!state.map || !state.map.getSource('h3-grid') || !state.vibeScores) {
        console.warn('⚠️  Cannot update colors: map not ready');
        return;
    }

    console.log('🎨 Updating colors for mode:', state.currentViewMode);

    const vizLookup = createVisualizationLookup();
    const colorScheme = getColorScheme();

    state.map.setPaintProperty('vibe-fill', 'fill-color', [
        'case',
        ['has', ['get', 'h3_index'], ['literal', vizLookup]],
        [
            'interpolate',
            ['linear'],
            ['get', ['get', 'h3_index'], ['literal', vizLookup]],
            ...colorScheme
        ],
        '#1e293b'
    ]);

    updateStats();
    updateLegend();
}

/**
 * Refresh vibe scores from server
 */
async function refreshVibeScores() {
    const refreshIcon = document.getElementById('refreshIcon');
    const refreshIconMobile = document.getElementById('refreshIconMobile');
    const refreshBtn = document.getElementById('btnRefresh');
    const refreshBtnMobile = document.getElementById('btnRefreshMobile');

    if (refreshIcon) {
        refreshIcon.classList.add('animate-spin');
    }
    if (refreshIconMobile) {
        refreshIconMobile.classList.add('animate-spin');
    }
    if (refreshBtn) {
        refreshBtn.disabled = true;
    }
    if (refreshBtnMobile) {
        refreshBtnMobile.disabled = true;
    }

    try {
        state.vibeScores = await loadVibeScores();
        updateMapColors();
        updateStats();
        updateGlobalPulse();
        updateInsights();

    } catch (error) {
        console.error('❌ Refresh failed:', error);
    } finally {
        if (refreshIcon) {
            refreshIcon.classList.remove('animate-spin');
        }
        if (refreshIconMobile) {
            refreshIconMobile.classList.remove('animate-spin');
        }
        if (refreshBtn) {
            refreshBtn.disabled = false;
        }
        if (refreshBtnMobile) {
            refreshBtnMobile.disabled = false;
        }
    }
}

// ============================================================================
// UI UPDATES
// ============================================================================

/**
 * Update global pulse ticker (both desktop and mobile)
 */
function updateGlobalPulse() {
    if (!state.pulse) return;

    // Desktop elements
    const pulseProgress = document.getElementById('pulseProgress');
    const pulseNoise = document.getElementById('pulseNoise');
    const pulseRatio = document.getElementById('pulseRatio');

    // Mobile elements
    const pulseProgressMobile = document.getElementById('pulseProgressMobile');
    const pulseNoiseMobile = document.getElementById('pulseNoiseMobile');
    const pulseRatioMobile = document.getElementById('pulseRatioMobile');

    const progressVal = state.pulse.progress_signal.toLocaleString();
    const noiseVal = state.pulse.noise_signal.toLocaleString();
    const ratioVal = state.pulse.humanity_ratio.toFixed(2);

    if (pulseProgress) pulseProgress.textContent = progressVal;
    if (pulseNoise) pulseNoise.textContent = noiseVal;
    if (pulseProgressMobile) pulseProgressMobile.textContent = progressVal;
    if (pulseNoiseMobile) pulseNoiseMobile.textContent = noiseVal;

    // Update ratio with color coding
    const ratio = state.pulse.humanity_ratio;
    let ratioClass = '';

    if (ratio > 1) {
        ratioClass = 'font-bold text-green-400';
    } else if (ratio > 0.5) {
        ratioClass = 'font-bold text-yellow-400';
    } else {
        ratioClass = 'font-bold text-red-400';
    }

    if (pulseRatio) {
        pulseRatio.textContent = ratioVal;
        pulseRatio.className = 'text-lg ' + ratioClass;
    }
    if (pulseRatioMobile) {
        pulseRatioMobile.textContent = ratioVal;
        pulseRatioMobile.className = ratioClass;
    }
}

/**
 * Update global insights panel
 */
function updateInsights() {
    if (!state.insights || state.insights.length === 0) return;

    const insightsContainer = document.getElementById('insightsList');
    if (!insightsContainer) return;

    insightsContainer.innerHTML = state.insights.map((insight, index) => `
        <a href="${insight.url}" target="_blank" rel="noopener noreferrer"
           class="block bg-slate-800/50 hover:bg-slate-800 rounded-lg p-3 transition-colors border border-slate-700 hover:border-green-500/50">
            <div class="flex items-start gap-2">
                <div class="text-green-400 font-bold text-xs mt-1">#${index + 1}</div>
                <div class="flex-1 text-sm text-slate-200 line-clamp-3">
                    ${insight.headline || 'Untitled Story'}
                </div>
            </div>
        </a>
    `).join('');
}

/**
 * Update stats panel (both desktop and mobile)
 */
function updateStats() {
    if (!state.vibeScores || !state.coreGrid) return;

    const totalRegions = Object.keys(state.vibeScores.cells).length;

    let totalEvents = 0;
    let totalVibe = 0;
    let totalNoise = 0;
    let totalProgress = 0;

    for (const cellData of Object.values(state.vibeScores.cells)) {
        totalVibe += cellData.vibe;
        totalEvents += cellData.count || 0;
        totalNoise += cellData.noise_count || 0;
        totalProgress += cellData.progress_count || 0;
    }

    const avgVibe = totalVibe / totalRegions;

    console.log('📊 Updating stats:', {
        totalRegions,
        totalEvents,
        avgVibe: avgVibe.toFixed(3),
        totalNoise,
        totalProgress
    });

    // Desktop stats
    const statRegions = document.getElementById('statRegions');
    const statEvents = document.getElementById('statEvents');
    const statAvgVibe = document.getElementById('statAvgVibe');
    const statAvgVibeSide = document.getElementById('statAvgVibeSide');
    const statNoise = document.getElementById('statNoise');
    const statProgress = document.getElementById('statProgress');

    // Mobile stats
    const statRegionsMobile = document.getElementById('statRegionsMobile');

    if (statRegions) {
        statRegions.textContent = totalRegions.toLocaleString();
        console.log('✅ Updated desktop statRegions:', statRegions.textContent);
    } else {
        console.warn('⚠️  statRegions element not found');
    }

    if (statEvents) {
        statEvents.textContent = totalEvents.toLocaleString();
        console.log('✅ Updated desktop statEvents:', statEvents.textContent);
    } else {
        console.warn('⚠️  statEvents element not found');
    }

    if (statAvgVibe) statAvgVibe.textContent = avgVibe.toFixed(3);
    if (statAvgVibeSide) statAvgVibeSide.textContent = avgVibe.toFixed(3);
    if (statNoise) statNoise.textContent = totalNoise.toLocaleString();
    if (statProgress) statProgress.textContent = totalProgress.toLocaleString();
    if (statRegionsMobile) {
        statRegionsMobile.textContent = totalRegions.toLocaleString();
        console.log('✅ Updated mobile statRegionsMobile:', statRegionsMobile.textContent);
    }

    const lastUpdated = document.getElementById('lastUpdated');
    if (lastUpdated && state.vibeScores.generated_at) {
        const date = new Date(state.vibeScores.generated_at);
        lastUpdated.textContent = date.toLocaleTimeString();
    }

    console.log('✅ Stats update complete');
}

/**
 * Update legend based on current view mode
 */
function updateLegend() {
    const legendContainer = document.getElementById('legendItems');
    if (!legendContainer) return;

    let legendHTML = '';

    switch (state.currentViewMode) {
        case 'progress':
            legendHTML = `
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 rounded bg-slate-800"></div>
                    <span class="text-slate-300">No Progress</span>
                </div>
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 rounded bg-green-500"></div>
                    <span class="text-slate-300">Low Intensity</span>
                </div>
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 rounded bg-emerald-500"></div>
                    <span class="text-slate-300">Medium Intensity</span>
                </div>
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 rounded bg-cyan-500"></div>
                    <span class="text-slate-300">High Intensity</span>
                </div>
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 rounded bg-sky-500"></div>
                    <span class="text-slate-300">Very High</span>
                </div>
            `;
            break;
        case 'noise':
            legendHTML = `
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 rounded bg-slate-800"></div>
                    <span class="text-slate-300">No Noise</span>
                </div>
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 rounded bg-amber-400"></div>
                    <span class="text-slate-300">Low Intensity</span>
                </div>
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 rounded bg-orange-500"></div>
                    <span class="text-slate-300">Medium Intensity</span>
                </div>
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 rounded bg-red-500"></div>
                    <span class="text-slate-300">High Intensity</span>
                </div>
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 rounded bg-red-900"></div>
                    <span class="text-slate-300">Very High</span>
                </div>
            `;
            break;
        case 'balanced':
        default:
            legendHTML = `
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 rounded bg-red-500"></div>
                    <span class="text-slate-300">Crisis (-1.0 to -0.5)</span>
                </div>
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 rounded bg-orange-500"></div>
                    <span class="text-slate-300">Tense (-0.5 to 0.0)</span>
                </div>
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 rounded bg-yellow-400"></div>
                    <span class="text-slate-300">Neutral (0.0 to 0.3)</span>
                </div>
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 rounded bg-lime-500"></div>
                    <span class="text-slate-300">Stable (0.3 to 0.6)</span>
                </div>
                <div class="flex items-center gap-2">
                    <div class="w-4 h-4 rounded bg-green-500"></div>
                    <span class="text-slate-300">Thriving (0.6 to 1.0)</span>
                </div>
            `;
            break;
    }

    legendContainer.innerHTML = legendHTML;
}

/**
 * Update active view button (both desktop and mobile)
 */
function updateActiveButton(mode) {
    // Desktop buttons
    const btnBalanced = document.getElementById('btnBalancedView');
    const btnProgress = document.getElementById('btnProgressView');
    const btnNoise = document.getElementById('btnNoiseView');

    // Mobile buttons
    const btnBalancedMobile = document.getElementById('btnBalancedViewMobile');
    const btnProgressMobile = document.getElementById('btnProgressViewMobile');
    const btnNoiseMobile = document.getElementById('btnNoiseViewMobile');

    const activeClass = 'px-4 py-2 rounded-lg font-medium text-sm transition-all bg-gradient-to-r from-green-500 to-blue-500 text-white shadow-lg';
    const inactiveClass = 'px-4 py-2 rounded-lg font-medium text-sm transition-all text-slate-400 hover:text-white hover:bg-slate-700/50';

    const activeClassMobile = 'flex-1 px-3 py-2 rounded-md font-medium text-xs transition-all bg-gradient-to-r from-green-500 to-blue-500 text-white';
    const inactiveClassMobile = 'flex-1 px-3 py-2 rounded-md font-medium text-xs transition-all text-slate-400 bg-slate-800/50';

    // Reset all buttons
    if (btnBalanced) btnBalanced.className = mode === 'balanced' ? activeClass : inactiveClass;
    if (btnProgress) btnProgress.className = mode === 'progress' ? activeClass : inactiveClass;
    if (btnNoise) btnNoise.className = mode === 'noise' ? activeClass : inactiveClass;

    if (btnBalancedMobile) btnBalancedMobile.className = mode === 'balanced' ? activeClassMobile : inactiveClassMobile;
    if (btnProgressMobile) btnProgressMobile.className = mode === 'progress' ? activeClassMobile : inactiveClassMobile;
    if (btnNoiseMobile) btnNoiseMobile.className = mode === 'noise' ? activeClassMobile : inactiveClassMobile;
}

/**
 * Update loading screen text
 */
function updateLoadingText(text, subtext = '') {
    const loadingText = document.getElementById('loadingText');
    const loadingSubtext = document.getElementById('loadingSubtext');

    if (loadingText) loadingText.textContent = text;
    if (loadingSubtext) loadingSubtext.textContent = subtext;
}

/**
 * Show error screen
 */
function showError(message, details = '') {
    const errorScreen = document.getElementById('errorScreen');
    const errorMessage = document.getElementById('errorMessage');
    const errorDetails = document.getElementById('errorDetails');
    const loadingScreen = document.getElementById('loadingScreen');

    if (loadingScreen) loadingScreen.classList.add('hidden');
    if (errorScreen) errorScreen.classList.remove('hidden');
    if (errorMessage) errorMessage.textContent = message;
    if (errorDetails) errorDetails.textContent = details;
}

/**
 * Hide loading screen
 */
function hideLoadingScreen() {
    const loadingScreen = document.getElementById('loadingScreen');
    if (loadingScreen) {
        setTimeout(() => {
            loadingScreen.classList.add('hidden');
        }, 500);
    }
}
// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

/**
 * Get color for vibe score
 */
function getVibeColor(vibe) {
    if (vibe < -0.5) return '#ef4444';
    if (vibe < 0.0) return '#f97316';
    if (vibe < 0.3) return '#eab308';
    if (vibe < 0.6) return '#84cc16';
    return '#22c55e';
}

/**
 * Get label for vibe score
 */
function getVibeLabel(vibe) {
    if (vibe < -0.5) return 'Crisis';
    if (vibe < 0.0) return 'Tense';
    if (vibe < 0.3) return 'Neutral';
    if (vibe < 0.6) return 'Stable';
    return 'Thriving';
}

// ============================================================================
// EVENT HANDLERS
// ============================================================================

/**
 * Setup all event listeners
 */
function setupEventListeners() {
    // Panel toggle buttons
    const btnTogglePanel = document.getElementById('btnTogglePanel');
    const btnClosePanel = document.getElementById('btnClosePanel');
    const panelBackdrop = document.getElementById('panelBackdrop');

    if (btnTogglePanel) {
        btnTogglePanel.addEventListener('click', toggleSidePanel);
    }

    if (btnClosePanel) {
        btnClosePanel.addEventListener('click', closeSidePanel);
    }

    if (panelBackdrop) {
        panelBackdrop.addEventListener('click', closeSidePanel);
    }

    // Desktop view toggle buttons
    const btnBalancedView = document.getElementById('btnBalancedView');
    const btnProgressView = document.getElementById('btnProgressView');
    const btnNoiseView = document.getElementById('btnNoiseView');

    if (btnBalancedView) {
        btnBalancedView.addEventListener('click', () => {
            state.currentViewMode = 'balanced';
            updateMapColors();
            updateActiveButton('balanced');
        });
    }

    if (btnProgressView) {
        btnProgressView.addEventListener('click', () => {
            state.currentViewMode = 'progress';
            updateMapColors();
            updateActiveButton('progress');
        });
    }

    if (btnNoiseView) {
        btnNoiseView.addEventListener('click', () => {
            state.currentViewMode = 'noise';
            updateMapColors();
            updateActiveButton('noise');
        });
    }

    // Mobile view toggle buttons
    const btnBalancedViewMobile = document.getElementById('btnBalancedViewMobile');
    const btnProgressViewMobile = document.getElementById('btnProgressViewMobile');
    const btnNoiseViewMobile = document.getElementById('btnNoiseViewMobile');

    if (btnBalancedViewMobile) {
        btnBalancedViewMobile.addEventListener('click', () => {
            state.currentViewMode = 'balanced';
            updateMapColors();
            updateActiveButton('balanced');
        });
    }

    if (btnProgressViewMobile) {
        btnProgressViewMobile.addEventListener('click', () => {
            state.currentViewMode = 'progress';
            updateMapColors();
            updateActiveButton('progress');
        });
    }

    if (btnNoiseViewMobile) {
        btnNoiseViewMobile.addEventListener('click', () => {
            state.currentViewMode = 'noise';
            updateMapColors();
            updateActiveButton('noise');
        });
    }

    // Refresh buttons (desktop and mobile)
    const btnRefresh = document.getElementById('btnRefresh');
    const btnRefreshMobile = document.getElementById('btnRefreshMobile');

    if (btnRefresh) {
        btnRefresh.addEventListener('click', refreshVibeScores);
    }

    if (btnRefreshMobile) {
        btnRefreshMobile.addEventListener('click', refreshVibeScores);
    }

    // Retry button
    const btnRetry = document.getElementById('btnRetry');
    if (btnRetry) {
        btnRetry.addEventListener('click', () => {
            window.location.reload();
        });
    }

    console.log('✅ Event listeners setup');
}

// ============================================================================
// INITIALIZATION
// ============================================================================

/**
 * Main initialization function
 */
async function initialize() {
    console.log('🚀 AIC initializing...');

    try {
        await initializeMap();
        setupEventListeners();

        await new Promise(resolve => {
            state.map.on('load', resolve);
        });

        console.log('🗺️  Map loaded, fetching data...');

        await loadAllData();

        addMapSources();
        addMapLayers();
        addMapInteractions();
        fitMapToBounds();

        updateStats();
        updateGlobalPulse();
        updateInsights();
        updateLegend();
        updateActiveButton('balanced');

        hideLoadingScreen();

        console.log('✅ AIC initialized successfully!');

    } catch (error) {
        console.error('❌ Initialization failed:', error);
        showError(
            'Failed to initialize AIC. This could be due to missing data files or network issues.',
            error.stack || error.message
        );
    }
}

// Start the app
if (document.readyState === 'complete') {
    initialize();
} else {
    window.addEventListener('load', initialize);
}

// Auto-refresh every 8 hours
setInterval(refreshVibeScores, 8 * 60 * 60 * 1000);

console.log('🎯 AIC script loaded');