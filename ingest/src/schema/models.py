from pydantic import BaseModel, Field
from typing import Literal, List, Optional
from datetime import datetime

class RawEvent(BaseModel):
    id: str
    headline: str  # Still empty for GKG, but see tip below
    source_type: Literal["gdelt", "owid", "good_news"]
    category: Literal["noise", "progress", "neutral"] # New: for the toggle logic
    tone: float 
    polarity: float # New: measures "Loudness/Noise"
    p_weight: float = 0.0
    n_weight: float = 0.0
    lat: float
    lng: float
    h3_index: str
    location_name: Optional[str]
    timestamp: datetime

class H3Cell(BaseModel):
    """Aggregated vibe for a single hexagon"""
    h3_index: str
    avg_tone: float  # Average GDELT tone
    progress_score: float  # From OWID (0-100 scale)
    good_news_count: int  # Number of positive stories
    final_vibe: float  # The weighted score
    event_count: int
    last_updated: datetime

class MapState(BaseModel):
    """The entire map export (GeoJSON compatible)"""
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: list[dict]  # Each H3 cell as a GeoJSON feature
    metadata: dict  # e.g., {"generated_at": "2025-02-04T10:00:00Z"}


class GlobalPulse(BaseModel):
    progress_signal: int
    noise_signal: int
    humanity_ratio: float

class GlobalInsight(BaseModel):
    headline: str
    url: str

class H3Cell(BaseModel):
    h3_index: str
    centroid_lat: float
    centroid_lng: float
    vibe: float
    top_progress_headline: str
    top_noise_headline: str
    p_intensity: float
    n_intensity: float
    avg_tone: float
    avg_polarity: float
    noise_count: int
    progress_count: int
    neutral_count: int
    total_events: int
    headline_sample: str
    location_sample: str
    last_updated: str

class AggregateResult(BaseModel):
    pulse: GlobalPulse
    insights: List[GlobalInsight]
    cells: List[H3Cell]

class GKGTone(BaseModel):
    avg_tone: float
    positive_score: float
    negative_score: float
    polarity: float
    activity_density: Optional[float]
    self_density: Optional[float]
    word_count: Optional[int]

class GKGLocation(BaseModel):
    location_type: int
    name: str
    country_code: str
    adm1_code: Optional[str]
    lat: float
    lng: float
    feature_id: Optional[str]

class GKGTheme(BaseModel):
    code: str

class ParsedGKGRecord(BaseModel):
    # Identity
    record_id: str
    document_url: str
    source_domain: Optional[str]

    # Time
    published_at: datetime

    # Content
    headline: Optional[str]

    # Semantics
    themes: List[GKGTheme] = []
    tone: GKGTone

    # Geography
    primary_location: GKGLocation

    # Entities
    persons: List[str] = []
    organizations: List[str] = []

class GKGRecord(BaseModel):
    # ------------------------------------------------------------------
    # Field 0 — GKGRECORDID
    # Format: YYYYMMDDHHMMSS-<rowid>
    # Meaning: Unique identifier for this GKG row (not globally stable across reruns)
    # Parsing: Treat as opaque string; useful for debugging or deduplication
    gkg_record_id: str

    # ------------------------------------------------------------------
    # Field 1 — DATE
    # Format: YYYYMMDDHHMMSS (UTC)
    # Meaning: Publication timestamp of the document
    # Parsing: Convert to datetime for event time (preferred over ingestion time)
    date: str

    # ------------------------------------------------------------------
    # Field 2 — SourceCollectionIdentifier
    # Format: Integer (e.g., 1 = Web, 2 = Broadcast)
    # Meaning: Identifies the source collection type
    # Parsing: Optional; safe to ignore unless filtering by media type
    source_collection_id: Optional[int]

    # ------------------------------------------------------------------
    # Field 3 — SourceCommonName
    # Format: Domain name (lowercase)
    # Meaning: Normalized source domain of the document
    # Parsing: Useful for attribution or trust scoring
    source_common_name: Optional[str]

    # ------------------------------------------------------------------
    # Field 4 — DocumentIdentifier
    # Format: URL
    # Meaning: Canonical URL of the source document
    # Parsing: Required for most applications; skip row if empty
    document_identifier: Optional[str]

    # ------------------------------------------------------------------
    # Field 5 — Counts
    # Format: COUNT_TYPE#OBJECT#COUNT#CHAROFFSET;...
    # Meaning: Frequency counts of named entities or concepts
    # Parsing: Rarely populated; typically ignored
    counts: Optional[str]

    # ------------------------------------------------------------------
    # Field 6 — EnhancedCounts
    # Format: Same as Counts, but richer and more granular
    # Meaning: Enhanced frequency metadata
    # Parsing: Often whitespace-filled; ignore unless doing deep NLP
    enhanced_counts: Optional[str]

    # ------------------------------------------------------------------
    # Field 7 — V2Themes
    # Format: THEME;THEME;THEME;
    # Meaning: High-level topical tags assigned by GDELT
    # Parsing: Split on ';' and drop empty entries
    v2_themes: Optional[str]

    # ------------------------------------------------------------------
    # Field 8 — V2EnhancedThemes
    # Format: THEME,CHAROFFSET;THEME,CHAROFFSET;
    # Meaning: Themes with positional offsets in the document text
    # Parsing: Optional; useful only for positional analysis
    v2_enhanced_themes: Optional[str]

    # ------------------------------------------------------------------
    # Field 9 — V2Locations
    # Format: TYPE#NAME#COUNTRY#ADM1#LAT#LON#FEATUREID;
    # Meaning: Geographic references detected in the document
    # Parsing: Commonly only the first entry is used as "primary" location
    v2_locations: Optional[str]

    # ------------------------------------------------------------------
    # Field 10 — V2EnhancedLocations
    # Format: Same as V2Locations plus confidence and offsets
    # Meaning: Higher-fidelity geolocation data
    # Parsing: Ignore unless you need confidence-weighted geocoding
    v2_enhanced_locations: Optional[str]

    # ------------------------------------------------------------------
    # Field 11 — V2Persons
    # Format: name;name;name;
    # Meaning: Person names mentioned in the document
    # Parsing: Split on ';', lowercase/normalize if needed
    v2_persons: Optional[str]

    # ------------------------------------------------------------------
    # Field 12 — V2Organizations
    # Format: org,CHAROFFSET;org,CHAROFFSET;
    # Meaning: Organization names mentioned in the document
    # Parsing: Offsets are optional; often stripped
    v2_organizations: Optional[str]

    # ------------------------------------------------------------------
    # Field 13 — V2EnhancedPersons
    # Format: person,CHAROFFSET;
    # Meaning: Enhanced person entity metadata
    # Parsing: Rarely populated; safe to ignore
    v2_enhanced_persons: Optional[str]

    # ------------------------------------------------------------------
    # Field 14 — V2Tone
    # Format:
    #   AvgTone,PosScore,NegScore,Polarity,
    #   ActivityDensity,SelfDensity,WordCount
    # Meaning: Overall emotional and tonal sentiment of the document
    # Parsing: AvgTone and Polarity are most commonly used
    v2_tone: Optional[str]

    # ------------------------------------------------------------------
    # Field 15 — V2RelativeTone
    # Format: Complex relational tone encoding
    # Meaning: Tone of relationships between entities
    # Parsing: Advanced use only; ignore for most pipelines
    v2_relative_tone: Optional[str]

    # ------------------------------------------------------------------
    # Field 16 — V2GCAM
    # Format: wc:<n>,c1.2:<n>,...,v20.1:<float>,...
    # Meaning: Full GCAM linguistic feature vector
    # Parsing: Extremely large; ignore unless doing ML on text features
    v2_gcam: Optional[str]

    # ------------------------------------------------------------------
    # Field 17 — SharingImage
    # Format: URL
    # Meaning: Social media preview image for the document
    # Parsing: Safe to store directly
    sharing_image: Optional[str]

    # ------------------------------------------------------------------
    # Field 18 — RelatedImages
    # Format: URL;URL;URL;
    # Meaning: Additional images associated with the document
    # Parsing: Optional enrichment
    related_images: Optional[str]

    # ------------------------------------------------------------------
    # Field 19 — SocialImageEmbeds
    # Format: HTML or URL references
    # Meaning: Inline social image embeds
    # Parsing: Rarely useful
    social_image_embeds: Optional[str]

    # ------------------------------------------------------------------
    # Field 20 — SocialVideoEmbeds
    # Format: HTML or URL references
    # Meaning: Inline video embeds
    # Parsing: Rarely useful
    social_video_embeds: Optional[str]

    # ------------------------------------------------------------------
    # Field 21 — YouTubeVideoEmbeds
    # Format: https://youtube.com/embed/...;
    # Meaning: Embedded YouTube videos
    # Parsing: Split on ';' if needed
    youtube_video_embeds: Optional[str]

    # ------------------------------------------------------------------
    # Field 22 — SocialAudioEmbeds
    # Format: URL or HTML references
    # Meaning: Embedded audio or podcast references
    # Parsing: Very rare
    social_audio_embeds: Optional[str]

    # ------------------------------------------------------------------
    # Field 23 — V2Mentions
    # Format: entity,CHAROFFSET;entity,CHAROFFSET;
    # Meaning: Mentions of key entities in the text
    # Parsing: Used for salience analysis
    v2_mentions: Optional[str]

    # ------------------------------------------------------------------
    # Field 24 — V2EnhancedMentions
    # Format: entity,confidence,offset;...
    # Meaning: Enhanced mention metadata
    # Parsing: Optional; advanced NLP use
    v2_enhanced_mentions: Optional[str]

    # ------------------------------------------------------------------
    # Field 25 — V2ToneByLocation
    # Format: Location-scoped tone encodings
    # Meaning: Sentiment broken down by geographic reference
    # Parsing: Rarely used
    v2_tone_by_location: Optional[str]

    # ------------------------------------------------------------------
    # Field 26 — ExtrasXML
    # Format: XML fragment (not a full document)
    # Meaning: Misc metadata such as PAGE_TITLE and PAGE_LINKS
    # Parsing: Extract PAGE_TITLE for headline
    extras_xml: Optional[str]
