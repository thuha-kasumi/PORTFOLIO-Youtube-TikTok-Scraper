"""
YouTube & TikTok Content Scraper - Production Version
======================================================
Integrated platform scraper supporting:
- YouTube (official API, quota tracking)
- TikTok (pyktok browser automation, free)

Features:
- Target-based comment collection
- Incremental saving to database
- Video popularity filters (views, date, comment count)
- Optional comment limit per video
- Duplicate detection and skipping
- Session-based resume capability
"""

import re
import uuid
import time
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple, Callable

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import NoTranscriptFound, TranscriptsDisabled
from tenacity import retry, stop_after_attempt, wait_exponential

# Try importing TikTok scraper (optional dependency)
try:
    import pyktok as pyk
    PYKTOK_AVAILABLE = True
except ImportError:
    PYKTOK_AVAILABLE = False

# ── Constants ────────────────────────────────────────────────────────────────
PLATFORM_YOUTUBE = "YouTube"
PLATFORM_TIKTOK = "TikTok"

# Sheet names
VIDEOS_SHEET      = "videos"
TRANSCRIPTS_SHEET = "transcripts"
COMMENTS_SHEET    = "comments"
CHANNELS_SHEET    = "channels"
LOG_SHEET         = "run_log"

# YouTube API Quota Costs (units per call)
QUOTA_COSTS = {
    "search": 100,      # search.list
    "videos": 1,        # videos.list (per 50 videos)
    "comments": 1,      # commentThreads.list (per 100 comments)
    "channels": 1,      # channels.list (per channel)
}


# ═══════════════════════════════════════════════════════════════════════════════
# Utility helpers
# ═══════════════════════════════════════════════════════════════════════════════

def normalize(text: str) -> str:
    if not text:
        return ""
    return str(text).strip().lower()


def all_keywords_present(text: str, keywords_str: str) -> bool:
    """Return True when every comma-separated keyword appears in text (AND logic)."""
    if not keywords_str.strip():
        return True
    kws = [k.strip().lower() for k in keywords_str.split(",") if k.strip()]
    lowered = normalize(text)
    return all(kw in lowered for kw in kws)


def update_quota_tracker(cost: int):
    """Track YouTube API quota usage in session state."""
    if "quota_used_today" not in st.session_state:
        st.session_state["quota_used_today"] = 0
    st.session_state["quota_used_today"] += cost


def check_quota_and_warn(additional_cost: int = 0) -> bool:
    """Check if adding additional_cost would exceed daily quota. Returns True if safe."""
    current = st.session_state.get("quota_used_today", 0)
    if current + additional_cost >= 9500:
        st.warning(f"⚠️ Approaching quota limit: {current}/10,000 units used.")
    if current + additional_cost >= 10000:
        st.error("❌ Daily YouTube API quota would be exceeded.")
        return False
    return True


def get_db_engine() -> Engine:
    """Create the shared data connection.

    Recommended for collaboration: set DATABASE_URL in Streamlit secrets to a hosted
    PostgreSQL URL, for example:
    postgresql+psycopg2://USER:PASSWORD@HOST:5432/DBNAME

    Local fallback: SQLite. This is useful for testing, but not ideal on Streamlit
    Cloud because the file is not a durable shared database.
    """
    database_url = st.secrets.get("DATABASE_URL", "sqlite:///scraper_local.db")
    return create_engine(database_url, pool_pre_ping=True)


def _table_exists(engine: Engine, table_name: str) -> bool:
    return inspect(engine).has_table(table_name)


def _quote_identifier(name: str) -> str:
    # The table/column names in this app are controlled constants, but quote anyway.
    return '"' + name.replace('"', '""') + '"'


def _add_missing_columns(engine: Engine, table_name: str, df: pd.DataFrame) -> None:
    """Allow the schema to evolve when new columns are added to the app."""
    if not _table_exists(engine, table_name):
        return

    existing_cols = {col["name"] for col in inspect(engine).get_columns(table_name)}
    missing_cols = [c for c in df.columns if c not in existing_cols]
    if not missing_cols:
        return

    dialect = engine.dialect.name
    with engine.begin() as conn:
        for col in missing_cols:
            if dialect == "postgresql":
                conn.execute(text(f'ALTER TABLE {_quote_identifier(table_name)} ADD COLUMN {_quote_identifier(col)} TEXT'))
            else:
                conn.execute(text(f'ALTER TABLE {_quote_identifier(table_name)} ADD COLUMN {_quote_identifier(col)} TEXT'))


def read_table_safe(engine: Engine, table_name: str) -> pd.DataFrame:
    """Safely read a table, returning an empty DataFrame on error."""
    try:
        if not _table_exists(engine, table_name):
            return pd.DataFrame()
        return pd.read_sql_table(table_name, engine)
    except Exception as exc:
        st.warning(f"Could not read table '{table_name}': {exc}")
        return pd.DataFrame()


def append_unique_rows(
    engine: Engine,
    table_name: str,
    new_df: pd.DataFrame,
    key_col: str,
) -> Tuple[int, int]:
    """Append rows whose key_col value is not already in the database table."""
    if new_df.empty:
        return 0, 0

    new_df = new_df.copy()
    if key_col not in new_df.columns:
        raise ValueError(f"Missing key column '{key_col}' in data for table '{table_name}'.")

    # Store flexible scraped data safely as text/object values.
    for col in new_df.columns:
        new_df[col] = new_df[col].where(pd.notna(new_df[col]), None)

    if not _table_exists(engine, table_name):
        new_df.to_sql(table_name, engine, if_exists="replace", index=False)
        with engine.begin() as conn:
            try:
                conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_{key_col} ON {_quote_identifier(table_name)} ({_quote_identifier(key_col)})'))
            except Exception:
                pass
        return len(new_df), 0

    _add_missing_columns(engine, table_name, new_df)

    try:
        existing_keys_df = pd.read_sql_query(
            f'SELECT {_quote_identifier(key_col)} FROM {_quote_identifier(table_name)}',
            engine,
        )
        existing_keys = set(existing_keys_df[key_col].astype(str).fillna("").tolist())
    except Exception:
        existing_keys = set()

    to_save = new_df[~new_df[key_col].astype(str).isin(existing_keys)].copy()
    duplicates_skipped = len(new_df) - len(to_save)

    if not to_save.empty:
        to_save.to_sql(table_name, engine, if_exists="append", index=False)

    return len(to_save), duplicates_skipped


def log_run(engine: Engine, payload: Dict) -> None:
    """Log a scrape run to the run_log table."""
    append_unique_rows(engine, LOG_SHEET, pd.DataFrame([payload]), key_col="run_id")


def get_existing_comment_ids(engine: Engine) -> set:
    """Get set of already-saved comment IDs for deduplication."""
    try:
        if not _table_exists(engine, COMMENTS_SHEET):
            return set()
        df = pd.read_sql_query(
            f'SELECT comment_id FROM {_quote_identifier(COMMENTS_SHEET)}',
            engine,
        )
        return set(df["comment_id"].astype(str).fillna("").tolist())
    except Exception:
        return set()


def get_recently_scraped_video_ids(engine: Engine, days: int = 7) -> set:
    """Get video IDs scraped in the last N days to avoid re-processing."""
    try:
        if not _table_exists(engine, VIDEOS_SHEET):
            return set()

        df = pd.read_sql_query(
            f'SELECT video_id, scraped_at FROM {_quote_identifier(VIDEOS_SHEET)}',
            engine,
        )
        if df.empty or "scraped_at" not in df.columns:
            return set()

        cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
        scraped_at = pd.to_datetime(df["scraped_at"], errors="coerce")
        recent = df[scraped_at >= cutoff]
        return set(recent["video_id"].astype(str).fillna("").tolist())
    except Exception:
        return set()


# ═══════════════════════════════════════════════════════════════════════════════
# YouTube API Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def get_yt_client():
    """Build a YouTube Data API v3 client."""
    api_key = st.secrets["YOUTUBE_API_KEY"]
    return build("youtube", "v3", developerKey=api_key, cache_discovery=False)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def search_youtube_video_ids(yt, title_kw: str, channel_kw: str, max_results: int) -> List[str]:
    """Search YouTube and return video IDs. Costs 100 quota units."""
    if not check_quota_and_warn(QUOTA_COSTS["search"]):
        return []

    query_parts = [p.strip() for p in [title_kw, channel_kw] if p.strip()]
    query = " ".join(query_parts) if query_parts else "youtube"

    video_ids = []
    next_page_token = None

    try:
        while len(video_ids) < max_results:
            resp = yt.search().list(
                part="id",
                q=query,
                type="video",
                maxResults=min(50, max_results - len(video_ids)),
                pageToken=next_page_token,
            ).execute()

            for item in resp.get("items", []):
                if item["id"]["kind"] == "youtube#video":
                    video_ids.append(item["id"]["videoId"])

            next_page_token = resp.get("nextPageToken")
            if not next_page_token:
                break

        update_quota_tracker(QUOTA_COSTS["search"])
        return video_ids

    except HttpError as e:
        if "quotaExceeded" in str(e):
            st.error("⚠️ Daily YouTube API quota exceeded. Please try again tomorrow.")
        else:
            st.error(f"YouTube API error: {e}")
        return []


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_youtube_video_details(yt, video_ids: List[str]) -> List[Dict]:
    """Fetch metadata for YouTube videos. Costs 1 unit per 50 videos."""
    if not video_ids:
        return []

    batches = (len(video_ids) + 49) // 50
    total_cost = batches * QUOTA_COSTS["videos"]

    if not check_quota_and_warn(total_cost):
        return []

    results = []
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i: i + 50]
        try:
            resp = yt.videos().list(
                part="snippet,statistics",
                id=",".join(chunk),
            ).execute()
            update_quota_tracker(QUOTA_COSTS["videos"])

            for item in resp.get("items", []):
                snippet = item.get("snippet", {})
                stats = item.get("statistics", {})
                results.append({
                    "video_id": item["id"],
                    "title": snippet.get("title", ""),
                    "channel_name": snippet.get("channelTitle", ""),
                    "channel_id": snippet.get("channelId", ""),
                    "published_at": (snippet.get("publishedAt") or "")[:10],
                    "view_count": stats.get("viewCount", "0"),
                    "like_count": stats.get("likeCount", "0"),
                    "comment_count": stats.get("commentCount", "0"),
                    "description": snippet.get("description", ""),
                    "video_url": f"https://www.youtube.com/watch?v={item['id']}",
                    "platform": "youtube",
                    "_transcript": "",
                })
        except HttpError as e:
            st.warning(f"Could not fetch video batch: {e}")
            continue

    return results


def fetch_youtube_transcript(video_id: str) -> str:
    """Download YouTube transcript (FREE - no API cost)."""
    try:
        segments = YouTubeTranscriptApi.get_transcript(video_id)
        return " ".join(seg["text"] for seg in segments)
    except (TranscriptsDisabled, NoTranscriptFound):
        return ""
    except Exception:
        return ""


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_youtube_comments_incremental(
    yt, 
    video_id: str, 
    max_comments: int, 
    existing_ids: set,
    comment_kw: str = "",
    save_callback: Optional[Callable] = None
) -> Tuple[List[Dict], int]:
    """
    Fetch YouTube comments, skipping existing IDs.
    Returns (list_of_new_comments, quota_units_used).
    """
    comments = []
    next_page_token = None
    quota_units = 0
    pages_fetched = 0
    max_pages = (max_comments + 99) // 100

    while len(comments) < max_comments and pages_fetched < max_pages:
        if not check_quota_and_warn(QUOTA_COSTS["comments"]):
            break

        try:
            resp = yt.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=min(100, max_comments - len(comments)),
                pageToken=next_page_token,
                textFormat="plainText",
                order="relevance",
            ).execute()

            update_quota_tracker(QUOTA_COSTS["comments"])
            quota_units += QUOTA_COSTS["comments"]
            pages_fetched += 1

            for item in resp.get("items", []):
                top = item["snippet"]["topLevelComment"]["snippet"]
                comment_id = item["id"]
                
                # Skip if already exists
                if comment_id in existing_ids:
                    continue
                    
                comment_data = {
                    "comment_id": comment_id,
                    "comment_text": top.get("textDisplay", ""),
                    "author": top.get("authorDisplayName", ""),
                    "comment_published_at": (top.get("publishedAt") or "")[:10],
                    "comment_likes": top.get("likeCount", 0),
                    "video_id": video_id,
                }

                if comment_kw.strip() and not all_keywords_present(comment_data["comment_text"], comment_kw):
                    continue

                comments.append(comment_data)
                
                # Incremental save callback
                if save_callback and len(comments) % 100 == 0:
                    save_callback(comments[-100:])

            next_page_token = resp.get("nextPageToken")
            if not next_page_token:
                break

        except HttpError as e:
            if "commentsDisabled" in str(e) or "disabled" in str(e).lower():
                break
            elif "quotaExceeded" in str(e):
                st.error("Quota exceeded while fetching comments.")
            break

    return comments, quota_units


def fetch_youtube_channel_info(yt, channel_id: str) -> Dict:
    """Fetch YouTube channel info. Costs 1 quota unit."""
    if not check_quota_and_warn(QUOTA_COSTS["channels"]):
        return {}

    try:
        resp = yt.channels().list(
            part="snippet,statistics",
            id=channel_id,
        ).execute()
        update_quota_tracker(QUOTA_COSTS["channels"])

        items = resp.get("items", [])
        if not items:
            return {}
        ch = items[0]
        snippet = ch.get("snippet", {})
        stats = ch.get("statistics", {})
        pub = snippet.get("publishedAt") or ""
        return {
            "channel_id": channel_id,
            "channel_name": snippet.get("title", ""),
            "channel_url": f"https://www.youtube.com/channel/{channel_id}",
            "platform": "youtube",
            "country": snippet.get("country", ""),
            "established_date": pub[:10],
            "subscriber_count": stats.get("subscriberCount", "0"),
            "video_count": stats.get("videoCount", "0"),
            "total_views": stats.get("viewCount", "0"),
        }
    except HttpError:
        return {}


# ═══════════════════════════════════════════════════════════════════════════════
# TikTok Helpers
# ═══════════════════════════════════════════════════════════════════════════════

class TikTokScraper:
    """TikTok scraper using pyktok (browser automation, free)."""

    def __init__(self, headless: bool = True):
        if not PYKTOK_AVAILABLE:
            st.error("pyktok not installed. Run: pip install pyktok selenium")
            st.stop()
        
        try:
            pyk.specify_browser('firefox')
            self.headless = headless
        except Exception as e:
            st.warning(f"TikTok browser setup issue: {e}")
            st.info("TikTok scraping may not work. Ensure Firefox is installed.")

    def search_videos(self, keyword: str, max_results: int = 100) -> List[Dict]:
        """Search TikTok for videos matching keyword."""
        search_url = f"https://www.tiktok.com/search?q={keyword.replace(' ', '%20')}"
        
        try:
            video_urls = pyk.get_tiktok_video_urls(
                search_url,
                headless=self.headless,
                n_videos=max_results
            )
            
            results = []
            for url in video_urls[:max_results]:
                video_id = url.split('/')[-1].split('?')[0]
                results.append({
                    "video_id": video_id,
                    "video_url": url,
                    "keyword": keyword,
                })
            return results
        except Exception as e:
            st.warning(f"TikTok search error: {e}")
            return []

    def fetch_video_details(self, video_url: str) -> Dict:
        """Fetch metadata for a single TikTok video."""
        try:
            metadata = pyk.get_tiktok_metadata(
                video_url,
                headless=self.headless
            )
            
            return {
                "video_id": metadata.get('id', ''),
                "video_url": video_url,
                "title": metadata.get('desc', '')[:200],
                "channel_name": metadata.get('author', ''),
                "channel_id": metadata.get('author_id', ''),
                "published_at": metadata.get('create_time', '')[:10] if metadata.get('create_time') else '',
                "view_count": str(metadata.get('play_count', 0)),
                "like_count": str(metadata.get('digg_count', 0)),
                "comment_count": str(metadata.get('comment_count', 0)),
                "description": metadata.get('desc', ''),
                "platform": "tiktok",
                "_transcript": "",  # TikTok doesn't have transcripts
            }
        except Exception as e:
            return {}

    def fetch_comments_incremental(
        self,
        video_url: str,
        video_id: str,
        max_comments: int,
        existing_ids: set,
        comment_kw: str = "",
        save_callback: Optional[Callable] = None
    ) -> Tuple[List[Dict], int]:
        """Fetch TikTok comments, skipping existing IDs. Returns (comments, quota_used=0)."""
        comments = []
        
        try:
            comment_df = pyk.get_tiktok_comments(
                video_url,
                headless=self.headless,
                n_comments=max_comments
            )
            
            if comment_df is not None and not comment_df.empty:
                for _, row in comment_df.iterrows():
                    comment_id = str(row.get('comment_id', ''))
                    
                    if comment_id in existing_ids:
                        continue
                        
                    comment_data = {
                        "comment_id": comment_id,
                        "comment_text": row.get('text', ''),
                        "author": row.get('author', ''),
                        "comment_likes": row.get('digg_count', 0),
                        "comment_published_at": row.get('create_time', '')[:10] if row.get('create_time') else '',
                        "video_id": video_id,
                    }

                    if comment_kw.strip() and not all_keywords_present(comment_data["comment_text"], comment_kw):
                        continue

                    comments.append(comment_data)
                    
                    if save_callback and len(comments) % 100 == 0:
                        save_callback(comments[-100:])
                        
        except Exception as e:
            st.warning(f"Error fetching TikTok comments for {video_id}: {e}")
            
        return comments, 0  # TikTok has no quota cost

    def fetch_channel_info(self, channel_id: str) -> Dict:
        """Fetch TikTok creator/profile information."""
        profile_url = f"https://www.tiktok.com/@{channel_id}"
        
        try:
            profile_data = pyk.get_profile_data(
                profile_url,
                headless=self.headless
            )
            
            return {
                "channel_id": channel_id,
                "channel_name": profile_data.get('uniqueId', ''),
                "channel_url": profile_url,
                "platform": "tiktok",
                "follower_count": str(profile_data.get('followerCount', 0)),
                "following_count": str(profile_data.get('followingCount', 0)),
                "video_count": str(profile_data.get('videoCount', 0)),
                "heart_count": str(profile_data.get('heartCount', 0)),
                "bio": profile_data.get('bio', ''),
            }
        except Exception:
            return {}


# ═══════════════════════════════════════════════════════════════════════════════
# Common filtering logic
# ═══════════════════════════════════════════════════════════════════════════════

def apply_popularity_filters(
    candidates: List[Dict],
    publish_date_start: Optional[date],
    publish_date_end: Optional[date],
    min_views: int,
    min_comment_count: int,
) -> List[Dict]:
    """Apply date, view, and comment count filters to video candidates."""
    filtered = []
    
    for v in candidates:
        # Date filter
        if publish_date_start:
            try:
                pub_date = datetime.strptime(v['published_at'], '%Y-%m-%d').date()
                if pub_date < publish_date_start:
                    continue
            except (ValueError, TypeError):
                continue
                
        if publish_date_end:
            try:
                pub_date = datetime.strptime(v['published_at'], '%Y-%m-%d').date()
                if pub_date > publish_date_end:
                    continue
            except (ValueError, TypeError):
                continue
        
        # View filter
        try:
            if min_views and int(v.get('view_count', 0)) < min_views:
                continue
        except (ValueError, TypeError):
            pass
            
        # Comment count filter
        try:
            if min_comment_count and int(v.get('comment_count', 0)) < min_comment_count:
                continue
        except (ValueError, TypeError):
            pass
            
        filtered.append(v)
        
    return filtered


# ═══════════════════════════════════════════════════════════════════════════════
# Session-state initialisation
# ═══════════════════════════════════════════════════════════════════════════════

def init_state() -> None:
    for key in ["results", "search_params", "quota_used_today"]:
        if key not in st.session_state:
            st.session_state[key] = None if key != "quota_used_today" else 0


init_state()


# ═══════════════════════════════════════════════════════════════════════════════
# Page config & styling
# ═══════════════════════════════════════════════════════════════════════════════

st.set_page_config(page_title="Content Scraper (YouTube + TikTok)", layout="wide")

st.markdown(
    """
    <style>
    div[data-testid="stButton"] > button[kind="primary"] {
        background-color: #d32f2f;
        color: white;
        border: none;
    }
    div[data-testid="stButton"] > button[kind="primary"]:hover {
        background-color: #b71c1c;
        color: white;
    }
    .small-grey {
        color: #808080;
        font-size: 0.9rem;
        margin-top: -10px;
        margin-bottom: 8px;
    }
    .filter-note {
        color: #1565c0;
        font-size: 0.85rem;
        font-style: italic;
        margin-bottom: 6px;
    }
    .quota-warning {
        background-color: #fff3e0;
        padding: 10px;
        border-radius: 5px;
        border-left: 4px solid #ff9800;
        margin-bottom: 10px;
    }
    .info-box {
        background-color: #e3f2fd;
        padding: 10px;
        border-radius: 5px;
        border-left: 4px solid #2196f3;
        margin-bottom: 10px;
        font-size: 0.85rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("📺🎵 Content Scraper (YouTube + TikTok)")
st.caption("Search → filter → preview → save unique rows to database")

# ── Quota display (YouTube only) ───────────────────────────────────────────────
with st.sidebar:
    quota_remaining = 10000 - st.session_state.get("quota_used_today", 0)
    quota_color = "green" if quota_remaining > 2000 else "orange" if quota_remaining > 500 else "red"
    st.markdown(
        f'<div class="quota-warning">📊 <strong>YouTube API Quota Remaining</strong><br>'
        f'<span style="color:{quota_color}; font-size:24px; font-weight:bold;">{quota_remaining}</span> / 10,000 units<br>'
        f'<span style="font-size:12px;">TikTok has no quota limits 🎵</span></div>',
        unsafe_allow_html=True,
    )

conn = get_db_engine()


# ═══════════════════════════════════════════════════════════════════════════════
# Sidebar — search setup
# ═══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.header("🔍 Platform & Search")
    
    # Platform selector
    platform = st.radio(
        "Select Platform",
        [PLATFORM_YOUTUBE, PLATFORM_TIKTOK],
        horizontal=True,
        help="YouTube uses official API (has quota limits). TikTok uses browser automation (free, but slower)."
    )
    
    st.markdown("---")
    
    user_name = st.text_input("Your name *", placeholder="Enter your name")
    st.markdown('<div class="small-grey">Required — recorded in run log</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("🔎 Keyword Filters")
    st.markdown(
        '<div class="filter-note">All filled keywords act as AND conditions.<br>'
        "Leave blank to skip that filter.</div>",
        unsafe_allow_html=True,
    )

    title_kw = st.text_input("Title/Description contains", placeholder="e.g. machine learning")
    channel_kw = st.text_input("Channel/Author name contains", placeholder="e.g. TED")
    
    # Platform-specific filters
    if platform == PLATFORM_YOUTUBE:
        transcript_kw = st.text_input(
            "Transcript contains (YouTube only)", 
            placeholder="e.g. neural network",
            help="⚠️ Only applies to videos WITH transcripts. Videos without transcript are skipped when this filter is active."
        )
    else:
        transcript_kw = ""
        st.info("🎵 TikTok transcripts are not available. Use Title/Description filter instead.")
    
    comment_kw = st.text_input("Comments contain", placeholder="e.g. helpful (slower)")
    
    st.markdown("---")
    st.subheader("🎯 Video Popularity Filters")
    
    use_date_filter = st.checkbox(
        "Filter by publish date",
        value=False,
        help="Turn this on when you need videos from a specific time period."
    )

    col1, col2 = st.columns(2)
    with col1:
        if use_date_filter:
            publish_date_start = st.date_input("Published after", value=date(2024, 1, 1))
        else:
            publish_date_start = None
            st.text_input("Published after", value="Not used", disabled=True)
        min_views = st.number_input("Minimum views", min_value=0, value=0, step=1000)
    with col2:
        if use_date_filter:
            publish_date_end = st.date_input("Published before", value=date.today())
        else:
            publish_date_end = None
            st.text_input("Published before", value="Not used", disabled=True)
        min_comments_on_video = st.number_input("Minimum video comments", min_value=0, value=0, step=100)
    
    st.markdown("---")
    st.subheader("⚙️ Collection Settings")
    
    target_comments = st.number_input(
        "🎯 Target number of comments to collect",
        min_value=100,
        max_value=50000,
        value=20000,
        step=1000,
        help="The scraper will collect comments until reaching this target."
    )
    
    scan_pool_size = st.number_input(
        "🔍 Search pool size (max videos to scan)",
        min_value=50,
        max_value=1000,
        value=1000,
        step=50,
        help="Number of videos to fetch metadata for before filtering."
    )
    
    enable_max_comments_per_video = st.checkbox(
        "Limit comments per video",
        value=True,
        help="Uncheck to fetch ALL available comments from matching videos."
    )
    
    if enable_max_comments_per_video:
        comments_per_video_limit = st.number_input(
            "Max comments per video",
            min_value=50,
            max_value=10000,
            value=500,
            step=100,
        )
    else:
        comments_per_video_limit = 10000
        st.caption("🔄 Will fetch all available comments (up to ~10,000 per video)")
    
    skip_recently_scraped = st.checkbox(
        "Skip videos scraped in last 7 days",
        value=False,
        help="Avoid re-processing videos that were recently scraped."
    )
    
    st.markdown("---")
    st.subheader("💾 Optional Saves")
    
    save_transcripts = st.checkbox(
        "📜 Save full transcripts (YouTube only)",
        value=False,
        help="Saves transcript to 'transcripts' sheet (FREE - no API cost)."
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Run button
# ═══════════════════════════════════════════════════════════════════════════════

run_search = st.button("▶  Run Scrape", type="primary")

if run_search:
    # Input validation
    if not user_name.strip():
        st.error("Your name is required.")
        st.stop()
    
    if platform == PLATFORM_YOUTUBE and not title_kw.strip() and not channel_kw.strip():
        st.error("Please enter at least one keyword for title or channel name.")
        st.stop()
    
    if platform == PLATFORM_TIKTOK and not title_kw.strip() and not channel_kw.strip():
        st.error("Please enter at least one keyword for description or author name.")
        st.stop()
    
    if target_comments <= 0:
        st.error("Target comments must be greater than 0.")
        st.stop()
    
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    all_comments_collected = []
    videos_processed = []
    total_quota_used = 0
    
    # Get existing comment IDs for deduplication
    existing_comment_ids = get_existing_comment_ids(conn)
    st.info(f"📊 Found {len(existing_comment_ids)} existing comments in sheet. Will skip duplicates.")
    
    # Get recently scraped videos if option enabled
    recently_scraped_ids = set()
    if skip_recently_scraped:
        recently_scraped_ids = get_recently_scraped_video_ids(conn, days=7)
        if recently_scraped_ids:
            st.info(f"⏭️ Will skip {len(recently_scraped_ids)} videos scraped in the last 7 days.")
    
    # ── PLATFORM-SPECIFIC SCRAPING ────────────────────────────────────────────
    
    if platform == PLATFORM_YOUTUBE:
        yt = get_yt_client()
        
        # Step 1: Search
        with st.spinner(f"🔎 Searching YouTube (scanning up to {scan_pool_size} videos)…"):
            video_ids = search_youtube_video_ids(yt, title_kw, channel_kw, max_results=scan_pool_size)
        
        if not video_ids:
            st.warning("No videos returned by YouTube search.")
            st.stop()
        
        # Step 2: Fetch metadata
        with st.spinner(f"📋 Fetching metadata for {len(video_ids)} videos…"):
            candidates = fetch_youtube_video_details(yt, video_ids)
        
        # Step 3: Apply keyword filters
        if title_kw.strip():
            candidates = [v for v in candidates if all_keywords_present(v["title"], title_kw)]
        if channel_kw.strip():
            candidates = [v for v in candidates if all_keywords_present(v["channel_name"], channel_kw)]
        
        # Step 4: Apply popularity filters
        candidates = apply_popularity_filters(
            candidates, publish_date_start, publish_date_end, min_views, min_comments_on_video
        )
        
        # Step 5: Skip recently scraped
        if skip_recently_scraped:
            candidates = [v for v in candidates if v["video_id"] not in recently_scraped_ids]
        
        if not candidates:
            st.warning("No videos matched the filters.")
            st.stop()
        
        # Step 6: Transcript filter (if specified)
        if transcript_kw.strip():
            st.info(f"📜 Checking transcripts for {len(candidates)} videos…")
            prog = st.progress(0)
            filtered = []
            for i, v in enumerate(candidates):
                transcript = fetch_youtube_transcript(v["video_id"])
                v["_transcript"] = transcript
                if all_keywords_present(transcript, transcript_kw):
                    filtered.append(v)
                prog.progress((i + 1) / len(candidates))
            prog.empty()
            candidates = filtered
            if not candidates:
                st.warning("No videos matched the transcript keyword.")
                st.stop()
        
        # Pre-fetch transcripts if saving
        elif save_transcripts:
            st.info(f"📜 Downloading transcripts for {len(candidates)} videos…")
            prog = st.progress(0)
            for i, v in enumerate(candidates):
                v["_transcript"] = fetch_youtube_transcript(v["video_id"])
                prog.progress((i + 1) / len(candidates))
            prog.empty()
        
        # Step 7: Comment collection
        st.info(f"💬 Starting comment collection. Target: {target_comments} comments")
        comment_progress = st.progress(0)
        comment_status = st.empty()
        
        # Callback for incremental save
        def save_youtube_comment_batch(batch):
            df_batch = pd.DataFrame(batch)
            df_batch["video_title"] = ""
            df_batch["video_url"] = ""
            df_batch["scraped_by"] = user_name
            df_batch["scraped_at"] = now_str
            df_batch["platform"] = "youtube"
            saved, _ = append_unique_rows(conn, COMMENTS_SHEET, df_batch, key_col="comment_id")
            if saved > 0:
                st.info(f"💾 Saved {saved} new comments incrementally...")
        
        total_comments_fetched = 0
        all_video_comments_preview = []
        
        for idx, video in enumerate(candidates):
            if total_comments_fetched >= target_comments:
                st.success(f"🎯 Reached target of {target_comments} comments!")
                break
            
            remaining_needed = target_comments - total_comments_fetched
            to_fetch = min(comments_per_video_limit, remaining_needed)
            
            comment_status.info(f"Fetching comments for: {video['title'][:50]}... (needs {remaining_needed} more)")
            
            video_comments, quota_used = fetch_youtube_comments_incremental(
                yt, video["video_id"], to_fetch, existing_comment_ids, comment_kw=comment_kw, save_callback=save_youtube_comment_batch
            )
            
            total_quota_used += quota_used
            total_comments_fetched += len(video_comments)
            videos_processed.append(video)
            
            # Add video metadata to comments for preview
            for c in video_comments[:20]:
                c["video_title"] = video["title"]
                c["video_url"] = video["video_url"]
                all_video_comments_preview.append(c)
            
            # Update existing IDs with new ones
            for c in video_comments:
                existing_comment_ids.add(c["comment_id"])
            
            comment_progress.progress(min(total_comments_fetched / target_comments, 1.0))
        
        comment_status.empty()
        comment_progress.empty()
        
        # Step 8: Fetch channel info
        unique_channel_ids = list({v["channel_id"] for v in videos_processed if v.get("channel_id")})
        channel_info_list = []
        with st.spinner(f"📡 Fetching info for {len(unique_channel_ids)} channels…"):
            for cid in unique_channel_ids:
                info = fetch_youtube_channel_info(yt, cid)
                if info:
                    channel_info_list.append(info)
        
    else:  # TikTok
        if not PYKTOK_AVAILABLE:
            st.error("TikTok scraping requires pyktok. Run: pip install pyktok selenium")
            st.stop()
        
        tiktok = TikTokScraper(headless=True)
        
        # Step 1: Search
        query = title_kw if title_kw else channel_kw
        with st.spinner(f"🎵 Searching TikTok for '{query}' (scanning up to {scan_pool_size} videos)…"):
            video_list = tiktok.search_videos(query, max_results=scan_pool_size)
        
        if not video_list:
            st.warning("No videos returned by TikTok search.")
            st.stop()
        
        # Step 2: Fetch metadata
        with st.spinner(f"📋 Fetching metadata for {len(video_list)} videos…"):
            candidates = []
            for vid in video_list:
                details = tiktok.fetch_video_details(vid["video_url"])
                if details:
                    candidates.append(details)
        
        # Step 3: Apply keyword filters
        if title_kw.strip():
            candidates = [v for v in candidates if all_keywords_present(v["title"], title_kw)]
        if channel_kw.strip():
            candidates = [v for v in candidates if all_keywords_present(v["channel_name"], channel_kw)]
        
        # Step 4: Apply popularity filters
        candidates = apply_popularity_filters(
            candidates, publish_date_start, publish_date_end, min_views, min_comments_on_video
        )
        
        # Step 5: Skip recently scraped
        if skip_recently_scraped:
            candidates = [v for v in candidates if v["video_id"] not in recently_scraped_ids]
        
        if not candidates:
            st.warning("No videos matched the filters.")
            st.stop()
        
        # Step 6: Comment collection
        st.info(f"💬 Starting TikTok comment collection. Target: {target_comments} comments")
        comment_progress = st.progress(0)
        comment_status = st.empty()
        
        def save_tiktok_comment_batch(batch):
            df_batch = pd.DataFrame(batch)
            df_batch["video_title"] = ""
            df_batch["video_url"] = ""
            df_batch["scraped_by"] = user_name
            df_batch["scraped_at"] = now_str
            df_batch["platform"] = "tiktok"
            saved, _ = append_unique_rows(conn, COMMENTS_SHEET, df_batch, key_col="comment_id")
            if saved > 0:
                st.info(f"💾 Saved {saved} new TikTok comments incrementally...")
        
        total_comments_fetched = 0
        all_video_comments_preview = []
        
        for idx, video in enumerate(candidates):
            if total_comments_fetched >= target_comments:
                st.success(f"🎯 Reached target of {target_comments} comments!")
                break
            
            remaining_needed = target_comments - total_comments_fetched
            to_fetch = min(comments_per_video_limit, remaining_needed)
            
            comment_status.info(f"Fetching comments for TikTok video... (needs {remaining_needed} more)")
            
            video_comments, _ = tiktok.fetch_comments_incremental(
                video["video_url"], video["video_id"], to_fetch, existing_comment_ids, comment_kw=comment_kw, save_callback=save_tiktok_comment_batch
            )
            
            total_comments_fetched += len(video_comments)
            videos_processed.append(video)
            
            # Add video metadata to comments for preview
            for c in video_comments[:20]:
                c["video_title"] = video["title"][:100] if video.get("title") else "TikTok Video"
                c["video_url"] = video["video_url"]
                all_video_comments_preview.append(c)
            
            # Update existing IDs with new ones
            for c in video_comments:
                existing_comment_ids.add(c["comment_id"])
            
            comment_progress.progress(min(total_comments_fetched / target_comments, 1.0))
        
        comment_status.empty()
        comment_progress.empty()
        
        # Fetch channel info
        unique_channel_ids = list({v["channel_id"] for v in videos_processed if v.get("channel_id")})
        channel_info_list = []
        with st.spinner(f"📡 Fetching info for {len(unique_channel_ids)} TikTok creators…"):
            for cid in unique_channel_ids:
                info = tiktok.fetch_channel_info(cid)
                if info:
                    channel_info_list.append(info)
        
        total_quota_used = 0  # TikTok has no quota
    
    # ── Build DataFrames for preview and saving ────────────────────────────────
    
    # Videos DataFrame
    videos_rows = []
    for v in videos_processed:
        videos_rows.append({
            "video_id": v["video_id"],
            "title": v.get("title", "")[:500],
            "channel_name": v.get("channel_name", ""),
            "channel_id": v.get("channel_id", ""),
            "platform": platform.lower(),
            "published_at": v.get("published_at", ""),
            "view_count": v.get("view_count", "0"),
            "like_count": v.get("like_count", "0"),
            "comment_count": v.get("comment_count", "0"),
            "video_url": v.get("video_url", ""),
            "description_snippet": (v.get("description", "") or "")[:300],
            "comments_fetched_from_video": comments_per_video_limit if enable_max_comments_per_video else "all",
            "search_title_kw": title_kw,
            "search_channel_kw": channel_kw,
            "search_comment_kw": comment_kw,
            "scraped_by": user_name,
            "scraped_at": now_str,
        })
    videos_df = pd.DataFrame(videos_rows)
    
    # Transcripts DataFrame (YouTube only)
    transcripts_df = pd.DataFrame()
    if platform == PLATFORM_YOUTUBE and (save_transcripts or transcript_kw.strip()):
        tr_rows = []
        for v in videos_processed:
            if v.get("_transcript"):
                tr_rows.append({
                    "video_id": v["video_id"],
                    "title": v.get("title", ""),
                    "channel_name": v.get("channel_name", ""),
                    "video_url": v.get("video_url", ""),
                    "full_transcript": v.get("_transcript", ""),
                    "scraped_by": user_name,
                    "scraped_at": now_str,
                })
        if tr_rows:
            transcripts_df = pd.DataFrame(tr_rows)
    
    # Comments preview DataFrame
    comments_preview_df = pd.DataFrame(all_video_comments_preview[:500])
    
    # Channels DataFrame
    channels_df = pd.DataFrame()
    if channel_info_list:
        channels_df = pd.DataFrame(channel_info_list)
        channels_df["scraped_by"] = user_name
        channels_df["scraped_at"] = now_str
    
    # Store in session state
    st.session_state["results"] = {
        "videos_df": videos_df,
        "transcripts_df": transcripts_df,
        "comments_preview_df": comments_preview_df,
        "channels_df": channels_df,
        "total_comments_collected": total_comments_fetched,
        "videos_processed": len(videos_processed),
        "quota_used_this_run": total_quota_used,
        "platform": platform,
    }
    st.session_state["search_params"] = {
        "user_name": user_name,
        "platform": platform,
        "title_kw": title_kw,
        "channel_kw": channel_kw,
        "transcript_kw": transcript_kw if platform == PLATFORM_YOUTUBE else "",
        "comment_kw": comment_kw,
        "target_comments": target_comments,
        "comments_per_video_limit": comments_per_video_limit if enable_max_comments_per_video else "unlimited",
        "save_transcripts": save_transcripts and platform == PLATFORM_YOUTUBE,
        "min_views": min_views,
        "min_comments_on_video": min_comments_on_video,
    }
    
    st.success(f"✅ Scraping complete! Collected {total_comments_fetched} comments from {len(videos_processed)} videos.")
    st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# Preview & Save
# ═══════════════════════════════════════════════════════════════════════════════

if st.session_state["results"] is not None:
    res = st.session_state["results"]
    params = st.session_state["search_params"]
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Platform", params["platform"])
    col2.metric("Videos processed", res["videos_processed"])
    col3.metric("Total comments collected", res["total_comments_collected"])
    col4.metric("Unique channels", len(res["channels_df"]) if not res["channels_df"].empty else 0)
    
    if params["platform"] == PLATFORM_YOUTUBE:
        st.info(f"📊 YouTube API Quota Used: {res['quota_used_this_run']} units")
    
    # Preview Videos
    st.subheader("📋 Preview: Videos Processed")
    preview_cols = ["title", "channel_name", "platform", "published_at", "view_count", "like_count", "video_url"]
    st.dataframe(
        res["videos_df"][[c for c in preview_cols if c in res["videos_df"].columns]].head(20),
        use_container_width=True,
    )
    
    # Preview Channels
    if not res["channels_df"].empty:
        st.subheader("📡 Preview: Channels / Creators")
        ch_preview = ["channel_name", "platform", "channel_url", "subscriber_count" if "subscriber_count" in res["channels_df"].columns else "follower_count"]
        st.dataframe(
            res["channels_df"][[c for c in ch_preview if c in res["channels_df"].columns]].head(10),
            use_container_width=True,
        )
    
    # Preview Comments
    if not res["comments_preview_df"].empty:
        st.subheader(f"💬 Preview: Sample Comments (showing first 20 of {res['total_comments_collected']} collected)")
        cm_preview = ["video_title", "author", "comment_text", "comment_published_at", "comment_likes"]
        st.dataframe(
            res["comments_preview_df"][[c for c in cm_preview if c in res["comments_preview_df"].columns]].head(20),
            use_container_width=True,
        )
        st.caption(f"✨ Comments were saved incrementally during collection. Total: {res['total_comments_collected']}")
    
    # Preview Transcripts (YouTube only)
    if not res["transcripts_df"].empty:
        st.subheader(f"📜 Preview: Transcripts (showing first 5 of {len(res['transcripts_df'])})")
        st.dataframe(
            res["transcripts_df"][["title", "channel_name", "full_transcript"]].head(5),
            use_container_width=True,
        )
    
    # Save remaining data
    st.markdown("---")
    if st.button("💾  SAVE REMAINING DATA TO GOOGLE SHEETS", type="primary"):
        with st.spinner("Saving to database…"):
            # Videos
            vid_saved, vid_dup = append_unique_rows(
                conn, VIDEOS_SHEET, res["videos_df"], key_col="video_id"
            )
            
            # Channels
            ch_saved = 0
            if not res["channels_df"].empty:
                ch_saved, _ = append_unique_rows(
                    conn, CHANNELS_SHEET, res["channels_df"], key_col="channel_id"
                )
            
            # Transcripts
            tr_saved = 0
            if not res["transcripts_df"].empty:
                tr_saved, _ = append_unique_rows(
                    conn, TRANSCRIPTS_SHEET, res["transcripts_df"], key_col="video_id"
                )
            
            # Log the run
            log_run(conn, {
                "run_id": str(uuid.uuid4())[:8],
                "run_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "user": params["user_name"],
                "platform": params["platform"],
                "title_kw": params["title_kw"],
                "channel_kw": params["channel_kw"],
                "comment_kw": params["comment_kw"],
                "target_comments": params["target_comments"],
                "videos_processed": res["videos_processed"],
                "comments_collected": res["total_comments_collected"],
                "videos_saved": vid_saved,
                "channels_saved": ch_saved,
                "transcripts_saved": tr_saved,
                "quota_used": res.get("quota_used_this_run", 0),
            })
        
        st.success(
            f"✅ Saved → "
            f"{vid_saved} new videos · "
            f"{ch_saved} new channels · "
            f"{tr_saved} new transcripts\n\n"
            f"📝 Comments were already saved incrementally during scraping!"
        )
        
        # Clear session state
        st.session_state["results"] = None
        st.session_state["search_params"] = None
        st.rerun()
