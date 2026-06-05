"""
Reusable Social Content Scraper
===============================
Streamlit app for YouTube-first, TikTok-ready scraping with Neon/PostgreSQL storage.

Core design:
- One durable PostgreSQL database for all projects.
- Project labels group multiple scrape runs without duplicating raw comments/videos.
- Unicode-safe search and export for multilingual comments.
- Project-scoped Excel and CSV ZIP downloads.
"""

from __future__ import annotations

import io
import re
import uuid
import zipfile
import unicodedata
from datetime import date, datetime
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import NoTranscriptFound, TranscriptsDisabled

try:
    import pyktok as pyk
    PYKTOK_AVAILABLE = True
except ImportError:
    PYKTOK_AVAILABLE = False


PLATFORM_YOUTUBE = "YouTube"
PLATFORM_TIKTOK = "TikTok"
DB_SCHEMA_DEFAULT = "social_scraper"

QUOTA_COSTS = {
    "search": 100,
    "videos": 1,
    "comments": 1,
    "channels": 1,
}


# =============================================================================
# Text and keyword matching helpers
# =============================================================================

def strip_accents(text_value: str) -> str:
    """Remove accents/diacritics while keeping base characters."""
    decomposed = unicodedata.normalize("NFKD", str(text_value or ""))
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def normalize_for_match(text_value: str, accent_insensitive: bool = True) -> str:
    """Normalize text for robust multilingual keyword matching.

    casefold() is stronger than lower() for Unicode text. Accent-insensitive mode
    makes Vietnamese searches more forgiving: 'Hanoi', 'Hà Nội', and 'ha noi'
    become easier to match. This affects only filtering, not the stored raw text.
    """
    normalized = unicodedata.normalize("NFKC", str(text_value or "")).casefold().strip()
    if accent_insensitive:
        normalized = strip_accents(normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def parse_keywords(keywords_str: str) -> List[str]:
    """Comma-separated keywords, using AND logic across all non-empty entries."""
    return [k.strip() for k in str(keywords_str or "").split(",") if k.strip()]


def all_keywords_present(text_value: str, keywords_str: str, accent_insensitive: bool = True) -> bool:
    """Return True if every comma-separated keyword appears anywhere in text."""
    keywords = parse_keywords(keywords_str)
    if not keywords:
        return True
    haystack = normalize_for_match(text_value, accent_insensitive=accent_insensitive)
    return all(normalize_for_match(k, accent_insensitive=accent_insensitive) in haystack for k in keywords)


def canonical_project_key(project_name: str) -> str:
    """Create a forgiving project key for grouping collaborator inputs.

    Examples:
    'Vietnam Street Food', 'vietnam street food', and 'Vietnam streetfood'
    all become 'vietnamstreetfood'.
    """
    key = normalize_for_match(project_name, accent_insensitive=True)
    key = re.sub(r"[^a-z0-9]+", "", key)
    return key or "untitledproject"


def build_seed_query(title_kw: str, channel_kw: str, transcript_kw: str, comment_kw: str) -> str:
    """Pick a useful YouTube search seed even when title is blank."""
    candidates = [title_kw, transcript_kw, comment_kw, channel_kw]
    for candidate in candidates:
        kws = parse_keywords(candidate)
        if kws:
            return " ".join(kws[:6])
    return "youtube"


def safe_int(value, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def safe_date(value):
    if not value:
        return None
    try:
        return pd.to_datetime(value).date()
    except Exception:
        return None


# =============================================================================
# Database helpers, schema setup, and project/run mapping
# =============================================================================

def get_db_schema() -> str:
    return st.secrets.get("DB_SCHEMA", DB_SCHEMA_DEFAULT)


def get_db_engine() -> Engine:
    database_url = st.secrets.get("DATABASE_URL", "sqlite:///scraper_local.db")
    return create_engine(database_url, pool_pre_ping=True)


def qualified(schema: str, table_name: str) -> str:
    return f'"{schema}"."{table_name}"'


def ensure_database_ready(engine: Engine, schema: str) -> None:
    """Create/patch the normalized schema used by the scraper."""
    if engine.dialect.name != "postgresql":
        # Local SQLite fallback: simple tables will be created by pandas/export only.
        return

    ddl = f'''
    CREATE SCHEMA IF NOT EXISTS "{schema}";
    CREATE EXTENSION IF NOT EXISTS pgcrypto;

    CREATE TABLE IF NOT EXISTS "{schema}".projects (
        project_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        project_name TEXT NOT NULL UNIQUE,
        project_key TEXT UNIQUE,
        description TEXT,
        created_by TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    ALTER TABLE "{schema}".projects ADD COLUMN IF NOT EXISTS project_key TEXT;
    CREATE UNIQUE INDEX IF NOT EXISTS projects_project_key_uq ON "{schema}".projects(project_key);

    CREATE TABLE IF NOT EXISTS "{schema}".scrape_runs (
        run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        project_id UUID REFERENCES "{schema}".projects(project_id) ON DELETE SET NULL,
        platform TEXT NOT NULL,
        run_status TEXT NOT NULL DEFAULT 'started',
        title_kw TEXT,
        channel_kw TEXT,
        transcript_kw TEXT,
        comment_kw TEXT,
        publish_date_start DATE,
        publish_date_end DATE,
        min_views BIGINT DEFAULT 0,
        min_comment_count BIGINT DEFAULT 0,
        scan_pool_size INTEGER DEFAULT 1000,
        target_comments INTEGER DEFAULT 20000,
        comments_per_video_limit INTEGER,
        scraped_by TEXT,
        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at TIMESTAMPTZ,
        videos_found INTEGER DEFAULT 0,
        videos_processed INTEGER DEFAULT 0,
        comments_collected INTEGER DEFAULT 0,
        quota_used INTEGER DEFAULT 0,
        error_message TEXT,
        notes TEXT
    );

    CREATE TABLE IF NOT EXISTS "{schema}".channels (
        platform TEXT NOT NULL,
        channel_id TEXT NOT NULL,
        channel_name TEXT,
        channel_url TEXT,
        country TEXT,
        established_date DATE,
        subscriber_count BIGINT,
        follower_count BIGINT,
        following_count BIGINT,
        video_count BIGINT,
        total_views BIGINT,
        heart_count BIGINT,
        bio TEXT,
        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (platform, channel_id)
    );

    CREATE TABLE IF NOT EXISTS "{schema}".videos (
        platform TEXT NOT NULL,
        video_id TEXT NOT NULL,
        channel_id TEXT,
        title TEXT,
        description TEXT,
        published_at DATE,
        video_url TEXT,
        view_count BIGINT,
        like_count BIGINT,
        comment_count BIGINT,
        duration_seconds INTEGER,
        language TEXT,
        comments_fetched_from_video TEXT,
        search_title_kw TEXT,
        search_channel_kw TEXT,
        search_comment_kw TEXT,
        first_seen_run_id UUID REFERENCES "{schema}".scrape_runs(run_id) ON DELETE SET NULL,
        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        raw_metadata JSONB,
        PRIMARY KEY (platform, video_id)
    );

    ALTER TABLE "{schema}".videos ADD COLUMN IF NOT EXISTS comments_fetched_from_video TEXT;
    ALTER TABLE "{schema}".videos ADD COLUMN IF NOT EXISTS search_title_kw TEXT;
    ALTER TABLE "{schema}".videos ADD COLUMN IF NOT EXISTS search_channel_kw TEXT;
    ALTER TABLE "{schema}".videos ADD COLUMN IF NOT EXISTS search_comment_kw TEXT;

    CREATE TABLE IF NOT EXISTS "{schema}".transcripts (
        platform TEXT NOT NULL DEFAULT 'youtube',
        video_id TEXT NOT NULL,
        transcript_text TEXT NOT NULL,
        transcript_language TEXT,
        is_auto_generated BOOLEAN,
        scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (platform, video_id)
    );

    CREATE TABLE IF NOT EXISTS "{schema}".comments (
        platform TEXT NOT NULL,
        comment_id TEXT NOT NULL,
        video_id TEXT NOT NULL,
        author TEXT,
        author_channel_id TEXT,
        comment_text TEXT NOT NULL,
        comment_published_at TIMESTAMPTZ,
        comment_likes BIGINT DEFAULT 0,
        comment_reply_count BIGINT DEFAULT 0,
        parent_comment_id TEXT,
        scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        first_seen_run_id UUID REFERENCES "{schema}".scrape_runs(run_id) ON DELETE SET NULL,
        raw_metadata JSONB,
        PRIMARY KEY (platform, comment_id)
    );

    ALTER TABLE "{schema}".comments ADD COLUMN IF NOT EXISTS comment_reply_count BIGINT DEFAULT 0;

    CREATE TABLE IF NOT EXISTS "{schema}".project_videos (
        project_id UUID NOT NULL REFERENCES "{schema}".projects(project_id) ON DELETE CASCADE,
        run_id UUID REFERENCES "{schema}".scrape_runs(run_id) ON DELETE SET NULL,
        platform TEXT NOT NULL,
        video_id TEXT NOT NULL,
        matched_title_kw BOOLEAN DEFAULT FALSE,
        matched_channel_kw BOOLEAN DEFAULT FALSE,
        matched_transcript_kw BOOLEAN DEFAULT FALSE,
        matched_comment_kw BOOLEAN DEFAULT FALSE,
        added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (project_id, platform, video_id)
    );

    CREATE TABLE IF NOT EXISTS "{schema}".project_comments (
        project_id UUID NOT NULL REFERENCES "{schema}".projects(project_id) ON DELETE CASCADE,
        run_id UUID REFERENCES "{schema}".scrape_runs(run_id) ON DELETE SET NULL,
        platform TEXT NOT NULL,
        comment_id TEXT NOT NULL,
        matched_comment_kw BOOLEAN DEFAULT FALSE,
        added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (project_id, platform, comment_id)
    );

    CREATE INDEX IF NOT EXISTS idx_project_videos_project ON "{schema}".project_videos(project_id);
    CREATE INDEX IF NOT EXISTS idx_project_comments_project ON "{schema}".project_comments(project_id);
    '''
    with engine.begin() as conn:
        conn.execute(text(ddl))


def test_database_connection(engine: Engine, schema: str) -> bool:
    try:
        ensure_database_ready(engine, schema)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        st.error("Database connection failed. Please verify DATABASE_URL, Neon project, branch, database name, password, and sslmode=require.")
        with st.expander("Technical details"):
            st.exception(exc)
        return False


def get_or_create_project(engine: Engine, schema: str, project_name: str, created_by: str) -> Tuple[str, str]:
    project_name = project_name.strip()
    project_key = canonical_project_key(project_name)
    with engine.begin() as conn:
        row = conn.execute(
            text(f'SELECT project_id, project_name FROM {qualified(schema, "projects")} WHERE project_key = :project_key'),
            {"project_key": project_key},
        ).mappings().first()
        if row:
            return str(row["project_id"]), row["project_name"]

        row = conn.execute(
            text(f'''
                INSERT INTO {qualified(schema, "projects")} (project_name, project_key, created_by)
                VALUES (:project_name, :project_key, :created_by)
                ON CONFLICT (project_name) DO UPDATE SET project_key = EXCLUDED.project_key
                RETURNING project_id, project_name
            '''),
            {"project_name": project_name, "project_key": project_key, "created_by": created_by},
        ).mappings().first()
        return str(row["project_id"]), row["project_name"]


def create_scrape_run(engine: Engine, schema: str, project_id: str, params: Dict) -> str:
    run_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(f'''
                INSERT INTO {qualified(schema, "scrape_runs")}
                (run_id, project_id, platform, title_kw, channel_kw, transcript_kw, comment_kw,
                 publish_date_start, publish_date_end, min_views, min_comment_count,
                 scan_pool_size, target_comments, comments_per_video_limit, scraped_by)
                VALUES
                (:run_id, :project_id, :platform, :title_kw, :channel_kw, :transcript_kw, :comment_kw,
                 :publish_date_start, :publish_date_end, :min_views, :min_comment_count,
                 :scan_pool_size, :target_comments, :comments_per_video_limit, :scraped_by)
            '''),
            {
                "run_id": run_id,
                "project_id": project_id,
                "platform": params["platform"].lower(),
                "title_kw": params.get("title_kw"),
                "channel_kw": params.get("channel_kw"),
                "transcript_kw": params.get("transcript_kw"),
                "comment_kw": params.get("comment_kw"),
                "publish_date_start": params.get("publish_date_start"),
                "publish_date_end": params.get("publish_date_end"),
                "min_views": params.get("min_views", 0),
                "min_comment_count": params.get("min_comments_on_video", 0),
                "scan_pool_size": params.get("scan_pool_size", 0),
                "target_comments": params.get("target_comments", 0),
                "comments_per_video_limit": params.get("comments_per_video_limit_numeric"),
                "scraped_by": params.get("user_name"),
            },
        )
    return run_id


def complete_scrape_run(engine: Engine, schema: str, run_id: str, status: str, videos_found: int, videos_processed: int, comments_collected: int, quota_used: int, error_message: str = "") -> None:
    with engine.begin() as conn:
        conn.execute(
            text(f'''
                UPDATE {qualified(schema, "scrape_runs")}
                SET run_status = :status,
                    completed_at = NOW(),
                    videos_found = :videos_found,
                    videos_processed = :videos_processed,
                    comments_collected = :comments_collected,
                    quota_used = :quota_used,
                    error_message = NULLIF(:error_message, '')
                WHERE run_id = :run_id
            '''),
            {
                "run_id": run_id,
                "status": status,
                "videos_found": videos_found,
                "videos_processed": videos_processed,
                "comments_collected": comments_collected,
                "quota_used": quota_used,
                "error_message": error_message,
            },
        )


def get_existing_comment_ids(engine: Engine, schema: str) -> set:
    try:
        ensure_database_ready(engine, schema)
        df = pd.read_sql_query(f'SELECT comment_id FROM {qualified(schema, "comments")}', engine)
        return set(df["comment_id"].astype(str).fillna("").tolist())
    except Exception:
        return set()


def get_recently_scraped_video_ids(engine: Engine, schema: str, days: int = 7) -> set:
    try:
        df = pd.read_sql_query(
            f'SELECT video_id, last_seen_at FROM {qualified(schema, "videos")} WHERE last_seen_at >= NOW() - INTERVAL \'{int(days)} days\'',
            engine,
        )
        return set(df["video_id"].astype(str).fillna("").tolist())
    except Exception:
        return set()


def save_results_to_database(engine: Engine, schema: str, project_id: str, run_id: str, res: Dict, params: Dict) -> Dict[str, int]:
    """Upsert scraped rows into normalized DB and map them to the project/run."""
    platform = params["platform"].lower()
    counts = {"videos": 0, "channels": 0, "transcripts": 0, "comments": 0}

    videos_df = res.get("videos_df", pd.DataFrame()).copy()
    channels_df = res.get("channels_df", pd.DataFrame()).copy()
    transcripts_df = res.get("transcripts_df", pd.DataFrame()).copy()
    comments_df = res.get("comments_df", pd.DataFrame()).copy()

    with engine.begin() as conn:
        # Channels first, so videos can reference them if FK constraints exist.
        for _, r in channels_df.iterrows():
            conn.execute(
                text(f'''
                    INSERT INTO {qualified(schema, "channels")}
                    (platform, channel_id, channel_name, channel_url, country, established_date,
                     subscriber_count, follower_count, following_count, video_count, total_views, heart_count, bio, last_seen_at)
                    VALUES
                    (:platform, :channel_id, :channel_name, :channel_url, :country, :established_date,
                     :subscriber_count, :follower_count, :following_count, :video_count, :total_views, :heart_count, :bio, NOW())
                    ON CONFLICT (platform, channel_id) DO UPDATE SET
                        channel_name = EXCLUDED.channel_name,
                        channel_url = EXCLUDED.channel_url,
                        country = COALESCE(EXCLUDED.country, {qualified(schema, "channels")}.country),
                        established_date = COALESCE(EXCLUDED.established_date, {qualified(schema, "channels")}.established_date),
                        subscriber_count = COALESCE(EXCLUDED.subscriber_count, {qualified(schema, "channels")}.subscriber_count),
                        follower_count = COALESCE(EXCLUDED.follower_count, {qualified(schema, "channels")}.follower_count),
                        following_count = COALESCE(EXCLUDED.following_count, {qualified(schema, "channels")}.following_count),
                        video_count = COALESCE(EXCLUDED.video_count, {qualified(schema, "channels")}.video_count),
                        total_views = COALESCE(EXCLUDED.total_views, {qualified(schema, "channels")}.total_views),
                        heart_count = COALESCE(EXCLUDED.heart_count, {qualified(schema, "channels")}.heart_count),
                        bio = COALESCE(EXCLUDED.bio, {qualified(schema, "channels")}.bio),
                        last_seen_at = NOW()
                '''),
                {
                    "platform": platform,
                    "channel_id": str(r.get("channel_id", "")),
                    "channel_name": r.get("channel_name"),
                    "channel_url": r.get("channel_url"),
                    "country": r.get("country"),
                    "established_date": safe_date(r.get("established_date")),
                    "subscriber_count": safe_int(r.get("subscriber_count"), None),
                    "follower_count": safe_int(r.get("follower_count"), None),
                    "following_count": safe_int(r.get("following_count"), None),
                    "video_count": safe_int(r.get("video_count"), None),
                    "total_views": safe_int(r.get("total_views"), None),
                    "heart_count": safe_int(r.get("heart_count"), None),
                    "bio": r.get("bio"),
                },
            )
            counts["channels"] += 1

        for _, r in videos_df.iterrows():
            conn.execute(
                text(f'''
                    INSERT INTO {qualified(schema, "videos")}
                    (platform, video_id, channel_id, title, description, published_at, video_url,
                     view_count, like_count, comment_count, comments_fetched_from_video,
                     search_title_kw, search_channel_kw, search_comment_kw, first_seen_run_id, last_seen_at)
                    VALUES
                    (:platform, :video_id, :channel_id, :title, :description, :published_at, :video_url,
                     :view_count, :like_count, :comment_count, :comments_fetched_from_video,
                     :search_title_kw, :search_channel_kw, :search_comment_kw, :run_id, NOW())
                    ON CONFLICT (platform, video_id) DO UPDATE SET
                        channel_id = COALESCE(EXCLUDED.channel_id, {qualified(schema, "videos")}.channel_id),
                        title = COALESCE(EXCLUDED.title, {qualified(schema, "videos")}.title),
                        description = COALESCE(EXCLUDED.description, {qualified(schema, "videos")}.description),
                        published_at = COALESCE(EXCLUDED.published_at, {qualified(schema, "videos")}.published_at),
                        video_url = COALESCE(EXCLUDED.video_url, {qualified(schema, "videos")}.video_url),
                        view_count = COALESCE(EXCLUDED.view_count, {qualified(schema, "videos")}.view_count),
                        like_count = COALESCE(EXCLUDED.like_count, {qualified(schema, "videos")}.like_count),
                        comment_count = COALESCE(EXCLUDED.comment_count, {qualified(schema, "videos")}.comment_count),
                        comments_fetched_from_video = COALESCE(EXCLUDED.comments_fetched_from_video, {qualified(schema, "videos")}.comments_fetched_from_video),
                        search_title_kw = COALESCE(EXCLUDED.search_title_kw, {qualified(schema, "videos")}.search_title_kw),
                        search_channel_kw = COALESCE(EXCLUDED.search_channel_kw, {qualified(schema, "videos")}.search_channel_kw),
                        search_comment_kw = COALESCE(EXCLUDED.search_comment_kw, {qualified(schema, "videos")}.search_comment_kw),
                        last_seen_at = NOW()
                '''),
                {
                    "platform": platform,
                    "video_id": str(r.get("video_id", "")),
                    "channel_id": r.get("channel_id"),
                    "title": r.get("title"),
                    "description": r.get("description"),
                    "published_at": safe_date(r.get("published_at")),
                    "video_url": r.get("video_url"),
                    "view_count": safe_int(r.get("view_count"), None),
                    "like_count": safe_int(r.get("like_count"), None),
                    "comment_count": safe_int(r.get("comment_count"), None),
                    "comments_fetched_from_video": str(r.get("comments_fetched_from_video", "")),
                    "search_title_kw": params.get("title_kw"),
                    "search_channel_kw": params.get("channel_kw"),
                    "search_comment_kw": params.get("comment_kw"),
                    "run_id": run_id,
                },
            )
            conn.execute(
                text(f'''
                    INSERT INTO {qualified(schema, "project_videos")}
                    (project_id, run_id, platform, video_id, matched_title_kw, matched_channel_kw, matched_transcript_kw, matched_comment_kw)
                    VALUES (:project_id, :run_id, :platform, :video_id, :matched_title_kw, :matched_channel_kw, :matched_transcript_kw, :matched_comment_kw)
                    ON CONFLICT (project_id, platform, video_id) DO UPDATE SET run_id = EXCLUDED.run_id
                '''),
                {
                    "project_id": project_id,
                    "run_id": run_id,
                    "platform": platform,
                    "video_id": str(r.get("video_id", "")),
                    "matched_title_kw": bool(params.get("title_kw")),
                    "matched_channel_kw": bool(params.get("channel_kw")),
                    "matched_transcript_kw": bool(params.get("transcript_kw")),
                    "matched_comment_kw": bool(params.get("comment_kw")),
                },
            )
            counts["videos"] += 1

        for _, r in transcripts_df.iterrows():
            conn.execute(
                text(f'''
                    INSERT INTO {qualified(schema, "transcripts")}
                    (platform, video_id, transcript_text, transcript_language, is_auto_generated, scraped_at)
                    VALUES (:platform, :video_id, :transcript_text, :transcript_language, :is_auto_generated, NOW())
                    ON CONFLICT (platform, video_id) DO UPDATE SET
                        transcript_text = EXCLUDED.transcript_text,
                        transcript_language = COALESCE(EXCLUDED.transcript_language, {qualified(schema, "transcripts")}.transcript_language),
                        is_auto_generated = COALESCE(EXCLUDED.is_auto_generated, {qualified(schema, "transcripts")}.is_auto_generated),
                        scraped_at = NOW()
                '''),
                {
                    "platform": platform,
                    "video_id": str(r.get("video_id", "")),
                    "transcript_text": r.get("full_transcript") or r.get("transcript_text") or "",
                    "transcript_language": r.get("transcript_language"),
                    "is_auto_generated": r.get("is_auto_generated"),
                },
            )
            counts["transcripts"] += 1

        for _, r in comments_df.iterrows():
            conn.execute(
                text(f'''
                    INSERT INTO {qualified(schema, "comments")}
                    (platform, comment_id, video_id, author, author_channel_id, comment_text,
                     comment_published_at, comment_likes, comment_reply_count, scraped_at, first_seen_run_id)
                    VALUES
                    (:platform, :comment_id, :video_id, :author, :author_channel_id, :comment_text,
                     :comment_published_at, :comment_likes, :comment_reply_count, NOW(), :run_id)
                    ON CONFLICT (platform, comment_id) DO UPDATE SET
                        video_id = COALESCE(EXCLUDED.video_id, {qualified(schema, "comments")}.video_id),
                        author = COALESCE(EXCLUDED.author, {qualified(schema, "comments")}.author),
                        author_channel_id = COALESCE(EXCLUDED.author_channel_id, {qualified(schema, "comments")}.author_channel_id),
                        comment_text = COALESCE(EXCLUDED.comment_text, {qualified(schema, "comments")}.comment_text),
                        comment_published_at = COALESCE(EXCLUDED.comment_published_at, {qualified(schema, "comments")}.comment_published_at),
                        comment_likes = COALESCE(EXCLUDED.comment_likes, {qualified(schema, "comments")}.comment_likes),
                        comment_reply_count = COALESCE(EXCLUDED.comment_reply_count, {qualified(schema, "comments")}.comment_reply_count),
                        scraped_at = NOW()
                '''),
                {
                    "platform": platform,
                    "comment_id": str(r.get("comment_id", "")),
                    "video_id": str(r.get("video_id", "")),
                    "author": r.get("author"),
                    "author_channel_id": r.get("author_channel_id"),
                    "comment_text": r.get("comment_text") or "",
                    "comment_published_at": pd.to_datetime(r.get("comment_published_at"), errors="coerce").to_pydatetime() if pd.notna(pd.to_datetime(r.get("comment_published_at"), errors="coerce")) else None,
                    "comment_likes": safe_int(r.get("comment_likes"), 0),
                    "comment_reply_count": safe_int(r.get("comment_reply_count"), 0),
                    "run_id": run_id,
                },
            )
            conn.execute(
                text(f'''
                    INSERT INTO {qualified(schema, "project_comments")}
                    (project_id, run_id, platform, comment_id, matched_comment_kw)
                    VALUES (:project_id, :run_id, :platform, :comment_id, :matched_comment_kw)
                    ON CONFLICT (project_id, platform, comment_id) DO UPDATE SET run_id = EXCLUDED.run_id
                '''),
                {
                    "project_id": project_id,
                    "run_id": run_id,
                    "platform": platform,
                    "comment_id": str(r.get("comment_id", "")),
                    "matched_comment_kw": bool(params.get("comment_kw")),
                },
            )
            counts["comments"] += 1

    return counts


# =============================================================================
# Export helpers
# =============================================================================

def read_project_exports(engine: Engine, schema: str, project_name: str) -> Dict[str, pd.DataFrame]:
    project_key = canonical_project_key(project_name)
    params = {"project_key": project_key}

    master_sql = f'''
        SELECT
            c.comment_id,
            c.comment_text,
            c.author AS comment_author,
            c.comment_published_at AS comment_date,
            c.comment_likes,
            c.comment_reply_count AS comment_replies,
            v.video_id,
            v.title AS video_title,
            v.video_url,
            v.published_at AS video_date,
            v.view_count AS video_views,
            v.like_count AS video_likes,
            v.comment_count AS video_comments,
            ch.channel_id,
            ch.channel_name,
            COALESCE(ch.subscriber_count, ch.follower_count) AS channel_subscribers,
            ch.video_count AS channel_videos,
            COALESCE(ch.total_views, ch.heart_count) AS channel_views,
            p.project_name AS project,
            sr.scraped_by,
            c.scraped_at,
            c.platform
        FROM {qualified(schema, "project_comments")} pc
        JOIN {qualified(schema, "projects")} p ON p.project_id = pc.project_id
        LEFT JOIN {qualified(schema, "scrape_runs")} sr ON sr.run_id = pc.run_id
        JOIN {qualified(schema, "comments")} c ON c.platform = pc.platform AND c.comment_id = pc.comment_id
        LEFT JOIN {qualified(schema, "videos")} v ON v.platform = c.platform AND v.video_id = c.video_id
        LEFT JOIN {qualified(schema, "channels")} ch ON ch.platform = v.platform AND ch.channel_id = v.channel_id
        WHERE p.project_key = :project_key
        ORDER BY c.scraped_at DESC, c.comment_published_at DESC NULLS LAST
    '''

    comments_sql = f'''
        SELECT
            c.comment_id,
            c.comment_text,
            c.author AS comment_author,
            c.comment_published_at AS comment_date,
            c.comment_likes,
            c.comment_reply_count AS comment_replies,
            c.video_id,
            v.title AS video_title,
            v.video_url,
            p.project_name AS project,
            sr.scraped_by,
            c.scraped_at,
            c.platform
        FROM {qualified(schema, "project_comments")} pc
        JOIN {qualified(schema, "projects")} p ON p.project_id = pc.project_id
        LEFT JOIN {qualified(schema, "scrape_runs")} sr ON sr.run_id = pc.run_id
        JOIN {qualified(schema, "comments")} c ON c.platform = pc.platform AND c.comment_id = pc.comment_id
        LEFT JOIN {qualified(schema, "videos")} v ON v.platform = c.platform AND v.video_id = c.video_id
        WHERE p.project_key = :project_key
        ORDER BY c.scraped_at DESC, c.comment_published_at DESC NULLS LAST
    '''

    videos_sql = f'''
        SELECT
            v.video_id,
            v.title AS video_title,
            v.channel_id,
            ch.channel_name,
            v.published_at AS video_date,
            v.view_count AS video_views,
            v.like_count AS video_likes,
            v.comment_count AS video_comments,
            v.video_url,
            v.description AS video_description,
            t.transcript_text AS video_transcript,
            v.comments_fetched_from_video,
            sr.title_kw AS search_title_kw,
            sr.channel_kw AS search_channel_kw,
            sr.comment_kw AS search_comment_kw,
            p.project_name AS project,
            sr.scraped_by,
            v.last_seen_at AS scraped_at,
            v.platform
        FROM {qualified(schema, "project_videos")} pv
        JOIN {qualified(schema, "projects")} p ON p.project_id = pv.project_id
        LEFT JOIN {qualified(schema, "scrape_runs")} sr ON sr.run_id = pv.run_id
        JOIN {qualified(schema, "videos")} v ON v.platform = pv.platform AND v.video_id = pv.video_id
        LEFT JOIN {qualified(schema, "channels")} ch ON ch.platform = v.platform AND ch.channel_id = v.channel_id
        LEFT JOIN {qualified(schema, "transcripts")} t ON t.platform = v.platform AND t.video_id = v.video_id
        WHERE p.project_key = :project_key
        ORDER BY v.last_seen_at DESC, v.published_at DESC NULLS LAST
    '''

    channels_sql = f'''
        SELECT DISTINCT
            ch.channel_id,
            ch.channel_name,
            ch.channel_url,
            ch.country,
            ch.established_date AS channel_established_date,
            COALESCE(ch.subscriber_count, ch.follower_count) AS channel_subscribers,
            ch.video_count AS channel_videos,
            COALESCE(ch.total_views, ch.heart_count) AS channel_views,
            p.project_name AS project,
            sr.scraped_by,
            ch.last_seen_at AS scraped_at,
            ch.platform
        FROM {qualified(schema, "project_videos")} pv
        JOIN {qualified(schema, "projects")} p ON p.project_id = pv.project_id
        LEFT JOIN {qualified(schema, "scrape_runs")} sr ON sr.run_id = pv.run_id
        JOIN {qualified(schema, "videos")} v ON v.platform = pv.platform AND v.video_id = pv.video_id
        JOIN {qualified(schema, "channels")} ch ON ch.platform = v.platform AND ch.channel_id = v.channel_id
        WHERE p.project_key = :project_key
        ORDER BY channel_subscribers DESC NULLS LAST, ch.channel_name
    '''

    projects_sql = f'''
        SELECT
            p.project_id,
            p.project_name,
            p.project_key,
            p.description,
            p.created_by,
            p.created_at,
            COUNT(DISTINCT pv.video_id) AS total_videos,
            COUNT(DISTINCT pc.comment_id) AS total_comments,
            COUNT(DISTINCT sr.run_id) AS total_scrape_runs,
            MAX(sr.completed_at) AS last_completed_run_at
        FROM {qualified(schema, "projects")} p
        LEFT JOIN {qualified(schema, "project_videos")} pv ON pv.project_id = p.project_id
        LEFT JOIN {qualified(schema, "project_comments")} pc ON pc.project_id = p.project_id
        LEFT JOIN {qualified(schema, "scrape_runs")} sr ON sr.project_id = p.project_id
        WHERE p.project_key = :project_key
        GROUP BY p.project_id, p.project_name, p.project_key, p.description, p.created_by, p.created_at
    '''

    run_log_sql = f'''
        SELECT
            sr.run_id,
            p.project_name AS project,
            sr.platform,
            sr.run_status,
            sr.title_kw,
            sr.channel_kw,
            sr.transcript_kw,
            sr.comment_kw,
            sr.publish_date_start,
            sr.publish_date_end,
            sr.min_views,
            sr.min_comment_count,
            sr.scan_pool_size,
            sr.target_comments,
            sr.comments_per_video_limit,
            sr.scraped_by,
            sr.started_at,
            sr.completed_at,
            sr.videos_found,
            sr.videos_processed,
            sr.comments_collected,
            sr.quota_used,
            sr.error_message
        FROM {qualified(schema, "scrape_runs")} sr
        JOIN {qualified(schema, "projects")} p ON p.project_id = sr.project_id
        WHERE p.project_key = :project_key
        ORDER BY sr.started_at DESC
    '''

    queries = {
        "master_analysis": master_sql,
        "comments": comments_sql,
        "videos": videos_sql,
        "channels": channels_sql,
        "projects": projects_sql,
        "run_log": run_log_sql,
    }
    return {name: pd.read_sql_query(text(sql), engine, params=params) for name, sql in queries.items()}


def make_excel_bytes(sheets: Dict[str, pd.DataFrame]) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            # Excel sheet names max 31 chars.
            safe_sheet = sheet_name[:31]
            df.to_excel(writer, sheet_name=safe_sheet, index=False)
    return output.getvalue()


def make_csv_zip_bytes(sheets: Dict[str, pd.DataFrame]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, df in sheets.items():
            # utf-8-sig makes Excel friendlier while remaining UTF-8 for BI tools.
            zf.writestr(f"{name}.csv", df.to_csv(index=False).encode("utf-8-sig"))
    return output.getvalue()


# =============================================================================
# YouTube API helpers
# =============================================================================

def update_quota_tracker(cost: int) -> None:
    if "quota_used_today" not in st.session_state:
        st.session_state["quota_used_today"] = 0
    st.session_state["quota_used_today"] += cost


def check_quota_and_warn(additional_cost: int = 0) -> bool:
    current = st.session_state.get("quota_used_today", 0)
    if current + additional_cost >= 9500:
        st.warning(f"Approaching quota limit: {current}/10,000 units used.")
    if current + additional_cost >= 10000:
        st.error("Daily YouTube API quota would be exceeded.")
        return False
    return True


def get_yt_client():
    return build("youtube", "v3", developerKey=st.secrets["YOUTUBE_API_KEY"], cache_discovery=False)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def search_youtube_video_ids(yt, seed_query: str, max_results: int) -> List[str]:
    if not check_quota_and_warn(QUOTA_COSTS["search"]):
        return []
    video_ids: List[str] = []
    next_page_token = None
    try:
        while len(video_ids) < max_results:
            resp = yt.search().list(
                part="id",
                q=seed_query or "youtube",
                type="video",
                maxResults=min(50, max_results - len(video_ids)),
                pageToken=next_page_token,
            ).execute()
            for item in resp.get("items", []):
                if item.get("id", {}).get("kind") == "youtube#video":
                    video_ids.append(item["id"]["videoId"])
            next_page_token = resp.get("nextPageToken")
            if not next_page_token:
                break
        update_quota_tracker(QUOTA_COSTS["search"])
        return video_ids
    except HttpError as exc:
        st.error(f"YouTube search error: {exc}")
        return []


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_youtube_video_details(yt, video_ids: List[str]) -> List[Dict]:
    if not video_ids:
        return []
    if not check_quota_and_warn(((len(video_ids) + 49) // 50) * QUOTA_COSTS["videos"]):
        return []
    results = []
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i + 50]
        try:
            resp = yt.videos().list(part="snippet,statistics", id=",".join(chunk)).execute()
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
        except HttpError as exc:
            st.warning(f"Could not fetch video metadata batch: {exc}")
    return results


def fetch_youtube_transcript(video_id: str) -> str:
    """Fetch transcript with compatibility across youtube-transcript-api versions."""
    try:
        # Older versions
        if hasattr(YouTubeTranscriptApi, "get_transcript"):
            segments = YouTubeTranscriptApi.get_transcript(video_id)
            return " ".join(seg.get("text", "") for seg in segments)

        # Newer versions
        api = YouTubeTranscriptApi()
        fetched = api.fetch(video_id)
        return " ".join(getattr(seg, "text", "") for seg in fetched)
    except (TranscriptsDisabled, NoTranscriptFound):
        return ""
    except Exception:
        return ""


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_youtube_comments(
    yt,
    video_id: str,
    max_comments: int,
    existing_ids: set,
    comment_kw: str = "",
) -> Tuple[List[Dict], int]:
    comments: List[Dict] = []
    next_page_token = None
    quota_units = 0
    pages_fetched = 0
    max_pages = max(1, (max_comments + 99) // 100)

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
                snippet = item.get("snippet", {})
                top_comment = snippet.get("topLevelComment", {})
                top = top_comment.get("snippet", {})
                comment_id = item.get("id", "")
                if comment_id in existing_ids:
                    continue

                comment_text = top.get("textDisplay", "")
                if comment_kw.strip() and not all_keywords_present(comment_text, comment_kw):
                    continue

                comments.append({
                    "comment_id": comment_id,
                    "comment_text": comment_text,
                    "author": top.get("authorDisplayName", ""),
                    "author_channel_id": (top.get("authorChannelId") or {}).get("value", ""),
                    "comment_published_at": (top.get("publishedAt") or "")[:10],
                    "comment_likes": top.get("likeCount", 0),
                    "comment_reply_count": snippet.get("totalReplyCount", 0),
                    "video_id": video_id,
                })

            next_page_token = resp.get("nextPageToken")
            if not next_page_token:
                break
        except HttpError as exc:
            if "commentsDisabled" in str(exc) or "disabled" in str(exc).lower():
                break
            if "quotaExceeded" in str(exc):
                st.error("Quota exceeded while fetching comments.")
            break
    return comments, quota_units


def fetch_youtube_channel_info(yt, channel_id: str) -> Dict:
    if not channel_id or not check_quota_and_warn(QUOTA_COSTS["channels"]):
        return {}
    try:
        resp = yt.channels().list(part="snippet,statistics", id=channel_id).execute()
        update_quota_tracker(QUOTA_COSTS["channels"])
        items = resp.get("items", [])
        if not items:
            return {}
        ch = items[0]
        snippet = ch.get("snippet", {})
        stats = ch.get("statistics", {})
        return {
            "channel_id": channel_id,
            "channel_name": snippet.get("title", ""),
            "channel_url": f"https://www.youtube.com/channel/{channel_id}",
            "platform": "youtube",
            "country": snippet.get("country", ""),
            "established_date": (snippet.get("publishedAt") or "")[:10],
            "subscriber_count": stats.get("subscriberCount", "0"),
            "video_count": stats.get("videoCount", "0"),
            "total_views": stats.get("viewCount", "0"),
        }
    except HttpError:
        return {}


# =============================================================================
# TikTok placeholder, still integrated but browser-dependent
# =============================================================================

class TikTokScraper:
    def __init__(self, headless: bool = True):
        if not PYKTOK_AVAILABLE:
            st.error("TikTok scraping requires pyktok and browser automation. Streamlit Cloud usually cannot run this reliably. Use the local TikTok requirements file or a backend worker.")
            st.stop()
        try:
            pyk.specify_browser("firefox")
            self.headless = headless
        except Exception as exc:
            st.warning(f"TikTok browser setup issue: {exc}")

    def search_videos(self, keyword: str, max_results: int = 100) -> List[Dict]:
        search_url = f"https://www.tiktok.com/search?q={keyword.replace(' ', '%20')}"
        try:
            video_urls = pyk.get_tiktok_video_urls(search_url, headless=self.headless, n_videos=max_results)
            return [{"video_id": url.split("/")[-1].split("?")[0], "video_url": url, "keyword": keyword} for url in video_urls[:max_results]]
        except Exception as exc:
            st.warning(f"TikTok search error: {exc}")
            return []

    def fetch_video_details(self, video_url: str) -> Dict:
        try:
            metadata = pyk.get_tiktok_metadata(video_url, headless=self.headless)
            return {
                "video_id": metadata.get("id", video_url.split("/")[-1].split("?")[0]),
                "video_url": video_url,
                "title": metadata.get("desc", "")[:500],
                "channel_name": metadata.get("author", ""),
                "channel_id": metadata.get("author_id", metadata.get("author", "")),
                "published_at": str(metadata.get("create_time", ""))[:10] if metadata.get("create_time") else "",
                "view_count": str(metadata.get("play_count", 0)),
                "like_count": str(metadata.get("digg_count", 0)),
                "comment_count": str(metadata.get("comment_count", 0)),
                "description": metadata.get("desc", ""),
                "platform": "tiktok",
                "_transcript": "",
            }
        except Exception:
            return {}

    def fetch_comments(self, video_url: str, video_id: str, max_comments: int, existing_ids: set, comment_kw: str = "") -> Tuple[List[Dict], int]:
        comments = []
        try:
            comment_df = pyk.get_tiktok_comments(video_url, headless=self.headless, n_comments=max_comments)
            if comment_df is not None and not comment_df.empty:
                for _, row in comment_df.iterrows():
                    comment_id = str(row.get("comment_id", ""))
                    comment_text = row.get("text", "")
                    if comment_id in existing_ids:
                        continue
                    if comment_kw.strip() and not all_keywords_present(comment_text, comment_kw):
                        continue
                    comments.append({
                        "comment_id": comment_id,
                        "comment_text": comment_text,
                        "author": row.get("author", ""),
                        "author_channel_id": row.get("author_id", ""),
                        "comment_likes": row.get("digg_count", 0),
                        "comment_reply_count": row.get("reply_comment_total", 0),
                        "comment_published_at": str(row.get("create_time", ""))[:10] if row.get("create_time") else "",
                        "video_id": video_id,
                    })
        except Exception as exc:
            st.warning(f"Error fetching TikTok comments for {video_id}: {exc}")
        return comments, 0

    def fetch_channel_info(self, channel_id: str) -> Dict:
        return {"channel_id": channel_id, "channel_name": channel_id, "channel_url": f"https://www.tiktok.com/@{channel_id}", "platform": "tiktok"}


# =============================================================================
# Filtering helpers
# =============================================================================

def apply_popularity_filters(candidates: List[Dict], publish_date_start: Optional[date], publish_date_end: Optional[date], min_views: int, min_comment_count: int) -> List[Dict]:
    filtered = []
    for v in candidates:
        pub_date = safe_date(v.get("published_at"))
        if publish_date_start and (not pub_date or pub_date < publish_date_start):
            continue
        if publish_date_end and (not pub_date or pub_date > publish_date_end):
            continue
        if min_views and safe_int(v.get("view_count"), 0) < min_views:
            continue
        if min_comment_count and safe_int(v.get("comment_count"), 0) < min_comment_count:
            continue
        filtered.append(v)
    return filtered


# =============================================================================
# Streamlit UI
# =============================================================================

st.set_page_config(page_title="Content Scraper (YouTube + TikTok)", layout="wide")

st.markdown(
    """
    <style>
    div[data-testid="stButton"] > button[kind="primary"] {
        background-color: #d32f2f; color: white; border: none;
    }
    div[data-testid="stButton"] > button[kind="primary"]:hover {
        background-color: #b71c1c; color: white;
    }
    .small-grey { color:#808080; font-size:0.85rem; margin-top:-8px; margin-bottom:8px; }
    .filter-note { color:#1565c0; font-size:0.85rem; font-style:italic; margin-bottom:6px; }
    .quota-warning { background-color:#fff3e0; padding:10px; border-radius:5px; border-left:4px solid #ff9800; margin-bottom:10px; }
    </style>
    """,
    unsafe_allow_html=True,
)

for key, default in {"results": None, "search_params": None, "quota_used_today": 0, "last_saved_project": ""}.items():
    if key not in st.session_state:
        st.session_state[key] = default

st.title("📺🎵 Content Scraper (YouTube + TikTok)")
st.caption("Search → filter → preview → save to Neon/PostgreSQL → export project datasets")

schema = get_db_schema()
conn = get_db_engine()

with st.sidebar:
    quota_remaining = 10000 - st.session_state.get("quota_used_today", 0)
    quota_color = "green" if quota_remaining > 2000 else "orange" if quota_remaining > 500 else "red"
    st.markdown(
        f'<div class="quota-warning">📊 <strong>YouTube API Quota Remaining</strong><br>'
        f'<span style="color:{quota_color}; font-size:24px; font-weight:bold;">{quota_remaining}</span> / 10,000 units<br>'
        f'<span style="font-size:12px;">TikTok requires local/browser setup</span></div>',
        unsafe_allow_html=True,
    )

with st.sidebar:
    st.header("🔍 Platform & Project")
    platform = st.radio("Select Platform", [PLATFORM_YOUTUBE, PLATFORM_TIKTOK], horizontal=True)
    user_name = st.text_input("Your name *", placeholder="e.g. Thu Ha")
    st.markdown('<div class="small-grey">Required; recorded in run log.</div>', unsafe_allow_html=True)
    project_name = st.text_input("Project name *", placeholder="e.g. Vietnam Street Food")
    st.markdown('<div class="small-grey">Case/spacing-insensitive grouping, e.g. Vietnam Street Food = vietnam streetfood.</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("🔎 Keyword Filters")
    st.markdown('<div class="filter-note">Comma-separated keywords use AND logic. Matching is case-insensitive and accent-insensitive.</div>', unsafe_allow_html=True)
    title_kw = st.text_input("Title/Description contains", placeholder="e.g. Vietnam, street food")
    channel_kw = st.text_input("Channel/Author name contains", placeholder="e.g. Best Ever Food Review")
    if platform == PLATFORM_YOUTUBE:
        transcript_kw = st.text_input("Transcript contains (YouTube only)", placeholder="e.g. spicy, Hanoi")
    else:
        transcript_kw = ""
        st.info("TikTok transcripts are not available. Use Title/Description or Comments filters.")
    comment_kw = st.text_input("Comments contain", placeholder="e.g. delicious, cheap")

    st.markdown("---")
    st.subheader("🎯 Video Popularity Filters")
    use_date_filter = st.checkbox("Filter by publish date", value=False)
    col1, col2 = st.columns(2)
    with col1:
        publish_date_start = st.date_input("Published after", value=date(2024, 1, 1)) if use_date_filter else None
        if not use_date_filter:
            st.text_input("Published after", value="Not used", disabled=True)
        min_views = st.number_input("Minimum views", min_value=0, value=0, step=1000)
    with col2:
        publish_date_end = st.date_input("Published before", value=date.today()) if use_date_filter else None
        if not use_date_filter:
            st.text_input("Published before", value="Not used", disabled=True)
        min_comments_on_video = st.number_input("Minimum video comments", min_value=0, value=0, step=100)

    st.markdown("---")
    st.subheader("⚙️ Collection Settings")
    target_comments = st.number_input("🎯 Target number of comments to collect", min_value=0, max_value=50000, value=20000, step=500)
    scan_pool_size = st.number_input("🔍 Search pool size (max videos to scan)", min_value=50, max_value=1000, value=1000, step=50)
    enable_max_comments_per_video = st.checkbox("Limit comments per video", value=True)
    if enable_max_comments_per_video:
        comments_per_video_limit = st.number_input("Max comments per video", min_value=50, max_value=10000, value=500, step=100)
    else:
        comments_per_video_limit = 10000
        st.caption("Will fetch up to around 10,000 top-level comments per video.")
    skip_recently_scraped = st.checkbox("Skip videos scraped in last 7 days", value=False)

    st.markdown("---")
    st.subheader("💾 Optional Saves")
    save_transcripts = st.checkbox("Save full transcripts (YouTube only)", value=True)


run_search = st.button("▶ Run Scrape", type="primary")

if run_search:
    if not test_database_connection(conn, schema):
        st.stop()
    if not user_name.strip():
        st.error("Your name is required.")
        st.stop()
    if not project_name.strip():
        st.error("Project name is required.")
        st.stop()
    if not any([title_kw.strip(), channel_kw.strip(), transcript_kw.strip(), comment_kw.strip()]):
        st.error("Please enter at least one keyword filter: Title/Description, Channel/Author, Transcript, or Comments.")
        st.stop()

    params = {
        "user_name": user_name.strip(),
        "project_name": project_name.strip(),
        "platform": platform,
        "title_kw": title_kw.strip(),
        "channel_kw": channel_kw.strip(),
        "transcript_kw": transcript_kw.strip(),
        "comment_kw": comment_kw.strip(),
        "publish_date_start": publish_date_start,
        "publish_date_end": publish_date_end,
        "min_views": min_views,
        "min_comments_on_video": min_comments_on_video,
        "scan_pool_size": scan_pool_size,
        "target_comments": target_comments,
        "comments_per_video_limit": "unlimited" if not enable_max_comments_per_video else comments_per_video_limit,
        "comments_per_video_limit_numeric": comments_per_video_limit,
        "save_transcripts": save_transcripts and platform == PLATFORM_YOUTUBE,
    }

    existing_comment_ids = get_existing_comment_ids(conn, schema)
    st.info(f"Found {len(existing_comment_ids)} existing comments in the database. Duplicates will be skipped.")

    recently_scraped_ids = get_recently_scraped_video_ids(conn, schema, days=7) if skip_recently_scraped else set()
    if recently_scraped_ids:
        st.info(f"Will skip {len(recently_scraped_ids)} videos scraped in the last 7 days.")

    videos_processed: List[Dict] = []
    all_comments: List[Dict] = []
    total_quota_used = 0
    channel_info_list: List[Dict] = []

    try:
        if platform == PLATFORM_YOUTUBE:
            yt = get_yt_client()
            seed_query = build_seed_query(title_kw, channel_kw, transcript_kw, comment_kw)
            with st.spinner(f"Searching YouTube for seed query: {seed_query!r}..."):
                video_ids = search_youtube_video_ids(yt, seed_query, max_results=int(scan_pool_size))
            if not video_ids:
                st.warning("No videos returned by YouTube search.")
                st.stop()

            with st.spinner(f"Fetching metadata for {len(video_ids)} videos..."):
                candidates = fetch_youtube_video_details(yt, video_ids)

            if title_kw.strip():
                candidates = [v for v in candidates if all_keywords_present((v.get("title", "") + " " + v.get("description", "")), title_kw)]
            if channel_kw.strip():
                candidates = [v for v in candidates if all_keywords_present(v.get("channel_name", ""), channel_kw)]
            candidates = apply_popularity_filters(candidates, publish_date_start, publish_date_end, int(min_views), int(min_comments_on_video))
            if skip_recently_scraped:
                candidates = [v for v in candidates if v.get("video_id") not in recently_scraped_ids]

            if not candidates:
                st.warning("No videos matched the metadata filters.")
                st.stop()

            # Transcript filtering and optional transcript saving share the same fetch step.
            if transcript_kw.strip() or save_transcripts:
                st.info(f"Checking/downloading transcripts for {len(candidates)} videos...")
                prog = st.progress(0)
                filtered_candidates = []
                for i, v in enumerate(candidates):
                    transcript_text = fetch_youtube_transcript(v["video_id"])
                    v["_transcript"] = transcript_text
                    if not transcript_kw.strip() or all_keywords_present(transcript_text, transcript_kw):
                        filtered_candidates.append(v)
                    prog.progress((i + 1) / max(len(candidates), 1))
                prog.empty()
                candidates = filtered_candidates
                if not candidates:
                    st.warning("No videos matched the transcript keyword. Try broader transcript terms or a larger search pool.")
                    st.stop()

            st.info(f"Starting comment collection. Target: {target_comments} comments")
            comment_progress = st.progress(0)
            comment_status = st.empty()
            total_comments_fetched = 0

            for video in candidates:
                if total_comments_fetched >= target_comments:
                    break
                remaining_needed = int(target_comments) - total_comments_fetched
                to_fetch = min(int(comments_per_video_limit), remaining_needed) if target_comments else int(comments_per_video_limit)
                comment_status.info(f"Fetching comments for: {video.get('title', '')[:80]}... needs {remaining_needed} more")
                video_comments, quota_used = fetch_youtube_comments(yt, video["video_id"], to_fetch, existing_comment_ids, comment_kw=comment_kw)
                total_quota_used += quota_used
                for c in video_comments:
                    c["video_title"] = video.get("title", "")
                    c["video_url"] = video.get("video_url", "")
                all_comments.extend(video_comments)
                total_comments_fetched += len(video_comments)
                if video_comments or not comment_kw.strip():
                    videos_processed.append(video)
                for c in video_comments:
                    existing_comment_ids.add(c["comment_id"])
                if target_comments:
                    comment_progress.progress(min(total_comments_fetched / int(target_comments), 1.0))
            comment_status.empty()
            comment_progress.empty()

            unique_channel_ids = list({v.get("channel_id") for v in videos_processed if v.get("channel_id")})
            with st.spinner(f"Fetching channel info for {len(unique_channel_ids)} channels..."):
                for cid in unique_channel_ids:
                    info = fetch_youtube_channel_info(yt, cid)
                    if info:
                        channel_info_list.append(info)

        else:
            if not PYKTOK_AVAILABLE:
                st.error("TikTok scraping requires pyktok + Selenium + Firefox/geckodriver. It is best run locally or on a backend worker, not Streamlit Cloud.")
                st.stop()
            tiktok = TikTokScraper(headless=True)
            seed_query = build_seed_query(title_kw, channel_kw, "", comment_kw)
            with st.spinner(f"Searching TikTok for {seed_query!r}..."):
                video_list = tiktok.search_videos(seed_query, max_results=int(scan_pool_size))
            candidates = [tiktok.fetch_video_details(v["video_url"]) for v in video_list]
            candidates = [v for v in candidates if v]
            if title_kw.strip():
                candidates = [v for v in candidates if all_keywords_present((v.get("title", "") + " " + v.get("description", "")), title_kw)]
            if channel_kw.strip():
                candidates = [v for v in candidates if all_keywords_present(v.get("channel_name", ""), channel_kw)]
            candidates = apply_popularity_filters(candidates, publish_date_start, publish_date_end, int(min_views), int(min_comments_on_video))

            total_comments_fetched = 0
            for video in candidates:
                if total_comments_fetched >= target_comments:
                    break
                remaining_needed = int(target_comments) - total_comments_fetched
                to_fetch = min(int(comments_per_video_limit), remaining_needed) if target_comments else int(comments_per_video_limit)
                video_comments, _ = tiktok.fetch_comments(video["video_url"], video["video_id"], to_fetch, existing_comment_ids, comment_kw=comment_kw)
                for c in video_comments:
                    c["video_title"] = video.get("title", "")
                    c["video_url"] = video.get("video_url", "")
                all_comments.extend(video_comments)
                total_comments_fetched += len(video_comments)
                if video_comments or not comment_kw.strip():
                    videos_processed.append(video)
                for c in video_comments:
                    existing_comment_ids.add(c["comment_id"])
            unique_channel_ids = list({v.get("channel_id") for v in videos_processed if v.get("channel_id")})
            channel_info_list = [tiktok.fetch_channel_info(cid) for cid in unique_channel_ids]

    except Exception as exc:
        st.exception(exc)
        st.stop()

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    videos_df = pd.DataFrame([{
        "video_id": v.get("video_id", ""),
        "title": v.get("title", "")[:500],
        "channel_id": v.get("channel_id", ""),
        "channel_name": v.get("channel_name", ""),
        "platform": platform.lower(),
        "published_at": v.get("published_at", ""),
        "view_count": v.get("view_count", "0"),
        "like_count": v.get("like_count", "0"),
        "comment_count": v.get("comment_count", "0"),
        "video_url": v.get("video_url", ""),
        "description": v.get("description", ""),
        "comments_fetched_from_video": comments_per_video_limit if enable_max_comments_per_video else "all",
        "search_title_kw": title_kw,
        "search_channel_kw": channel_kw,
        "search_comment_kw": comment_kw,
        "project": project_name.strip(),
        "scraped_by": user_name.strip(),
        "scraped_at": now_str,
    } for v in videos_processed])

    transcripts_df = pd.DataFrame([{
        "video_id": v.get("video_id", ""),
        "full_transcript": v.get("_transcript", ""),
        "scraped_by": user_name.strip(),
        "scraped_at": now_str,
        "platform": platform.lower(),
    } for v in videos_processed if v.get("_transcript")])

    comments_df = pd.DataFrame([{**c, "project": project_name.strip(), "scraped_by": user_name.strip(), "scraped_at": now_str, "platform": platform.lower()} for c in all_comments])

    channels_df = pd.DataFrame(channel_info_list)
    if not channels_df.empty:
        channels_df["project"] = project_name.strip()
        channels_df["scraped_by"] = user_name.strip()
        channels_df["scraped_at"] = now_str
        channels_df["platform"] = platform.lower()

    st.session_state["results"] = {
        "videos_df": videos_df,
        "transcripts_df": transcripts_df,
        "comments_df": comments_df,
        "comments_preview_df": comments_df.head(500).copy(),
        "channels_df": channels_df,
        "total_comments_collected": len(comments_df),
        "videos_processed": len(videos_df),
        "quota_used_this_run": total_quota_used,
        "platform": platform,
    }
    st.session_state["search_params"] = params
    st.success(f"Scraping complete: {len(comments_df)} comments from {len(videos_df)} videos.")
    st.rerun()


# =============================================================================
# Preview, save, and download
# =============================================================================

if st.session_state["results"] is not None:
    res = st.session_state["results"]
    params = st.session_state["search_params"]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Project", params["project_name"])
    col2.metric("Platform", params["platform"])
    col3.metric("Videos processed", res["videos_processed"])
    col4.metric("Comments collected", res["total_comments_collected"])

    if params["platform"] == PLATFORM_YOUTUBE:
        st.info(f"YouTube API quota used this run: {res['quota_used_this_run']} units")

    st.subheader("Preview: Videos")
    preview_cols = ["title", "channel_name", "platform", "published_at", "view_count", "like_count", "comment_count", "video_url"]
    st.dataframe(res["videos_df"][[c for c in preview_cols if c in res["videos_df"].columns]].head(20), use_container_width=True)

    if not res["channels_df"].empty:
        st.subheader("Preview: Channels / Creators")
        ch_cols = ["channel_name", "platform", "channel_url", "subscriber_count", "follower_count", "video_count", "total_views"]
        st.dataframe(res["channels_df"][[c for c in ch_cols if c in res["channels_df"].columns]].head(20), use_container_width=True)

    if not res["comments_preview_df"].empty:
        st.subheader(f"Preview: Comments (first 20 of {res['total_comments_collected']})")
        cm_cols = ["video_title", "author", "comment_text", "comment_published_at", "comment_likes", "comment_reply_count"]
        st.dataframe(res["comments_preview_df"][[c for c in cm_cols if c in res["comments_preview_df"].columns]].head(20), use_container_width=True)

    if not res["transcripts_df"].empty:
        st.subheader(f"Preview: Transcripts ({len(res['transcripts_df'])} videos)")
        st.dataframe(res["transcripts_df"][["video_id", "full_transcript"]].head(5), use_container_width=True)

    st.markdown("---")
    if st.button("💾 Save to Database", type="primary"):
        if not test_database_connection(conn, schema):
            st.stop()
        try:
            project_id, canonical_name = get_or_create_project(conn, schema, params["project_name"], params["user_name"])
            run_id = create_scrape_run(conn, schema, project_id, params)
            counts = save_results_to_database(conn, schema, project_id, run_id, res, params)
            complete_scrape_run(
                conn, schema, run_id, "completed",
                videos_found=len(res["videos_df"]),
                videos_processed=len(res["videos_df"]),
                comments_collected=len(res["comments_df"]),
                quota_used=res.get("quota_used_this_run", 0),
            )
            st.session_state["last_saved_project"] = canonical_name
            st.success(
                f"Saved to database for project '{canonical_name}': "
                f"{counts['videos']} videos, {counts['channels']} channels, "
                f"{counts['transcripts']} transcripts, {counts['comments']} comments."
            )
        except Exception as exc:
            st.error("Save failed. Please check your Neon connection and schema.")
            st.exception(exc)

# Always show project export once DB is reachable.
st.markdown("---")
st.subheader("📥 Download Project Data")
export_project_name = st.text_input(
    "Project name to export",
    value=st.session_state.get("last_saved_project") or (st.session_state.get("search_params") or {}).get("project_name", ""),
    placeholder="Type the project name exactly or with minor case/spacing differences",
)

if export_project_name.strip():
    if st.button("Prepare project export"):
        if not test_database_connection(conn, schema):
            st.stop()
        try:
            sheets = read_project_exports(conn, schema, export_project_name.strip())
            total_rows = sum(len(df) for df in sheets.values())
            if total_rows == 0:
                st.warning("No rows found for that project name. Check spelling or whether data has been saved to this database.")
            else:
                st.session_state["export_sheets"] = sheets
                st.session_state["export_project_name"] = export_project_name.strip()
                st.success(f"Prepared export with {total_rows:,} total rows across {len(sheets)} sheets.")
        except Exception as exc:
            st.error("Export preparation failed.")
            st.exception(exc)

if "export_sheets" in st.session_state:
    sheets = st.session_state["export_sheets"]
    base_name = canonical_project_key(st.session_state.get("export_project_name", "project")) or "project"
    excel_bytes = make_excel_bytes(sheets)
    zip_bytes = make_csv_zip_bytes(sheets)
    col_a, col_b = st.columns(2)
    with col_a:
        st.download_button(
            "Download Excel workbook (.xlsx)",
            data=excel_bytes,
            file_name=f"{base_name}_scraper_export.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with col_b:
        st.download_button(
            "Download CSV package (.zip)",
            data=zip_bytes,
            file_name=f"{base_name}_scraper_export_csv.zip",
            mime="application/zip",
        )
