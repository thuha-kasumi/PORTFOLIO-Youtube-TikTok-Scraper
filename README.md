# Multi-Platform Content Scraper (YouTube + TikTok)

A reusable Streamlit-based scraper for collecting YouTube and TikTok video metadata, channel/creator metadata, transcripts, and comments for portfolio analytics projects. The tool is designed for topic-based research: change the search keywords, run a new scrape, and store normalized results in a shared PostgreSQL database such as Neon.

> Current production path: YouTube scraping on Streamlit Cloud + Neon PostgreSQL. TikTok is kept in the app design, but browser-based scraping is best run locally or in a backend environment that supports Firefox/geckodriver.

---

## Features

| Feature | Detail |
|---|---|
| **Reusable topic workflow** | Run different research topics by changing title, channel/author, transcript, and comment keywords. |
| **YouTube API integration** | Uses YouTube Data API v3 for search, video metadata, comments, and channel metadata. |
| **PostgreSQL data layer** | Stores videos, comments, channels, transcripts, projects, and run logs in Neon/PostgreSQL. |
| **Target-based comment collection** | Set a target number of comments; scraper stops when the target is reached or matching videos are exhausted. |
| **Incremental saving** | Saves comment batches during scraping to reduce data-loss risk. |
| **Duplicate prevention** | Uses stable IDs and database constraints to avoid re-saving the same videos/comments. |
| **Keyword filters** | Supports AND-style filters for title/description, channel/author, transcript, and comments. |
| **Date and popularity filters** | Filter by publish date range, minimum views, and minimum video comments. |
| **Quota tracking** | Displays YouTube API quota usage estimates during the session. |
| **Preview tables** | Shows videos, channels, and sample comments before final metadata save. |
| **Portfolio-ready logging** | Captures run metadata such as platform, keywords, user, quota usage, and result counts. |

---

## Data Layer

This project now uses PostgreSQL instead of Google Sheets as the primary storage layer. Neon is recommended because it is easy to share with collaborators and works well with Streamlit Cloud.

Recommended core tables:

| Table | Purpose |
|---|---|
| `projects` | One row per analysis project or topic. |
| `scrape_runs` | One row per scraper execution. |
| `channels` | One row per YouTube channel or TikTok creator. |
| `videos` | One row per video. |
| `transcripts` | One row per video transcript, when available. |
| `comments` | One row per comment. |
| `project_videos` | Many-to-many bridge between projects and videos. |
| `project_comments` | Many-to-many bridge between projects and comments. |

This structure allows one raw video/comment to be reused across multiple projects without duplication.

---

## Repository Structure

```text
content-scraper/
├── app.py                         # Main Streamlit app
├── requirements.txt               # Streamlit Cloud dependencies
├── README.md                      # Project documentation
├── LICENSE                        # MIT license
├── secrets.toml.example           # Example Streamlit secrets file
└── sql/
    └── neon_social_scraper_schema.sql   # Optional database schema setup
```

---

## Setup

### 1. Clone the project

```bash
git clone https://github.com/YOUR_USERNAME/portfolio-youtube-tiktok-scraper.git
cd portfolio-youtube-tiktok-scraper
pip install -r requirements.txt
```

### 2. Create a Neon PostgreSQL database

1. Create a Neon project.
2. Create or use the default database, for example `neondb`.
3. Copy the pooled connection string.
4. Add the connection string to Streamlit secrets as `DATABASE_URL`.

Example:

```toml
YOUTUBE_API_KEY = "YOUR_YOUTUBE_DATA_API_V3_KEY"
DATABASE_URL = "postgresql+psycopg2://USER:PASSWORD@HOST/DATABASE?sslmode=require"
```

Do not commit `.streamlit/secrets.toml` to GitHub.

### 3. Create the database schema

Run the PostgreSQL schema file in Neon SQL Editor or through a local SQL client:

```bash
psql "$DATABASE_URL" -f sql/neon_social_scraper_schema.sql
```

### 4. Create a YouTube Data API key

1. Open Google Cloud Console.
2. Enable **YouTube Data API v3**.
3. Create an API key.
4. Add the key to Streamlit secrets as `YOUTUBE_API_KEY`.

---

## Streamlit Cloud Deployment

Use this `requirements.txt` for Streamlit Cloud:

```txt
streamlit>=1.35.0
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.9
google-api-python-client>=2.130.0
youtube-transcript-api>=0.6.2
pandas>=2.0.0
tenacity>=8.2.0
```

Do not include `streamlit-gsheets-connection` in the deployed requirements. The app now uses PostgreSQL.

---

## Usage

1. Open the Streamlit app.
2. Select the platform.
3. Enter your name for run logging.
4. Add keyword filters.
5. Optionally enable publish-date filtering.
6. Set minimum views or minimum video comments.
7. Set target comments and scan pool size.
8. Run the scrape.
9. Review preview tables.
10. Save final metadata to PostgreSQL.

Comments are saved incrementally during collection. Final save stores videos, channels, transcripts, and the run log.

---

## Suggested Research Workflow

For each portfolio topic:

1. Create a project label, such as `Vietnam QSR Market Entry`.
2. Run several keyword groups, such as:
   - `Vietnam street food`
   - `McDonald's Vietnam`
   - `KFC Vietnam`
   - `foreign fast food Vietnam`
   - `Vietnam food delivery`
3. Collect broad raw data first.
4. Clean, standardize, translate, and classify comments later.
5. Analyze themes such as price sensitivity, taste expectations, localization, brand trust, service quality, and convenience.

---

## YouTube Quota Notes

Approximate quota costs:

| Operation | Approximate cost |
|---|---:|
| Search request | 100 units |
| Video metadata batch | 1 unit per up to 50 videos |
| Comment page | 1 unit per up to 100 comments |
| Channel metadata | 1 unit per channel |

A 1,500-comment test run may use roughly 50-100 units depending on the number of videos and channels processed.

---

## TikTok Notes

TikTok scraping is browser-automation based and may not run reliably on Streamlit Cloud. Keep TikTok integrated in the interface for a unified workflow, but run TikTok collection locally or on a backend that supports browser automation.

Suggested optional local requirements:

```txt
pyktok>=0.2.0
selenium>=4.15.0
```

---

## Troubleshooting

| Problem | Likely cause | Fix |
|---|---|---|
| Dependency install fails | Old Google Sheets package still in requirements | Remove `streamlit-gsheets-connection`. |
| Database connection fails | Invalid `DATABASE_URL` or expired Neon password | Re-copy the pooled Neon connection string and confirm `sslmode=require`. |
| No videos matched filters | Filters are too narrow | Remove filters one by one and increase scan pool size. |
| Few comments collected | Matching videos have limited comments | Increase scan pool size or broaden keywords. |
| TikTok fails on Streamlit Cloud | Browser automation unavailable | Run TikTok locally or on a backend server. |

---

## Portfolio Case Study Value

This project demonstrates:

| Competency | Evidence |
|---|---|
| API integration | YouTube Data API v3 usage with quota awareness. |
| Data engineering | PostgreSQL schema, deduplication, incremental inserts. |
| Research tooling | Reusable scraper for multiple business topics. |
| Product thinking | Streamlit UI with filters, previews, and run tracking. |
| Analytics readiness | Normalized tables for later cleaning, NLP, and dashboarding. |

---

## License

MIT License. See `LICENSE`.
