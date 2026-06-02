# Multi-Platform Content Scraper (YouTube + TikTok)

A production-ready Streamlit tool to search YouTube and TikTok, apply keyword filters, collect comments with target-based quotas, and save everything to Google Sheets. Built for scalable data collection (10,000+ comments per day) with intelligent quota management and incremental saving.

---

## Features

| Feature | Detail |
|---|---|
| **Dual-platform support** | Toggle between YouTube and TikTok in the same interface |
| **Target-based comment collection** | Set a specific target (e.g., 5,000 comments) — scraper stops automatically when reached |
| **Incremental comment saving** | Comments saved every 100 rows during collection (no memory limits, no data loss) |
| **4 AND-logic keyword filters** | Title/Description · Channel/Author · Transcript (YouTube only) · Comments |
| **Video popularity filters** | Date range · Minimum views · Minimum comment count on video |
| **YouTube API quota dashboard** | Real-time tracking of daily 10,000 unit limit with visual warnings |
| **TikTok free scraping** | Uses browser automation (pyktok) — no API key or quota limits |
| **Optional transcript save** | One row per video in the `transcripts` sheet (YouTube only, FREE) |
| **Optional comment limit per video** | Toggle on/off — prevent bias from viral videos or fetch all comments |
| **Skip recently scraped videos** | Avoid re-processing videos from the last 7 days |
| **Duplicate detection** | Rows already in a sheet (matched by ID) are skipped automatically |
| **Channel/creator info sheet** | Subscribers, video count, total views, bio, and more |
| **Run log sheet** | Timestamp, user, platform, keywords, counts saved, quota used |
| **Preview before saving** | Inspect all result sets before committing to Sheets |
| **Retry logic with backoff** | Automatically retries failed API calls (handles rate limits gracefully) |

---

## Google Spreadsheet Sheet Structure

Create a new Google Spreadsheet and add these **5 sheets** (tabs) — names are case-sensitive:

| Sheet name | Purpose |
|---|---|
| `videos` | One row per scraped video (includes platform column) |
| `transcripts` | One row per video with full transcript (YouTube only) |
| `comments` | One row per comment (includes platform column) |
| `channels` | One row per unique channel/creator (includes platform column) |
| `run_log` | One row per scrape run |

The tool will create headers automatically on first save — just leave the sheets empty.

---

## Setup

### 1 — Clone / copy the project

```bash
git clone https://github.com/YOUR_USERNAME/content-scraper.git
cd content-scraper
pip install -r requirements.txt
```

### 2 — YouTube Data API v3 key

1. Go to [Google Cloud Console](https://console.cloud.google.com/).
2. Create a project (or use an existing one).
3. Enable **YouTube Data API v3** under *APIs & Services → Library*.
4. Under *APIs & Services → Credentials*, click **Create Credentials → API key**.
5. Copy the key.

> **Quota note:**
> The free tier gives 10,000 units/day.
> A typical scrape run of 50 videos uses roughly 300–500 units.
> Transcript fetching uses `youtube-transcript-api` (no quota cost).
> Comment fetching costs ~1 unit per page of 100 comments.

> **YouTube Quota Breakdown Simulation (for 5,000 comments):**
> Search: 100 units (one-time)
> Video metadata: ~1 unit per 50 videos
> Comments: 1 unit per 100 comments (≈50 units for 5,000 comments)
> Channels: 1 unit per unique channel
> **Total: ~150-200 units for 5,000 comments** — well within the 10,000 daily limit.

### 3 — Google Sheets service account

1. In the same GCP project, go to *IAM & Admin → Service Accounts*.
2. Create a new service account, give it no special roles.
3. Create a JSON key for it and download it.
4. **Share your Google Spreadsheet** with the service-account email address (Editor role).

### 4 — Streamlit secrets

1. Create the .streamlit directory and secrets.toml file:
```bash
mkdir -p .streamlit
touch .streamlit/secrets.toml
```

2. Edit .streamlit/secrets.toml and add the following:
```toml
# .streamlit/secrets.toml
YOUTUBE_API_KEY = "AIzaSyYourActualYouTubeAPIKeyHere"

[connections.gsheets]
type = "service_account"
project_id = "your-project-id"
private_key_id = "abc123def456..."
private_key = "-----BEGIN PRIVATE KEY-----\nMIIEv...\n-----END PRIVATE KEY-----\n"
client_email = "your-service-account@your-project.iam.gserviceaccount.com"
client_id = "123456789012345678901"
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
spreadsheet = "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit"
```

**⚠️Never commit `secrets.toml`** — it is listed in `.gitignore`.

### 5 — TikTok Dependencies (Optional)

TikTok scraping requires additional setup:
1. Install Firefox (recommended) or Chrome:
> macOS: `brew install firefox`
> Ubuntu: `sudo apt install firefox`
> Windows: Download from Mozilla (https://www.mozilla.org/firefox/)
2. Install geckodriver (for Firefox automation):
>  macOS: `brew install geckodriver`
> Ubuntu: `sudo apt install firefox-geckodriver`
> Windows: Download from GitHub releases (https://github.com/mozilla/geckodriver/releases)

**💡 Note:** TikTok scraping uses browser automation and may open a window during first run. This is normal.

### 6 — Run

```bash
streamlit run app.py
```

---

## Usage

1. Select platform (YouTube or TikTok) in the sidebar
2. Enter your name (recorded in run log)
3. Fill keyword filters (all active filters must match — AND logic)
4. Set popularity filters (optional): date range, minimum views, minimum comments
5. Set target comments (e.g., 5,000) — scraper stops automatically
6. Configure collection settings:
> Search pool size (how many videos to scan)
> Enable/disable comment limit per video
> Skip recently scraped videos (7 days)
7. Click ▶ Run Scrape
8. Review preview tables
9. Click 💾 SAVE REMAINING DATA to save videos, channels, and transcripts

**💡 Comments are saved automatically during scraping** — no second save step needed!

---

## Platform Comparison

| Aspect | YouTube | TikTok
|---|---|---|
| API Cost | Quota-based (10k units/day free) | Free (browser automation)
| Speed | Fast (~1-2 sec/video) | Slower (~5-10 sec/video)
| Transcripts | Available (auto-generated) | Not available
| Comment depth | Top-level only | Top-level only
| Rate limiting | Official API limits | Browser-dependent
| Best for | Structured analysis, transcripts | Emerging trends, free scraping

---

## Filter behaviour

1. YouTube
| Filter | What it checks | Cost | Speed
|---|---|---|---|
| Title/Description | Video title + description | Free (metadata) | Fast
| Channel name | Channel display name | Free (metadata) | Fast
| Transcript | Full auto-transcript text | Free (no API cost) | Slow (1-2 sec/video)
| Comments | Any top-level comment text | ~1 unit per 100 comments | Medium
| Date range | Published date | Free (metadata) | Fast
| Min views | View count | Free (metadata) | Fast

2. TikTok
| Filter | What it checks | Cost | Speed
|---|---|---|---|
| Title/Description | Video description text | Free | Medium
| Author name | Creator username | Free | Medium
| Comments | Any comment text | Free | Slow
| Date range | Published date | Free | Medium

**Important notes:**
> When multiple filters are set, **all** must match for a video to be included.
> YouTube transcript filter **only applies to videos WITH transcripts** (videos without transcript are skipped).
> TikTok does not support transcript search.

---

## Screenshots

1. Platform Selector & Quota Dashboard
┌─────────────────────────────────────────────┐
│ 🔍 Platform & Search                        │
│ ○ YouTube  ● TikTok                         │
│                                             │
│ ┌─────────────────────────────────────────┐ │
│ │ 📊 YouTube API Quota Remaining          │ │
│ │                                         │ │
│ │     9,850 / 10,000                      │ │
│ │     ████████░░ 98%                      │ │
│ │                                         │ │
│ │ TikTok has no quota limits 🎵           │ │
│ └─────────────────────────────────────────┘ │
└─────────────────────────────────────────────┘

2. Popularity Filters
┌─────────────────────────────────────────────┐
│ 🎯 Video Popularity Filters                 │
│                                             │
│ Published after    [2024-01-01]            │
│ Published before   [2024-12-31]            │
│ Minimum views      [1000    ] (+)          │
│ Minimum comments   [100     ] (+)          │
└─────────────────────────────────────────────┘

3. Collection Settings
┌─────────────────────────────────────────────┐
│ ⚙️ Collection Settings                      │
│                                             │
│ 🎯 Target comments:  [5000    ] (+)        │
│ 🔍 Search pool size: [200     ] (+)        │
│                                             │
│ ☑️ Limit comments per video                 │
│    Max per video:    [500     ] (+)        │
│                                             │
│ ☑️ Skip videos scraped in last 7 days      │
└─────────────────────────────────────────────┘

---

## ## Support and Troubleshooting

For issues:
1. Check the Troubleshooting section.
2. Verify your API key and sheet permissions.
3. Ensure all 5 sheets exist in your Google Spreadsheet.
4. For TikTok, confirm Firefox and geckodriver are installed.

Troubleshooting:

1. YouTube Issues
| Problem | Solution
|---|---|
| Quota exceeded | Check sidebar for usage (resets daily at UTC midnight). Reduce target or wait until tomorrow.
| No transcripts found | Not all videos have transcripts. Try increasing search pool size.
| Comments not saving | Verify `comments` sheet exists and service account has Editor access.
| "quotaExceeded" during comments | Comments cost 1 unit/100. 10,000 comments = 100 units (should be fine). Check other API usage.

2. TikTok Issues
| Problem | Solution
|---|---|
| pyktok not found | Run `pip install pyktok selenium`.
| Browser not launching | Install Firefox and geckodriver (see Setup section).
| No videos found | TikTok search may be rate-limited. Try different keywords or wait a few minutes.
| Comments not scraping | TikTok may require login. Some videos have comments disabled.
| Slow performance | TikTok uses browser automation — 5-10 seconds per video is normal.

3. General Issues
| Problem | Solution
|---|---|
| "Your name is required" | Enter any identifier in the sidebar (e.g., "John" or "project_alpha")
| No videos matched filters | Try removing filters one by one to identify the blocker
| Sheet connection error | Verify `secrets.toml` has correct spreadsheet URL and service account has access
| Duplicate comments not skipped | Tool uses `comment_id` for deduplication — existing comments won't be re-saved

---

## Performance Benchmarks

1. YouTube (with API)
| Target comments | Videos processed | API calls | Time (est.) | Quota used
|---|---|---|---|---|
| 1,000 | 20-30 | 10-15 | 2-3 min | ~40 units
| 5,000 | 100-150 | 50-75 | 10-15 min | ~150 units
| 10,000 | 200-300 | 100-150 | 20-30 min | ~250 units
| 50,000 | 1,000+ | 500+ | 2-3 hours | ~1,000 units

2. TikTok (browser automation)
| Target comments | Videos processed | Time (est.) | Quota used
|---|---|---|---|
| 500 | 10-20 | 5-10 min | Browser launches per video
| 1,000 | 20-40 | 15-20 min | Can be parallelized
| 5,000 | 100-200 | 1-2 hours | Best for overnight runs

**Note:** All YouTube benchmarks within free daily quota (10,000 units/day).

---

## Project Structure

```text
content-scraper/
├── app.py                    # Main application (YouTube + TikTok)
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── .gitignore               # Git ignore rules
└── .streamlit/
    └── secrets.toml         # API keys (NOT committed)
```

---

## Portfolio Case Study Notes

This project demonstrates:
| Competency | Evidence
|---|---|
| API integration | YouTube Data API v3 with quota management
| Rate limit handling | Retry logic with exponential backoff
| Scalable data collection | Target-based collection, incremental saving
| Platform trade-offs | YouTube (structured, quota) vs TikTok (free, slower)
| Data deduplication | Comment ID tracking across sessions
| User experience | Preview before save, progress bars, real-time status
| Production considerations | Secrets management, error handling, logging

---

## Roadmap / Future Enhancements

> **Instagram Reels** support (Graph API or browser automation)
> **Facebook public page comments** (Graph API, requires app review)
> **Comment replies** (nested comments for YouTube and TikTok)
> **Sentiment analysis** integration
> **Export to CSV/JSON** option (in addition to Google Sheets)
> **Parallel processing** for faster TikTok scraping
> **Docker container** for easy deployment

---

## License

MIT — free for academic and portfolio use.

---

## Version History

| Version | Date | Changes
|---|---|---|
| v1.0 | 2026-06-02 | YouTube-TikTok multi-platform release with keyword filters and batch saving, popularity filters, deduplication.

---

## Acknowledgements

> YouTube Data API v3 (https://developers.google.com/youtube/v3)
> youtube-transcript-api (https://github.com/jdepoix/youtube-transcript-api)
> pyktok for TikTok scraping (https://github.com/dfreelon/pyktok)
> Streamlit for the UI framework (https://streamlit.io/)

---

## .gitignore

```
.streamlit/secrets.toml
__pycache__/
*.pyc
.env
```
