# Twitter/X Scraper

**Name:** `twitter-scraper`  
**Version:** `1.0.0`  
**Runtime:** `python3`  
**Browser:** `chromium` (via Playwright)

## Overview

Two-phase scraping system for Twitter/X public profiles:

1. **Profile Discovery** — Find Twitter accounts via Google Custom Search API or DuckDuckGo (searches `x.com` and `twitter.com`)
2. **Browser Scraping** — Scrape public profiles using Playwright with anti-detection (no login required)

## Setup

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install Playwright browser
playwright install chromium
```

### Google Custom Search API (Optional)

For discovery via Google:

1. Create API key at [Google Cloud Console](https://console.cloud.google.com/)
2. Create Programmable Search Engine at [cse.google.com](https://cse.google.com/) scoped to `x.com` and `twitter.com`
3. Set values in `config/scraper_config.json`:
   ```json
   {
     "google_search": {
       "enabled": true,
       "api_key": "YOUR_API_KEY",
       "search_engine_id": "YOUR_CX_ID"
     }
   }
   ```

If not configured, discovery falls back to DuckDuckGo (no API key needed).

## Commands

### Discover Profiles

```bash
# Interactive mode
python main.py discover

# Specific location + category
python main.py discover --location "New York" --category tech --count 15

# Batch mode (multiple cities x categories)
python main.py discover --batch

# JSON output (for agent integration)
python main.py discover --location "Miami" --category crypto --output json
```

### Scrape Profiles

```bash
# Scrape from a queue file
python main.py scrape data/queue/New_York_tech_20260217_120000.json

# Scrape a single profile
python main.py scrape --username elonmusk

# Scrape multiple usernames (manual list)
python main.py scrape --usernames user1,user2,user3 --category tech

# Headless mode
python main.py scrape --username nasa --headless

# JSON output
python main.py scrape --username elonmusk --output json
```

### List Queue Files

```bash
python main.py list
```

### Export Data

```bash
# Export to both JSON and CSV
python main.py export --format both

# JSON only
python main.py export --format json

# CSV only (produces profiles CSV + tweets CSV)
python main.py export --format csv
```

## Directory Structure

```
twitter-scraper/
├── main.py                  # CLI entry point
├── scraper.py               # Core TwitterScraper class
├── discovery.py             # Profile discovery (Google/DuckDuckGo)
├── anti_detection.py        # Anti-detection system
├── requirements.txt
├── SKILL.md
├── config/
│   └── scraper_config.json  # Configuration
├── data/
│   ├── browser_fingerprints.json
│   ├── output/              # Per-profile JSON output
│   └── queue/               # Discovery queue files
└── thumbnails/              # Downloaded media
```

## Data State

| Directory | Contents |
|-----------|----------|
| `data/queue/` | Queue files: `{location}_{category}_{timestamp}.json` |
| `data/output/` | Scraped profiles: `{username}.json` |
| `thumbnails/` | Downloaded images: `{username}/profile_*.jpg`, `{username}/tweet_media_*.jpg` |

## Output Schema

### Profile JSON (`data/output/{username}.json`)

```json
{
  "username": "elonmusk",
  "display_name": "Elon Musk",
  "bio": "...",
  "followers": 180000000,
  "following": 800,
  "tweets_count": 45000,
  "is_verified": true,
  "profile_pic_url": "https://...",
  "profile_pic_local": "thumbnails/elonmusk/profile_abc123.jpg",
  "user_location": "Mars & Earth",
  "join_date": "June 2009",
  "website": "https://x.ai",
  "influencer_tier": "mega",
  "category": "tech",
  "scrape_location": "New York",
  "scraped_at": "2026-02-17T12:00:00",
  "recent_tweets": [
    {
      "id": "1234567890",
      "text": "Tweet content...",
      "timestamp": "2026-02-17T10:30:00.000Z",
      "likes": 50000,
      "retweets": 12000,
      "replies": 3000,
      "views": "5.2M",
      "media_urls": ["https://..."],
      "media_local": ["thumbnails/elonmusk/tweet_media_0_def456.jpg"],
      "is_retweet": false,
      "is_reply": false,
      "url": "https://x.com/elonmusk/status/1234567890"
    }
  ]
}
```

### Queue File (`data/queue/{location}_{category}_{timestamp}.json`)

```json
{
  "location": "New York",
  "category": "tech",
  "total": 15,
  "usernames": ["user1", "user2", "..."],
  "completed": ["user1"],
  "failed": {"user3": "not_found"},
  "current_index": 2,
  "created_at": "2026-02-17T12:00:00",
  "source": "google_api"
}
```

## Influencer Tiers

| Tier | Followers |
|------|-----------|
| nano | < 1,000 |
| micro | 1,000 - 10,000 |
| mid | 10,000 - 100,000 |
| macro | 100,000 - 1,000,000 |
| mega | > 1,000,000 |

## Configuration (`config/scraper_config.json`)

| Setting | Default | Description |
|---------|---------|-------------|
| `scraper.headless` | `false` | Run browser without GUI |
| `scraper.min_followers` | `500` | Skip profiles below this count |
| `scraper.max_tweets` | `20` | Max recent tweets to scrape per profile |
| `scraper.max_thumbnails` | `6` | Max media images to download |
| `scraper.download_thumbnails` | `true` | Download profile pics and media |
| `scraper.delay_between_profiles` | `[4, 8]` | Random delay range (seconds) |
| `scraper.timeout` | `60000` | Page load timeout (ms) |

## Anti-Detection

The scraper uses multiple anti-detection techniques:

- **Browser fingerprinting** — 4 rotating fingerprint profiles (viewport, user agent, timezone, WebGL, etc.)
- **Stealth JavaScript** — Hides `navigator.webdriver`, spoofs plugins/languages/hardware, canvas noise, fake `chrome` object
- **Human behavior simulation** — Random delays, mouse movements, scrolling patterns
- **Network randomization** — Variable timing between requests
- **Login wall handling** — Automatically dismisses Twitter's login prompts and overlays

## Filters

Profiles are automatically skipped if:
- Account doesn't exist or is suspended
- Account is protected (private)
- Followers count < `min_followers` config value

## Notes

- **No login required** — Only scrapes publicly visible content
- **Checkpoint/resume** — Queue files track progress; interrupted scrapes can be resumed with `--resume`
- **Rate limiting** — Waits 60s on rate limit, stops on daily limit detection
- **Twitter selectors** — Uses `data-testid` attributes (stable across UI changes) with fallbacks to `aria-label` and structural selectors
