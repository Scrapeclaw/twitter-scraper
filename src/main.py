#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Apify Actor entry point for Twitter/X Profile Scraper & Discovery.

Wraps the existing discovery.py and scraper.py so they run on the Apify platform.
No login is required — all scraping is public-only.

Input  → Actor.get_input()
Output → Actor.push_data()  (default dataset)
Images → Actor.set_value()  (key-value store, optional)
State  → Actor.set_value() / Actor.get_value() (key-value store)
"""

import asyncio
import json
import os
import sys
import logging
import random
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from apify import Actor

# ---------------------------------------------------------------------------
# Ensure the twitter-scraper root is importable (discovery.py, scraper.py, etc.)
# ---------------------------------------------------------------------------
SCRIPTS_DIR = Path(__file__).parent.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Region / location presets (Twitter-oriented categories)
# ---------------------------------------------------------------------------
REGION_PRESETS: Dict[str, Dict] = {
    "us": {
        "categories": ["tech", "politics", "sports", "entertainment", "finance", "news", "gaming"],
        "locations": ["New York", "Los Angeles", "Miami", "Chicago", "San Francisco",
                      "Houston", "Phoenix", "Seattle"],
    },
    "uk": {
        "categories": ["politics", "sports", "entertainment", "finance", "news", "tech"],
        "locations": ["London", "Manchester", "Birmingham", "Glasgow", "Leeds", "Liverpool"],
    },
    "ind": {
        "categories": ["tech", "politics", "cricket", "entertainment", "finance", "news",
                       "startup", "crypto"],
        "locations": ["India", "Mumbai", "Delhi", "Bangalore", "Hyderabad",
                      "Chennai", "Kolkata", "Pune"],
    },
    "eur": {
        "categories": ["politics", "sports", "tech", "finance", "entertainment", "news"],
        "locations": ["Germany", "France", "Spain", "Italy", "Netherlands",
                      "Paris", "Berlin", "Amsterdam"],
    },
    "gulf": {
        "categories": ["finance", "crypto", "politics", "entertainment", "tech", "news"],
        "locations": ["UAE", "Dubai", "Abu Dhabi", "Saudi Arabia", "Riyadh",
                      "Kuwait", "Qatar", "Doha"],
    },
    "east": {
        "categories": ["tech", "gaming", "entertainment", "finance", "crypto", "news"],
        "locations": ["Japan", "South Korea", "Thailand", "Indonesia", "Singapore",
                      "Malaysia", "Philippines", "Tokyo"],
    },
}


# ---------------------------------------------------------------------------
# Helper: build a config dict compatible with discovery.py / scraper.py
# ---------------------------------------------------------------------------
def build_config(actor_input: Dict) -> Dict:
    region = actor_input.get("region", "us").lower()
    preset = REGION_PRESETS.get(region, REGION_PRESETS["us"])

    categories = actor_input.get("categories") or preset["categories"]
    locations  = actor_input.get("locations")  or preset["locations"]

    google_api_key = actor_input.get("googleApiKey", "")
    google_cx      = actor_input.get("googleSearchEngineId", "")

    return {
        "proxy": {"enabled": False},
        "google_search": {
            "enabled": bool(google_api_key and google_cx),
            "api_key": google_api_key,
            "search_engine_id": google_cx,
            "queries_per_location": 3,
        },
        "scraper": {
            "headless": True,
            "min_followers": actor_input.get("minFollowers", 500),
            "download_thumbnails": actor_input.get("downloadThumbnails", False),
            "max_thumbnails": actor_input.get("maxThumbnailsPerProfile", 6),
            "max_tweets": actor_input.get("maxTweetsPerProfile", 20),
            "delay_between_profiles": [4, 8],
            "timeout": 90000,
        },
        "cities": locations,
        "categories": categories,
    }


# ---------------------------------------------------------------------------
# Discovery: returns sorted unique list of profile dicts
# ---------------------------------------------------------------------------
def run_discovery(config: Dict, categories: List[str], locations: List[str]) -> List[Dict]:
    """Use discovery.py (sync, requests-based) to find Twitter/X usernames."""
    from discovery import discover_profiles_google, discover_profiles_duckduckgo

    found: List[Dict] = []
    seen: set = set()

    for location in locations:
        for category in categories:
            logger.info(f"Discovering: {category} in {location}")
            try:
                if config["google_search"]["enabled"]:
                    usernames = discover_profiles_google(location, category, 10, config)
                else:
                    usernames = discover_profiles_duckduckgo(location, category, 10)

                for u in usernames:
                    if u and u not in seen:
                        seen.add(u)
                        found.append({"username": u, "category": category, "location": location})
            except Exception as exc:
                logger.warning(f"Discovery error ({location}/{category}): {exc}")

            import time
            time.sleep(random.uniform(1, 2))

    logger.info(f"Discovery complete. Found {len(found)} unique profiles.")
    return found


# ---------------------------------------------------------------------------
# Apify-aware scraper wrapper
# ---------------------------------------------------------------------------
class ApifyTwitterScraper:
    """
    Wraps TwitterScraper to:
    - inject Apify proxy into Playwright context (no login required)
    - push each scraped record to Apify dataset
    - optionally store thumbnails in KV store
    - respect maxProfiles limit
    """

    def __init__(
        self,
        proxy_url: Optional[str],
        min_followers: int,
        max_profiles: int,
        download_thumbnails: bool,
        max_thumbnails: int,
        config_path: str,
    ):
        self.proxy_url           = proxy_url
        self.min_followers       = min_followers
        self.max_profiles        = max_profiles
        self.download_thumbnails = download_thumbnails
        self.max_thumbnails      = max_thumbnails
        self.config_path         = config_path
        self._scraped            = 0

    async def scrape_profiles(self, profiles: List[Any]) -> Dict:
        """Scrape a list of profile dicts/strings and push results to dataset."""
        from scraper import (
            TwitterScraper,
            ProfileNotFoundException,
            ProfileSkippedException,
            RateLimitException,
            DailyLimitException,
        )

        stats = {"success": 0, "failed": 0, "skipped": 0}

        inst = TwitterScraper(config_path=Path(self.config_path))

        # Inject Apify proxy into proxy_manager so start_browser picks it up.
        # Playwright requires separate username/password keys — not embedded in server.
        if self.proxy_url:
            inst.proxy_manager.enabled = True
            inst.proxy_manager.get_playwright_proxy = lambda: _parse_proxy_url(self.proxy_url)
            parsed = _parse_proxy_url(self.proxy_url)
            logger.info(f"Proxy injected: {parsed.get('server')} (credentials hidden)")

        await inst.start_browser(headless=True)

        try:
            for entry in profiles:
                if self.max_profiles and self._scraped >= self.max_profiles:
                    logger.info(f"Reached maxProfiles limit ({self.max_profiles}). Stopping.")
                    break

                if isinstance(entry, str):
                    username = entry.lstrip("@")
                    category = ""
                    location = ""
                else:
                    username = entry.get("username", "").lstrip("@")
                    category = entry.get("category", "")
                    location = entry.get("location", "")

                if not username:
                    continue

                try:
                    profile = await inst.scrape_profile(username, category, location)

                    if not profile:
                        stats["skipped"] += 1
                        continue

                    # Re-check follower threshold in case config differs
                    followers = profile.get("followers", 0)
                    if self.min_followers and followers < self.min_followers:
                        logger.info(f"Skipping @{username}: {followers:,} followers < {self.min_followers:,}")
                        stats["skipped"] += 1
                        continue

                    # Handle thumbnails
                    if not self.download_thumbnails:
                        profile.pop("profile_pic_local", None)
                    else:
                        await self._store_thumbnails(profile, username)

                    await Actor.push_data(profile)
                    logger.info(
                        f"[{self._scraped + 1}] Pushed: @{username} "
                        f"({followers:,} followers, tier={profile.get('influencer_tier')})"
                    )
                    self._scraped += 1
                    stats["success"] += 1

                except ProfileNotFoundException:
                    logger.warning(f"Profile not found: @{username}")
                    stats["failed"] += 1
                except ProfileSkippedException:
                    logger.info(f"Profile skipped: @{username}")
                    stats["skipped"] += 1
                except RateLimitException:
                    logger.warning("Rate limited by Twitter — sleeping 90 s…")
                    await asyncio.sleep(90)
                    stats["failed"] += 1
                except DailyLimitException:
                    logger.error("Daily scraping limit reached. Stopping.")
                    break
                except Exception as exc:
                    logger.error(f"Error scraping @{username}: {exc}")
                    stats["failed"] += 1

                await asyncio.sleep(random.uniform(4, 8))

        finally:
            await inst.cleanup()

        return stats

    async def _store_thumbnails(self, profile: Dict, username: str):
        """Upload downloaded thumbnail files to the Apify Key-Value store."""
        async def _upload(local_path: str, key: str):
            p = Path(local_path)
            if p.exists():
                with open(p, "rb") as fh:
                    data = fh.read()
                await Actor.set_value(key, data, content_type="image/jpeg")
                logger.debug(f"Stored: {key}")

        if profile.get("profile_pic_local"):
            await _upload(profile["profile_pic_local"], f"tw_{username}_profile")

        # Upload tweet media thumbnails if present
        for i, tweet in enumerate(profile.get("recent_tweets", []), 1):
            for j, local_path in enumerate(tweet.get("media_local", []), 1):
                await _upload(local_path, f"tw_{username}_tweet{i}_media{j}")


# ---------------------------------------------------------------------------
# Proxy helpers (same pattern as instagram-scraper)
# ---------------------------------------------------------------------------
def _parse_proxy_url(proxy_url: str) -> Dict[str, str]:
    """Break an Apify proxy URL into Playwright-friendly components.

    ``proxy_configuration.new_url()`` returns a URL with embedded credentials,
    e.g. ``http://user:pass@proxy.apify.com:8000``.
    Playwright does **not** parse credentials from the ``server`` value — it
    requires separate ``username`` / ``password`` keys.
    """
    from urllib.parse import urlparse

    parsed = urlparse(proxy_url)
    server = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
    result: Dict[str, str] = {"server": server}
    if parsed.username:
        result["username"] = parsed.username
    if parsed.password:
        result["password"] = parsed.password
    return result


async def validate_proxy(proxy_url: str) -> bool:
    """Concurrent proxy connectivity check across multiple endpoints.

    Runs all endpoint checks in parallel with ``asyncio.FIRST_COMPLETED`` so
    the function returns as soon as any single probe succeeds.  Total wall-clock
    time is capped at ``overall_timeout`` ms to prevent Apify actor timeouts.

    Set env var ``SKIP_PROXY_VALIDATION=1`` to bypass this check entirely.
    """
    if os.getenv("SKIP_PROXY_VALIDATION"):
        logger.info("Skipping proxy validation (SKIP_PROXY_VALIDATION env var set)")
        return True

    from playwright.async_api import async_playwright

    endpoints = [
        "https://x.com",
        "https://twitter.com",
        "https://www.google.com",
    ]

    is_residential = "RESIDENTIAL" in proxy_url.upper()
    per_endpoint_timeout = 30000 if is_residential else 20000  # ms
    overall_timeout      = 60000  # ms — total budget

    async def _check(ctx, endpoint: str) -> bool:
        try:
            logger.info(f"Validating proxy via {endpoint}…")
            page = await ctx.new_page()
            await page.goto(endpoint, timeout=per_endpoint_timeout, wait_until="domcontentloaded")
            await page.close()
            logger.info(f"✓ Proxy validation successful via {endpoint}")
            return True
        except Exception as exc:
            logger.debug(f"  Endpoint {endpoint} failed: {exc}")
            return False

    pw = None
    try:
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(headless=True)
        proxy_opts = _parse_proxy_url(proxy_url)
        ctx = await browser.new_context(proxy=proxy_opts)

        tasks = [asyncio.create_task(_check(ctx, ep)) for ep in endpoints]
        done, pending = await asyncio.wait(
            tasks, timeout=overall_timeout / 1000, return_when=asyncio.FIRST_COMPLETED
        )

        success = any(t.result() for t in done)

        for task in pending:
            task.cancel()

        await browser.close()
        await pw.stop()

        if not success:
            logger.error("Proxy validation failed: no endpoint responded within budget")
        return success

    except Exception as exc:
        logger.error(f"Proxy validation error: {exc}")
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass
        return False


# ---------------------------------------------------------------------------
# Main actor logic
# ---------------------------------------------------------------------------
async def main():
    async with Actor:
        # ----------------------------------------------------------------
        # 1. Read input
        # ----------------------------------------------------------------
        actor_input: Dict = await Actor.get_input() or {}
        logger.info(f"Actor input: {json.dumps(actor_input, indent=2, default=str)}")

        mode              = actor_input.get("mode", "full")
        region            = actor_input.get("region", "us").lower()
        min_followers     = actor_input.get("minFollowers", 500)
        max_profiles      = actor_input.get("maxProfiles", 50)
        download_thumbs   = actor_input.get("downloadThumbnails", False)
        max_thumbs        = actor_input.get("maxThumbnailsPerProfile", 6)
        profile_usernames = actor_input.get("profileUsernames", [])

        # ----------------------------------------------------------------
        # 2. Proxy configuration
        # ----------------------------------------------------------------
        proxy_url: Optional[str] = None
        proxy_cfg_input = actor_input.get("proxyConfiguration")
        if proxy_cfg_input:
            try:
                proxy_configuration = await Actor.create_proxy_configuration(
                    actor_proxy_input=proxy_cfg_input
                )
                if proxy_configuration:
                    proxy_url = await proxy_configuration.new_url()
                safe = _parse_proxy_url(proxy_url).get("server")
                logger.info(f"Using Apify proxy: {safe}")
            except Exception as exc:
                logger.warning(f"Could not create proxy configuration: {exc}")

        # Validate proxy connectivity (skip if user opts out)
        if proxy_url:
            if actor_input.get("skipProxyValidation", False):
                logger.info("Skipping proxy validation (skipProxyValidation=true)")
            else:
                logger.info("Validating proxy connectivity…")
                ok = await validate_proxy(proxy_url)
                if not ok:
                    if actor_input.get("dropProxyOnFailure", False):
                        logger.warning("Proxy validation failed — removing proxy and running direct.")
                        proxy_url = None
                    else:
                        logger.warning(
                            "Proxy validation failed — continuing with proxy anyway. "
                            "Set dropProxyOnFailure=true to disable it on failure."
                        )

        # ----------------------------------------------------------------
        # 3. Build config and write temp config file
        # ----------------------------------------------------------------
        config = build_config(actor_input)
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        )
        json.dump(config, tmp)
        tmp.close()
        config_path = tmp.name
        logger.info(f"Wrote temp config to {config_path}")

        preset     = REGION_PRESETS.get(region, REGION_PRESETS["us"])
        categories = actor_input.get("categories") or preset["categories"]
        locations  = actor_input.get("locations")  or preset["locations"]

        # ----------------------------------------------------------------
        # 4. Execute based on mode
        # ----------------------------------------------------------------
        profiles_to_scrape: List[Any] = []

        if mode == "scrape_profiles":
            if not profile_usernames:
                await Actor.fail(
                    status_message="mode=scrape_profiles requires profileUsernames to be set."
                )
                return
            profiles_to_scrape = profile_usernames
            logger.info(f"scrape_profiles mode: {len(profiles_to_scrape)} usernames provided")

        elif mode == "discovery_only":
            logger.info("discovery_only mode: discovering profiles and pushing usernames to dataset…")
            discovered = run_discovery(config, categories, locations)
            logger.info(f"Discovered {len(discovered)} profiles")
            for p in discovered:
                await Actor.push_data(p)
            logger.info("Discovery complete. Exiting.")
            return

        else:  # full
            logger.info("full mode: discovering profiles then scraping…")

            state_key = f"tw_state_{region}"
            state = await Actor.get_value(state_key) or {}

            if state.get("profiles") and state.get("phase") not in ("completed", None):
                logger.info(f"Resuming from saved state ({len(state['profiles'])} profiles)")
                profiles_to_scrape = state["profiles"]
            else:
                discovered = run_discovery(config, categories, locations)
                logger.info(f"Discovery found {len(discovered)} profiles")
                profiles_to_scrape = discovered
                await Actor.set_value(
                    state_key,
                    {
                        "profiles": profiles_to_scrape,
                        "phase": "scraping",
                        "discovered_at": datetime.now(timezone.utc).isoformat(),
                    },
                )

        # ----------------------------------------------------------------
        # 5. Scrape profiles
        # ----------------------------------------------------------------
        if not profiles_to_scrape:
            logger.warning("No profiles to scrape. Finishing.")
            return

        logger.info(
            f"Starting to scrape {len(profiles_to_scrape)} profiles "
            f"(max={max_profiles or 'unlimited'})…"
        )

        scraper_wrapper = ApifyTwitterScraper(
            proxy_url=proxy_url,
            min_followers=min_followers,
            max_profiles=max_profiles,
            download_thumbnails=download_thumbs,
            max_thumbnails=max_thumbs,
            config_path=config_path,
        )

        stats = await scraper_wrapper.scrape_profiles(profiles_to_scrape)

        logger.info(
            f"Scraping complete — success={stats['success']}, "
            f"failed={stats['failed']}, skipped={stats['skipped']}"
        )

        # Mark state as completed
        if mode == "full":
            await Actor.set_value(
                f"tw_state_{region}",
                {
                    "phase": "completed",
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "stats": stats,
                },
            )

        # ----------------------------------------------------------------
        # 6. Clean up temp config
        # ----------------------------------------------------------------
        try:
            Path(config_path).unlink(missing_ok=True)
        except Exception:
            pass


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
