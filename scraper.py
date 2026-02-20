#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Twitter/X Scraper with Playwright Browser Automation
Scrapes public Twitter profiles, recent tweets, and media without login
Uses anti-detection techniques to avoid bot detection
"""

import asyncio
import json
import os
import sys
import logging
import time
import csv
import re
from typing import List, Dict, Optional
from pathlib import Path
from playwright.async_api import async_playwright, Page, Browser
from datetime import datetime
import random
from dotenv import load_dotenv
import aiohttp
import hashlib
from PIL import Image
import io

# Set UTF-8 encoding for stdout
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Base directory for the skill
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'
OUTPUT_DIR = DATA_DIR / 'output'
QUEUE_DIR = DATA_DIR / 'queue'
THUMBNAILS_DIR = BASE_DIR / 'thumbnails'
CONFIG_PATH = BASE_DIR / 'config' / 'scraper_config.json'


class ProfileSkippedException(Exception):
    """Exception raised when a profile should be skipped (e.g. protected/private)"""
    pass


class ProfileNotFoundException(Exception):
    """Exception raised when a profile doesn't exist or is suspended"""
    pass


class RateLimitException(Exception):
    """Exception raised when Twitter rate limits the request"""
    pass


class DailyLimitException(Exception):
    """Exception raised when daily scraping limit is reached"""
    pass


class TwitterScraper:
    """Twitter/X scraper using Playwright for browser automation (public-only, no login)"""

    def __init__(self, config_path: Path = None):
        self.config = self._load_config(config_path or CONFIG_PATH)
        self.browser = None
        self.context = None
        self.page = None
        self.playwright = None

        # Setup directories
        self.thumbnails_dir = THUMBNAILS_DIR
        self.output_dir = OUTPUT_DIR
        self.thumbnails_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize anti-detection
        from anti_detection import AntiDetectionManager
        self.anti_detection = AntiDetectionManager(DATA_DIR)

    def _load_config(self, config_path: Path) -> Dict:
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load config: {e}. Using defaults.")
            return {
                'scraper': {
                    'headless': False,
                    'min_followers': 500,
                    'download_thumbnails': True,
                    'max_thumbnails': 6,
                    'max_tweets': 20,
                    'delay_between_profiles': [4, 8],
                    'timeout': 60000
                }
            }

    async def start_browser(self, headless: bool = None):
        """Start Playwright browser with anti-detection"""
        if headless is None:
            headless = self.config.get('scraper', {}).get('headless', False)
        
        logger.info("Starting browser with anti-detection...")
        
        self.playwright = await async_playwright().start()

        self.browser = await self.playwright.chromium.launch(
            headless=headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
            ]
        )

        # Apply fingerprint using the anti-detection manager
        fingerprint = self.anti_detection.get_fingerprint_for_account('twitter_public')
        context_options = self.anti_detection.get_context_options(fingerprint)
        self.context = await self.browser.new_context(**context_options)

        self.page = await self.context.new_page()

        # Inject stealth scripts
        stealth_js = self.anti_detection.get_stealth_scripts(fingerprint)
        await self.page.add_init_script(stealth_js)

        logger.info("Browser started with anti-detection")

    async def handle_login_wall(self, page: Page) -> bool:
        """
        Handle Twitter's login wall that appears for non-authenticated users.
        Twitter aggressively pushes login prompts on public profile views.
        
        Returns True if login wall was handled, False if page seems clean.
        """
        handled = False
        
        try:
            # Strategy 1: Close modal dialogs / bottom sheets
            # Twitter shows a "Sign in" or "Log in" bottom sheet
            close_selectors = [
                '[data-testid="sheetDialog"] [aria-label="Close"]',
                '[role="dialog"] [aria-label="Close"]',
                '[data-testid="app-bar-close"]',
                'div[role="button"][aria-label="Close"]',
            ]
            
            for selector in close_selectors:
                try:
                    close_btn = await page.wait_for_selector(selector, timeout=3000)
                    if close_btn:
                        await close_btn.click()
                        await asyncio.sleep(1)
                        handled = True
                        logger.info("Dismissed login dialog")
                        break
                except:
                    continue
            
            # Strategy 2: Dismiss "Sign up" / "Log in" banner at the bottom
            try:
                # Twitter shows a bottom banner prompting login
                banner_dismiss = await page.query_selector('[data-testid="BottomBar"] [role="button"]')
                if banner_dismiss:
                    # Don't click the sign up/log in buttons - just scroll past
                    await page.evaluate('window.scrollTo(0, 0)')
                    handled = True
            except:
                pass
            
            # Strategy 3: If we got redirected to login page, navigate back
            current_url = page.url
            if '/login' in current_url or '/i/flow/login' in current_url:
                logger.warning("Redirected to login page - navigating back")
                await page.go_back()
                await asyncio.sleep(2)
                handled = True
            
            # Strategy 4: Remove overlay elements that block content
            try:
                await page.evaluate('''() => {
                    // Remove common overlay/modal elements
                    const overlays = document.querySelectorAll(
                        '[data-testid="sheetDialog"], ' +
                        '[role="dialog"], ' +
                        '[data-testid="mask"]'
                    );
                    overlays.forEach(el => el.remove());
                    
                    // Remove any backdrop/overlay divs
                    const backdrops = document.querySelectorAll('div[style*="position: fixed"]');
                    backdrops.forEach(el => {
                        if (el.style.zIndex > 0) el.remove();
                    });
                    
                    // Re-enable scrolling on body
                    document.body.style.overflow = 'auto';
                    document.documentElement.style.overflow = 'auto';
                }''')
            except:
                pass
                
        except Exception as e:
            logger.debug(f"Login wall handling: {e}")
        
        return handled

    async def download_image(self, url: str, username: str, image_type: str, index: int = 0) -> Optional[str]:
        """Download and resize image to ~150KB"""
        try:
            user_dir = self.thumbnails_dir / username
            user_dir.mkdir(parents=True, exist_ok=True)

            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            filename = f"{image_type}_{index}_{url_hash}.jpg" if image_type != 'profile' else f"profile_{url_hash}.jpg"
            filepath = user_dir / filename

            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.read()

                        img = Image.open(io.BytesIO(content))

                        # Convert to RGB
                        if img.mode in ('RGBA', 'LA', 'P'):
                            rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                            if img.mode == 'P':
                                img = img.convert('RGBA')
                            rgb_img.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
                            img = rgb_img

                        # Resize to max 1000px
                        max_dimension = 1000
                        if max(img.size) > max_dimension:
                            ratio = max_dimension / max(img.size)
                            new_size = (int(img.size[0] * ratio), int(img.size[1] * ratio))
                            img = img.resize(new_size, Image.Resampling.LANCZOS)

                        # Save with compression
                        quality = 85
                        output = io.BytesIO()
                        img.save(output, format='JPEG', quality=quality, optimize=True)

                        # Adjust quality to meet ~150KB target
                        while output.tell() > 150 * 1024 and quality > 50:
                            output = io.BytesIO()
                            quality -= 5
                            img.save(output, format='JPEG', quality=quality, optimize=True)

                        with open(filepath, 'wb') as f:
                            f.write(output.getvalue())

                        logger.info(f"Downloaded: {filename} ({output.tell()/1024:.1f}KB)")
                        return str(filepath)

            return None
        except Exception as e:
            logger.error(f"Error downloading image: {e}")
            return None

    async def scrape_profile(self, username: str, category: str = '', location: str = '') -> Optional[Dict]:
        """
        Scrape a single Twitter/X public profile.
        
        Extracts: display name, bio, followers/following/tweets count,
        verified status, profile pic, location, join date, website,
        and recent tweets with engagement metrics + media.
        """
        try:
            timeout = self.config.get('scraper', {}).get('timeout', 60000)
            url = f'https://x.com/{username}'
            logger.info(f"Scraping profile: @{username}")

            # Pre-navigation anti-detection
            await self.anti_detection.apply_pre_navigation_behavior(self.page)
            
            response = await self.page.goto(url, wait_until='domcontentloaded', timeout=timeout)
            
            # Post-navigation anti-detection
            await self.anti_detection.apply_post_navigation_behavior(self.page)

            # Wait for primary content
            try:
                await self.page.wait_for_selector('[data-testid="UserName"], [data-testid="error-detail"]', timeout=15000)
            except:
                pass

            # Handle Twitter's login wall
            await self.handle_login_wall(self.page)

            # Wait for content to render
            await self.anti_detection.behavior_sim.simulate_content_render(self.page)

            # Check HTTP status
            if response and response.status >= 400:
                if response.status == 404:
                    raise ProfileNotFoundException(f"Profile @{username} not found (HTTP 404)")
                elif response.status == 429:
                    raise RateLimitException("Rate limited (HTTP 429)")

            # Check page content for error states
            page_content = await self.page.content()
            page_content_lower = page_content.lower()

            # Check for non-existent account
            not_found_indicators = [
                "this account doesn't exist",
                "account suspended",
                "account is suspended",
                "hmm...this page doesn't exist",
                "this account doesn't exist",
                "user not found",
            ]
            for indicator in not_found_indicators:
                if indicator in page_content_lower:
                    raise ProfileNotFoundException(f"Profile @{username}: {indicator}")

            # Check for protected/private account
            if 'these tweets are protected' in page_content_lower:
                raise ProfileSkippedException(f"Profile @{username} is protected")

            # Check for rate limiting
            if 'rate limit' in page_content_lower or 'try again later' in page_content_lower:
                raise RateLimitException("Rate limited by Twitter")

            # Scroll to load content
            await self.anti_detection.behavior_sim.simulate_scroll(self.page)

            # Final wait before extraction
            await self.anti_detection.behavior_sim.simulate_final_wait(self.page)

            # Extract profile data via JavaScript
            profile_data = await self.page.evaluate(r'''() => {
                const data = {};
                
                // Username from URL
                data.username = window.location.pathname.split('/').filter(x => x)[0] || '';
                
                // Display name - from UserName testid
                const userNameEl = document.querySelector('[data-testid="UserName"]');
                if (userNameEl) {
                    // First line/span is the display name, second is @handle
                    const spans = userNameEl.querySelectorAll('span');
                    if (spans.length > 0) {
                        data.display_name = spans[0].textContent.trim();
                    }
                }
                if (!data.display_name) {
                    // Fallback: try page title
                    const titleMatch = document.title.match(/^(.+?)\s*[\(\(@]/);
                    data.display_name = titleMatch ? titleMatch[1].trim() : data.username;
                }
                
                // Bio / description
                const bioEl = document.querySelector('[data-testid="UserDescription"]');
                data.bio = bioEl ? bioEl.textContent.trim() : '';
                
                // Parse count helper (handles K, M suffixes and commas)
                function parseCount(text) {
                    if (!text) return 0;
                    text = text.toUpperCase().replace(/,/g, '');
                    if (text.includes('K')) return Math.floor(parseFloat(text) * 1000);
                    if (text.includes('M')) return Math.floor(parseFloat(text) * 1000000);
                    if (text.includes('B')) return Math.floor(parseFloat(text) * 1000000000);
                    return parseInt(text) || 0;
                }
                
                // Followers / Following counts
                // These are in <a> tags linking to /username/followers and /username/following or /username/verified_followers
                const followersLink = document.querySelector('a[href*="/followers"], a[href*="/verified_followers"]');
                const followingLink = document.querySelector('a[href*="/following"]');
                
                if (followersLink) {
                    const followerText = followersLink.innerText.trim();
                    const followerMatch = followerText.match(/([\d,.KkMmBb]+)/);
                    data.followers = followerMatch ? parseCount(followerMatch[1]) : 0;
                } else {
                    // Fallback: try to find any text that looks like "1.2K Followers"
                    const allLinks = Array.from(document.querySelectorAll('a'));
                    const fLink = allLinks.find(a => a.innerText.includes('Followers'));
                    if (fLink) {
                        const m = fLink.innerText.match(/([\d,.KkMmBb]+)/);
                        data.followers = m ? parseCount(m[1]) : 0;
                    } else {
                        data.followers = 0;
                    }
                }
                
                if (followingLink) {
                    const followingText = followingLink.innerText.trim();
                    const followingMatch = followingText.match(/([\d,.KkMmBb]+)/);
                    data.following = followingMatch ? parseCount(followingMatch[1]) : 0;
                } else {
                    data.following = 0;
                }
                
                // Tweets count - from the header/nav area
                // Try to find it in the profile header stats text
                const bodyText = document.body.innerText;
                const postsMatch = bodyText.match(/([\d,.KkMm]+)\s+(?:posts?|tweets?)/i);
                data.tweets_count = postsMatch ? parseCount(postsMatch[1]) : 0;
                
                // Verified badge
                data.is_verified = !!(
                    document.querySelector('[data-testid="icon-verified"]') ||
                    document.querySelector('svg[aria-label="Verified account"]') ||
                    document.querySelector('[aria-label="Verified"]')
                );
                
                // Profile picture
                const profilePicLink = document.querySelector('a[href$="/photo"] img');
                const profilePicAlt = document.querySelector('img[alt="Opens profile photo"]');
                const profilePicEl = profilePicLink || profilePicAlt;
                data.profile_pic_url = profilePicEl ? profilePicEl.src : '';
                
                // Location, join date, website from UserProfileHeader_Items
                const headerItems = document.querySelectorAll('[data-testid="UserProfileHeader_Items"] span');
                data.user_location = '';
                data.join_date = '';
                data.website = '';
                
                headerItems.forEach(span => {
                    const text = span.textContent.trim();
                    // Join date usually starts with "Joined"
                    if (text.startsWith('Joined ')) {
                        data.join_date = text.replace('Joined ', '');
                    }
                });
                
                // Location - first text item in header items (if not a link or date)
                const headerContainer = document.querySelector('[data-testid="UserProfileHeader_Items"]');
                if (headerContainer) {
                    const locationSpan = headerContainer.querySelector('span[data-testid="UserLocation"]');
                    if (locationSpan) {
                        data.user_location = locationSpan.textContent.trim();
                    } else {
                        // Fallback: check for location icon (has a path like location pin)  
                        const allSpans = headerContainer.querySelectorAll('span');
                        for (const sp of allSpans) {
                            const t = sp.textContent.trim();
                            if (t && !t.startsWith('Joined') && !t.startsWith('http') && !t.includes('.') && t.length > 1 && t.length < 50) {
                                // Could be location
                                const prevSibling = sp.previousElementSibling;
                                if (prevSibling && prevSibling.querySelector('svg')) {
                                    data.user_location = t;
                                    break;
                                }
                            }
                        }
                    }
                }
                
                // Website URL
                const urlEl = document.querySelector('[data-testid="UserUrl"] a');
                data.website = urlEl ? urlEl.href || urlEl.textContent.trim() : '';
                
                // Protected account check
                data.is_protected = bodyText.toLowerCase().includes('these tweets are protected');
                
                return data;
            }''')

            # Validate data
            if not profile_data.get('username'):
                profile_data['username'] = username

            if profile_data.get('is_protected'):
                raise ProfileSkippedException(f"Profile @{username} is protected")

            # Check minimum followers
            min_followers = self.config.get('scraper', {}).get('min_followers', 500)
            if profile_data.get('followers', 0) < min_followers:
                logger.warning(f"Skipping @{username}: {profile_data.get('followers', 0)} followers < {min_followers}")
                raise ProfileSkippedException(f"Below minimum followers ({min_followers})")

            # Classify tier
            followers = profile_data.get('followers', 0)
            if followers < 1000:
                tier = 'nano'
            elif followers < 10000:
                tier = 'micro'
            elif followers < 100000:
                tier = 'mid'
            elif followers < 1000000:
                tier = 'macro'
            else:
                tier = 'mega'

            profile_data['influencer_tier'] = tier

            # Scrape recent tweets
            max_tweets = self.config.get('scraper', {}).get('max_tweets', 20)
            recent_tweets = await self.scrape_recent_tweets(self.page, username, max_tweets)
            profile_data['recent_tweets'] = recent_tweets

            # Download profile picture
            if self.config.get('scraper', {}).get('download_thumbnails', True):
                if profile_data.get('profile_pic_url'):
                    profile_pic_local = await self.download_image(
                        profile_data['profile_pic_url'],
                        username,
                        'profile'
                    )
                    profile_data['profile_pic_local'] = profile_pic_local

                # Download tweet media
                max_thumbnails = self.config.get('scraper', {}).get('max_thumbnails', 6)
                media_download_count = 0
                for tweet in recent_tweets:
                    for media_url in tweet.get('media_urls', []):
                        if media_download_count >= max_thumbnails:
                            break
                        local_path = await self.download_image(
                            media_url, username, 'tweet_media', media_download_count
                        )
                        if local_path:
                            tweet.setdefault('media_local', []).append(local_path)
                            media_download_count += 1
                    if media_download_count >= max_thumbnails:
                        break

            # Add metadata
            profile_data['category'] = category
            profile_data['scrape_location'] = location
            profile_data['scraped_at'] = datetime.now().isoformat()

            logger.info(f"Scraped: @{username} ({followers:,} followers, {tier}, {len(recent_tweets)} tweets)")
            return profile_data

        except (ProfileNotFoundException, ProfileSkippedException, RateLimitException, DailyLimitException):
            raise
        except Exception as e:
            logger.error(f"Error scraping @{username}: {e}")
            return None

    async def scrape_recent_tweets(self, page: Page, username: str, max_tweets: int = 20) -> List[Dict]:
        """
        Scrape recent tweets from a Twitter profile page.
        Scrolls the timeline to load tweets and extracts text, engagement, media.
        
        Returns:
            List of tweet dicts with keys: id, text, timestamp, likes, retweets,
            replies, media_urls, is_retweet, is_reply, url
        """
        tweets = []
        seen_ids = set()
        no_new_count = 0
        max_scrolls = max(3, max_tweets // 5)  # Rough estimate: ~5 tweets per scroll

        for scroll_round in range(max_scrolls):
            if len(tweets) >= max_tweets:
                break

            # Extract tweets currently visible on the page
            new_tweets = await page.evaluate(r'''(seenIds) => {
                const results = [];
                const tweetEls = document.querySelectorAll('[data-testid="tweet"]');
                
                for (const tweetEl of tweetEls) {
                    const tweet = {};
                    
                    // Tweet URL / ID
                    const timeLink = tweetEl.querySelector('a[href*="/status/"] time');
                    const statusLink = tweetEl.querySelector('a[href*="/status/"]');
                    if (statusLink) {
                        const href = statusLink.getAttribute('href');
                        const idMatch = href.match(/\/status\/(\d+)/);
                        tweet.id = idMatch ? idMatch[1] : '';
                        tweet.url = 'https://x.com' + href;
                    } else {
                        tweet.id = '';
                        tweet.url = '';
                    }
                    
                    // Skip if already seen
                    if (!tweet.id || seenIds.includes(tweet.id)) continue;
                    
                    // Tweet text
                    const textEl = tweetEl.querySelector('[data-testid="tweetText"]');
                    tweet.text = textEl ? textEl.textContent.trim() : '';
                    
                    // Timestamp
                    const timeEl = tweetEl.querySelector('time[datetime]');
                    tweet.timestamp = timeEl ? timeEl.getAttribute('datetime') : '';
                    
                    // Engagement metrics from aria-labels
                    // Reply count
                    const replyBtn = tweetEl.querySelector('[data-testid="reply"]');
                    if (replyBtn) {
                        const replyLabel = replyBtn.getAttribute('aria-label') || '';
                        const replyMatch = replyLabel.match(/([\d,KkMm.]+)\s*repl/i);
                        tweet.replies = replyMatch ? replyMatch[1].replace(/,/g, '') : '0';
                    } else {
                        tweet.replies = '0';
                    }
                    
                    // Retweet count
                    const retweetBtn = tweetEl.querySelector('[data-testid="retweet"]');
                    if (retweetBtn) {
                        const rtLabel = retweetBtn.getAttribute('aria-label') || '';
                        const rtMatch = rtLabel.match(/([\d,KkMm.]+)\s*re(?:post|tweet)/i);
                        tweet.retweets = rtMatch ? rtMatch[1].replace(/,/g, '') : '0';
                    } else {
                        tweet.retweets = '0';
                    }
                    
                    // Like count
                    const likeBtn = tweetEl.querySelector('[data-testid="like"]');
                    if (likeBtn) {
                        const likeLabel = likeBtn.getAttribute('aria-label') || '';
                        const likeMatch = likeLabel.match(/([\d,KkMm.]+)\s*like/i);
                        tweet.likes = likeMatch ? likeMatch[1].replace(/,/g, '') : '0';
                    } else {
                        tweet.likes = '0';
                    }
                    
                    // Views count (if available)
                    const viewsEl = tweetEl.querySelector('a[href*="/analytics"] span');
                    tweet.views = viewsEl ? viewsEl.textContent.trim() : '';
                    
                    // Media URLs (images)
                    tweet.media_urls = [];
                    const mediaImgs = tweetEl.querySelectorAll('[data-testid="tweetPhoto"] img');
                    mediaImgs.forEach(img => {
                        const src = img.src;
                        if (src && src.startsWith('http') && !src.includes('emoji') && !src.includes('profile')) {
                            tweet.media_urls.push(src);
                        }
                    });
                    
                    // Check if retweet
                    const socialContext = tweetEl.querySelector('[data-testid="socialContext"]');
                    tweet.is_retweet = !!(socialContext && 
                        (socialContext.textContent.toLowerCase().includes('reposted') || 
                         socialContext.textContent.toLowerCase().includes('retweeted')));
                    
                    // Check if reply
                    const replyContext = tweetEl.querySelector('div[data-testid="tweet"] > div > div > div');
                    tweet.is_reply = !!(tweetEl.textContent.includes('Replying to'));
                    
                    results.push(tweet);
                }
                
                return results;
            }''', list(seen_ids))

            if new_tweets:
                for t in new_tweets:
                    if t['id'] and t['id'] not in seen_ids and len(tweets) < max_tweets:
                        seen_ids.add(t['id'])
                        # Parse engagement counts
                        t['replies'] = self._parse_count_str(t.get('replies', '0'))
                        t['retweets'] = self._parse_count_str(t.get('retweets', '0'))
                        t['likes'] = self._parse_count_str(t.get('likes', '0'))
                        tweets.append(t)
                no_new_count = 0
            else:
                no_new_count += 1
                if no_new_count >= 3:
                    logger.info(f"No new tweets after {no_new_count} scroll attempts, stopping")
                    break

            # Scroll down to load more tweets
            if len(tweets) < max_tweets:
                await self.anti_detection.apply_infinite_scroll(self.page, scroll_count=1)

        logger.info(f"Extracted {len(tweets)} tweets from @{username}")
        return tweets

    @staticmethod
    def _parse_count_str(text: str) -> int:
        """Parse a count string like '1.2K', '3M', '42' into an integer"""
        if not text:
            return 0
        text = text.strip().upper().replace(',', '')
        try:
            if 'K' in text:
                return int(float(text.replace('K', '')) * 1000)
            elif 'M' in text:
                return int(float(text.replace('M', '')) * 1000000)
            elif 'B' in text:
                return int(float(text.replace('B', '')) * 1000000000)
            return int(text)
        except (ValueError, TypeError):
            return 0

    def save_profile(self, profile: Dict):
        """Save profile to JSON file"""
        username = profile.get('username', 'unknown')
        filepath = self.output_dir / f"{username}.json"
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(profile, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved: {filepath}")

    async def cleanup(self):
        """Close browser"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("Browser closed")


# ─────────────────────────────────────────────────────────
# Standalone functions (queue management, CLI helpers)
# ─────────────────────────────────────────────────────────

def load_queue_file(filepath: str) -> Dict:
    """Load queue file with checkpoint data"""
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if 'completed' not in data:
        data['completed'] = []
    if 'current_index' not in data:
        data['current_index'] = 0
    if 'failed' not in data:
        data['failed'] = {}

    return data


def save_queue_file(filepath: str, data: Dict):
    """Save queue file with checkpoint"""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


async def scrape_from_queue(queue_file: str, resume: bool = True, headless: bool = False) -> List[Dict]:
    """Scrape profiles from a queue file with checkpoint/resume support"""
    queue_data = load_queue_file(queue_file)
    
    usernames = queue_data.get('usernames', [])
    completed = set(queue_data.get('completed', []))
    location = queue_data.get('location', '')
    category = queue_data.get('category', '')
    
    # Filter remaining usernames
    remaining = [u for u in usernames if u not in completed]
    
    print(f"\n{'='*50}")
    print(f"Queue: {Path(queue_file).name}")
    print(f"   Location: {location}")
    print(f"   Category: {category}")
    print(f"   Total: {len(usernames)} | Completed: {len(completed)} | Remaining: {len(remaining)}")
    print(f"{'='*50}\n")
    
    if not remaining:
        print("All profiles already scraped!")
        return []
    
    scraper = TwitterScraper()
    results = []
    
    # Read delay from config instead of hardcoding
    delay_range = scraper.config.get('scraper', {}).get('delay_between_profiles', [4, 8])
    
    try:
        await scraper.start_browser(headless=headless)
        
        for i, username in enumerate(remaining, 1):
            print(f"\n[{i}/{len(remaining)}] Scraping: @{username}")
            
            try:
                profile = await scraper.scrape_profile(username, category, location)
                
                if profile:
                    results.append(profile)
                    scraper.save_profile(profile)
                    queue_data['completed'].append(username)
                else:
                    queue_data['failed'][username] = 'no_data'
                
            except ProfileNotFoundException as e:
                queue_data['failed'][username] = 'not_found'
                logger.warning(f"Profile not found: @{username}")
            except ProfileSkippedException as e:
                queue_data['failed'][username] = 'skipped'
                logger.warning(f"Profile skipped: @{username} - {e}")
            except RateLimitException:
                logger.error("Rate limited! Waiting 60 seconds...")
                await asyncio.sleep(60)
            except DailyLimitException:
                logger.error("Daily limit reached! Stopping.")
                break
            except Exception as e:
                queue_data['failed'][username] = str(e)
                logger.error(f"Error: {e}")
            
            # Save checkpoint after every profile
            save_queue_file(queue_file, queue_data)
            
            # Delay between profiles (read from config)
            if i < len(remaining):
                delay = random.uniform(delay_range[0], delay_range[1])
                logger.info(f"Waiting {delay:.1f}s...")
                await asyncio.sleep(delay)
        
    finally:
        await scraper.cleanup()
    
    print(f"\nDone! Scraped {len(results)} profiles successfully.")
    return results


async def scrape_single(username: str, output_json: bool = False, headless: bool = False) -> Optional[Dict]:
    """Scrape a single Twitter profile"""
    scraper = TwitterScraper()
    
    try:
        await scraper.start_browser(headless=headless)
        
        profile = await scraper.scrape_profile(username)
        
        if profile:
            scraper.save_profile(profile)
            if output_json:
                return profile
            print(f"\nScraped: @{username}")
            print(f"   Display Name: {profile.get('display_name', 'N/A')}")
            print(f"   Followers: {profile.get('followers', 0):,}")
            print(f"   Following: {profile.get('following', 0):,}")
            print(f"   Tweets: {profile.get('tweets_count', 0):,}")
            print(f"   Verified: {profile.get('is_verified', False)}")
            print(f"   Tier: {profile.get('influencer_tier', 'unknown')}")
            print(f"   Recent Tweets: {len(profile.get('recent_tweets', []))}")
            return profile
        else:
            if output_json:
                return {"error": "Could not scrape profile"}
            print(f"\nCould not scrape: @{username}")
            return None
        
    except ProfileNotFoundException as e:
        msg = f"Profile not found: @{username}"
        if output_json:
            return {"error": msg}
        print(f"\n{msg}")
        return None
    except ProfileSkippedException as e:
        msg = f"Profile skipped: {e}"
        if output_json:
            return {"error": msg}
        print(f"\n{msg}")
        return None
    finally:
        await scraper.cleanup()


async def scrape_manual_list(usernames: List[str], category: str = 'general', headless: bool = False) -> List[Dict]:
    """Scrape a list of manually-provided usernames"""
    from discovery import create_manual_queue
    
    queue_file = create_manual_queue(usernames, category)
    if not queue_file:
        logger.error("Failed to create manual queue")
        return []
    
    return await scrape_from_queue(queue_file, resume=True, headless=headless)


def export_data(output_format: str = 'both'):
    """Export all scraped data to JSON and/or CSV"""
    output_files = list(OUTPUT_DIR.glob('*.json'))
    
    if not output_files:
        print("No data to export")
        return
    
    profiles = []
    for f in output_files:
        with open(f, 'r', encoding='utf-8') as file:
            profiles.append(json.load(file))
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if output_format in ('json', 'both'):
        json_path = DATA_DIR / f"export_{timestamp}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(profiles, f, indent=2, ensure_ascii=False)
        print(f"JSON export: {json_path}")
    
    if output_format in ('csv', 'both'):
        csv_path = DATA_DIR / f"export_{timestamp}.csv"
        if profiles:
            # Profile-level CSV
            profile_keys = [
                'username', 'display_name', 'followers', 'following', 'tweets_count',
                'is_verified', 'bio', 'influencer_tier', 'category', 'scrape_location',
                'user_location', 'join_date', 'website'
            ]
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=profile_keys, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(profiles)
            print(f"CSV export (profiles): {csv_path}")
            
            # Tweets CSV (separate file)
            tweets_csv_path = DATA_DIR / f"export_tweets_{timestamp}.csv"
            tweet_keys = [
                'username', 'tweet_id', 'text', 'timestamp', 'likes', 'retweets',
                'replies', 'views', 'is_retweet', 'is_reply', 'url'
            ]
            tweet_rows = []
            for profile in profiles:
                uname = profile.get('username', '')
                for tweet in profile.get('recent_tweets', []):
                    row = {
                        'username': uname,
                        'tweet_id': tweet.get('id', ''),
                        'text': tweet.get('text', ''),
                        'timestamp': tweet.get('timestamp', ''),
                        'likes': tweet.get('likes', 0),
                        'retweets': tweet.get('retweets', 0),
                        'replies': tweet.get('replies', 0),
                        'views': tweet.get('views', ''),
                        'is_retweet': tweet.get('is_retweet', False),
                        'is_reply': tweet.get('is_reply', False),
                        'url': tweet.get('url', ''),
                    }
                    tweet_rows.append(row)
            
            if tweet_rows:
                with open(tweets_csv_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=tweet_keys, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(tweet_rows)
                print(f"CSV export (tweets): {tweets_csv_path}")


def list_queue_files():
    """List all queue files with progress"""
    queue_files = sorted(QUEUE_DIR.glob('*.json'))
    
    if not queue_files:
        print("No queue files found")
        return
    
    print(f"\n{'='*60}")
    print("Available Queue Files")
    print(f"{'='*60}")
    
    for i, qf in enumerate(queue_files, 1):
        try:
            with open(qf, 'r') as f:
                data = json.load(f)
            total = len(data.get('usernames', []))
            completed = len(data.get('completed', []))
            pct = int(completed/total*100) if total > 0 else 0
            print(f"{i}. {qf.name}")
            print(f"   Location: {data.get('location', 'N/A')} | Category: {data.get('category', 'N/A')}")
            print(f"   Source: {data.get('source', 'N/A')}")
            print(f"   Progress: {completed}/{total} ({pct}%)")
        except:
            print(f"{i}. {qf.name} (error reading)")


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Twitter/X Profile Scraper')
    parser.add_argument('queue_file', nargs='?', help='Queue file to scrape')
    parser.add_argument('--username', '-u', type=str, help='Single username to scrape')
    parser.add_argument('--usernames', type=str, help='Comma-separated usernames to scrape')
    parser.add_argument('--list', '-l', action='store_true', help='List queue files')
    parser.add_argument('--resume', '-r', action='store_true', default=True, help='Resume from checkpoint')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--export', '-e', type=str, choices=['json', 'csv', 'both'], help='Export data')
    parser.add_argument('--output', '-o', type=str, choices=['json', 'text'], default='text', help='Output format')
    
    args = parser.parse_args()
    
    if args.list:
        list_queue_files()
    elif args.export:
        export_data(args.export)
    elif args.username:
        result = asyncio.run(scrape_single(args.username, args.output == 'json', args.headless))
        if args.output == 'json' and result:
            print(json.dumps(result, indent=2))
    elif args.usernames:
        names = [u.strip() for u in args.usernames.split(',') if u.strip()]
        asyncio.run(scrape_manual_list(names, headless=args.headless))
    elif args.queue_file:
        asyncio.run(scrape_from_queue(args.queue_file, args.resume, args.headless))
    else:
        parser.print_help()
