#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Residential Proxy Manager for Twitter/X Scraper

Supports multiple proxy providers with automatic IP rotation,
sticky sessions, and geo-targeting. Works seamlessly with Playwright
browser contexts and aiohttp/requests sessions.

Supported providers:
    - Bright Data (brd.superproxy.io)
    - IProyal (proxy.iproyal.com)
    - Storm Proxies (rotating.stormproxies.com)
    - NetNut (gw-resi.netnut.io)
    - Custom (any SOCKS5/HTTP proxy)
"""

import os
import json
import uuid
import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Default host/port for each supported provider
PROVIDER_DEFAULTS = {
    "brightdata": {"host": "brd.superproxy.io", "port": 22225},
    "iproyal": {"host": "proxy.iproyal.com", "port": 12321},
    "stormproxies": {"host": "rotating.stormproxies.com", "port": 9999},
    "netnut": {"host": "gw-resi.netnut.io", "port": 5959},
}

CONFIG_PATH = Path(__file__).parent / "config" / "scraper_config.json"


class ProxyManager:
    """
    Manages residential proxy connections for Playwright and HTTP clients.

    Usage:
        pm = ProxyManager.from_config()   # reads config/scraper_config.json + env
        pm = ProxyManager.from_env()      # reads only environment variables

        # For Playwright browser context
        proxy_opts = pm.get_playwright_proxy()
        context = await browser.new_context(proxy=proxy_opts, ...)

        # For requests / aiohttp
        proxies = pm.get_requests_proxy()

        # Rotate to a new IP
        pm.rotate_session()
    """

    def __init__(
        self,
        provider: str = "brightdata",
        host: str = "",
        port: int = 0,
        username: str = "",
        password: str = "",
        country: str = "",
        sticky: bool = True,
        sticky_ttl_minutes: int = 10,
        enabled: bool = True,
    ):
        self.provider = provider.lower()
        self.username = username
        self.password = password
        self.country = country
        self.sticky = sticky
        self.sticky_ttl_minutes = sticky_ttl_minutes
        self.enabled = enabled

        # Resolve host/port from provider defaults if not supplied
        defaults = PROVIDER_DEFAULTS.get(self.provider, {})
        self.host = host or defaults.get("host", "")
        self.port = port or defaults.get("port", 0)

        # Generate initial session id for sticky sessions
        self._session_id = uuid.uuid4().hex[:12]

    # ── Factory methods ──────────────────────────────────

    @classmethod
    def from_config(cls, config_path: Path = None) -> "ProxyManager":
        """Build ProxyManager from config file + environment variable overrides."""
        config_path = config_path or CONFIG_PATH
        proxy_cfg: Dict = {}
        try:
            with open(config_path, "r") as f:
                proxy_cfg = json.load(f).get("proxy", {})
        except Exception:
            pass

        # Environment variables take precedence
        enabled = os.getenv("PROXY_ENABLED", str(proxy_cfg.get("enabled", False))).lower() in (
            "true", "1", "yes",
        )
        provider = os.getenv("PROXY_PROVIDER", proxy_cfg.get("provider", "brightdata"))
        host = os.getenv("PROXY_HOST", proxy_cfg.get("host", ""))
        port = int(os.getenv("PROXY_PORT", proxy_cfg.get("port", 0)))
        username = os.getenv("PROXY_USERNAME", proxy_cfg.get("username", ""))
        password = os.getenv("PROXY_PASSWORD", proxy_cfg.get("password", ""))
        country = os.getenv("PROXY_COUNTRY", proxy_cfg.get("country", ""))
        sticky = os.getenv("PROXY_STICKY", str(proxy_cfg.get("sticky", True))).lower() in (
            "true", "1", "yes",
        )
        sticky_ttl = int(os.getenv("PROXY_STICKY_TTL", proxy_cfg.get("sticky_ttl_minutes", 10)))

        return cls(
            provider=provider,
            host=host,
            port=port,
            username=username,
            password=password,
            country=country,
            sticky=sticky,
            sticky_ttl_minutes=sticky_ttl,
            enabled=enabled,
        )

    @classmethod
    def from_env(cls) -> "ProxyManager":
        """Build ProxyManager exclusively from environment variables."""
        enabled = os.getenv("PROXY_ENABLED", "false").lower() in ("true", "1", "yes")
        return cls(
            provider=os.getenv("PROXY_PROVIDER", "brightdata"),
            host=os.getenv("PROXY_HOST", ""),
            port=int(os.getenv("PROXY_PORT", "0")),
            username=os.getenv("PROXY_USERNAME", ""),
            password=os.getenv("PROXY_PASSWORD", ""),
            country=os.getenv("PROXY_COUNTRY", ""),
            sticky=os.getenv("PROXY_STICKY", "true").lower() in ("true", "1", "yes"),
            sticky_ttl_minutes=int(os.getenv("PROXY_STICKY_TTL", "10")),
            enabled=enabled,
        )

    # ── Session management ───────────────────────────────

    def rotate_session(self):
        """Generate a new session ID to force IP rotation."""
        old = self._session_id
        self._session_id = uuid.uuid4().hex[:12]
        logger.info(f"Proxy session rotated: {old} → {self._session_id}")

    # ── Credential helpers ───────────────────────────────

    def _build_username(self) -> str:
        """
        Build the provider-specific username string.

        Many residential providers encode options in the username field, e.g.:
            brd-customer-CUST-zone-ZONE-country-us-session-abc123
        """
        user = self.username

        if self.provider == "brightdata":
            parts = [user]
            if self.country:
                parts.append(f"country-{self.country}")
            if self.sticky:
                parts.append(f"session-{self._session_id}")
            return "-".join(parts)

        if self.provider == "netnut":
            parts = [user]
            if self.country:
                parts.append(f"country-{self.country}")
            if self.sticky:
                parts.append(f"session-{self._session_id}")
            return "-".join(parts)

        if self.provider == "iproyal":
            parts = [user]
            if self.country:
                parts.append(f"country-{self.country}")
            if self.sticky:
                parts.append(f"session-{self._session_id}")
                parts.append(f"sessTime-{self.sticky_ttl_minutes}")
            return "_".join(parts)

        if self.provider == "stormproxies":
            # Storm Proxies typically don't encode in username, but we
            # pass country/session if the plan supports it
            parts = [user]
            if self.country:
                parts.append(f"country-{self.country}")
            if self.sticky:
                parts.append(f"session-{self._session_id}")
            return "-".join(parts)

        # Custom / fallback — return raw username
        return user

    # ── Proxy URLs ───────────────────────────────────────

    def _proxy_url(self, scheme: str = "http") -> str:
        user = self._build_username()
        pwd = self.password
        return f"{scheme}://{user}:{pwd}@{self.host}:{self.port}"

    def get_playwright_proxy(self) -> Optional[Dict]:
        """Return proxy dict for Playwright ``browser.new_context(proxy=...)``.

        Returns ``None`` if proxy is disabled or credentials are missing.
        """
        if not self.enabled or not self.host or not self.username:
            return None

        return {
            "server": f"http://{self.host}:{self.port}",
            "username": self._build_username(),
            "password": self.password,
        }

    def get_requests_proxy(self) -> Optional[Dict]:
        """Return proxy dict for ``requests`` / ``aiohttp`` sessions.

        Returns ``None`` if proxy is disabled.
        """
        if not self.enabled or not self.host or not self.username:
            return None

        url = self._proxy_url("http")
        return {"http": url, "https": url}

    # ── Info / repr ──────────────────────────────────────

    def info(self) -> Dict:
        return {
            "enabled": self.enabled,
            "provider": self.provider,
            "host": self.host,
            "port": self.port,
            "country": self.country,
            "sticky": self.sticky,
            "sticky_ttl_minutes": self.sticky_ttl_minutes,
            "session_id": self._session_id,
        }

    def __repr__(self) -> str:
        status = "enabled" if self.enabled else "disabled"
        return f"<ProxyManager provider={self.provider} {status} host={self.host}:{self.port}>"
