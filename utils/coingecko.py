import os
import time
import requests
from typing import Dict, Any, Optional

CG = "https://api.coingecko.com/api/v3"

# Simple in-memory cache per run (prevents repeated calls for same ticker)
_CACHE: Dict[str, Dict[str, Any]] = {}

def _get(url: str, params=None) -> Dict[str, Any]:
    r = requests.get(
        url,
        params=params or {},
        timeout=30,
        headers={"User-Agent": "cex-listing-bot"},
    )

    # Handle rate limit gracefully
    if r.status_code == 429:
        # Respect Retry-After if present, but don't hard-fail the bot
        return {"_rate_limited": True}

    # If any other error, don't crash the whole bot
    if r.status_code >= 400:
        return {"_error": f"HTTP {r.status_code}"}

    return r.json()

def search_coin(query: str) -> Optional[Dict[str, Any]]:
    data = _get(f"{CG}/search", {"query": query})
    if data.get("_rate_limited") or data.get("_error"):
        return None
    coins = data.get("coins", [])
    return coins[0] if coins else None

def coin_data(coin_id: str) -> Optional[Dict[str, Any]]:
    data = _get(f"{CG}/coins/{coin_id}", {
        "localization": "false",
        "tickers": "false",
        "market_data": "true",
        "community_data": "false",
        "developer_data": "false",
        "sparkline": "false"
    })
    if data.get("_rate_limited") or data.get("_error"):
        return None
    return data

def enrich(ticker: str) -> Dict[str, Any]:
    """
    Best-effort enrichment. NEVER fails the bot.
    Returns:
      market_cap_usd, volume_24h_usd, platform_contracts
    """
    # Optional hard-disable from workflow / env
    if os.getenv("DISABLE_COINGECKO", "").lower() in ("1", "true", "yes"):
        return {"market_cap_usd": None, "volume_24h_usd": None, "platform_contracts": {}}

    t = (ticker or "").upper().strip()
    if not t:
        return {"market_cap_usd": None, "volume_24h_usd": None, "platform_contracts": {}}

    if t in _CACHE:
        return _CACHE[t]

    out = {"market_cap_usd": None, "volume_24h_usd": None, "platform_contracts": {}}

    hit = search_coin(t)
    if not hit:
        _CACHE[t] = out
        return out

    data = coin_data(hit["id"])
    if not data:
        _CACHE[t] = out
        return out

    md = data.get("market_data", {}) or {}
    out["market_cap_usd"] = (md.get("market_cap", {}) or {}).get("usd")
    out["volume_24h_usd"] = (md.get("total_volume", {}) or {}).get("usd")
    out["platform_contracts"] = data.get("platforms", {}) or {}

    # small delay to be polite (helps reduce 429s on shared runners)
    time.sleep(0.7)

    _CACHE[t] = out
    return out
