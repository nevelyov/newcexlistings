import os
import time
import requests
from typing import Dict, Any, Optional

CG = "https://api.coingecko.com/api/v3"

# Cache per run
_CACHE_ENRICH: Dict[str, Dict[str, Any]] = {}
_CACHE_SEARCH: Dict[str, Optional[Dict[str, Any]]] = {}

def _get(url: str, params=None) -> Dict[str, Any]:
    r = requests.get(
        url,
        params=params or {},
        timeout=30,
        headers={"User-Agent": "cex-listing-bot"},
    )

    if r.status_code == 429:
        return {"_rate_limited": True}

    if r.status_code >= 400:
        return {"_error": f"HTTP {r.status_code}"}

    return r.json()

def search_coin(query: str) -> Optional[Dict[str, Any]]:
    """
    Returns first CoinGecko search hit dict (includes 'id') or None.
    Cached per run.
    """
    q = (query or "").upper().strip()
    if not q:
        return None

    if q in _CACHE_SEARCH:
        return _CACHE_SEARCH[q]

    data = _get(f"{CG}/search", {"query": q})
    if data.get("_rate_limited") or data.get("_error"):
        _CACHE_SEARCH[q] = None
        return None

    coins = data.get("coins", []) or []
    hit = coins[0] if coins else None
    _CACHE_SEARCH[q] = hit
    return hit

def coin_data(coin_id: str) -> Optional[Dict[str, Any]]:
    data = _get(
        f"{CG}/coins/{coin_id}",
        {
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "false",
            "developer_data": "false",
            "sparkline": "false",
        },
    )
    if data.get("_rate_limited") or data.get("_error"):
        return None
    return data

def enrich(ticker: str) -> Dict[str, Any]:
    """
    Best-effort enrichment. NEVER fails.
    Returns:
      market_cap_usd, volume_24h_usd, platform_contracts
    """
    if os.getenv("DISABLE_COINGECKO", "").lower() in ("1", "true", "yes"):
        return {"market_cap_usd": None, "volume_24h_usd": None, "platform_contracts": {}}

    t = (ticker or "").upper().strip()
    if not t:
        return {"market_cap_usd": None, "volume_24h_usd": None, "platform_contracts": {}}

    if t in _CACHE_ENRICH:
        return _CACHE_ENRICH[t]

    out = {"market_cap_usd": None, "volume_24h_usd": None, "platform_contracts": {}}

    hit = search_coin(t)
    if not hit or not hit.get("id"):
        _CACHE_ENRICH[t] = out
        return out

    data = coin_data(hit["id"])
    if not data:
        _CACHE_ENRICH[t] = out
        return out

    md = data.get("market_data", {}) or {}
    out["market_cap_usd"] = (md.get("market_cap", {}) or {}).get("usd")
    out["volume_24h_usd"] = (md.get("total_volume", {}) or {}).get("usd")
    out["platform_contracts"] = data.get("platforms", {}) or {}

    # be polite to avoid 429 on shared runners
    time.sleep(0.7)

    _CACHE_ENRICH[t] = out
    return out
