import requests
from typing import Dict, Any, Optional

CG = "https://api.coingecko.com/api/v3"

def _get(url: str, params=None) -> Dict[str, Any]:
    r = requests.get(url, params=params or {}, timeout=30, headers={"User-Agent":"cex-listing-bot"})
    r.raise_for_status()
    return r.json()

def search_coin(query: str) -> Optional[Dict[str, Any]]:
    data = _get(f"{CG}/search", {"query": query})
    coins = data.get("coins", [])
    return coins[0] if coins else None

def coin_data(coin_id: str) -> Dict[str, Any]:
    return _get(f"{CG}/coins/{coin_id}", {
        "localization":"false",
        "tickers":"false",
        "market_data":"true",
        "community_data":"false",
        "developer_data":"false",
        "sparkline":"false"
    })

def enrich(ticker: str) -> Dict[str, Any]:
    out = {"market_cap_usd": None, "volume_24h_usd": None, "platform_contracts": {}}
    hit = search_coin(ticker)
    if not hit:
        return out
    d = coin_data(hit["id"])
    md = d.get("market_data", {}) or {}
    out["market_cap_usd"] = (md.get("market_cap", {}) or {}).get("usd")
    out["volume_24h_usd"] = (md.get("total_volume", {}) or {}).get("usd")
    out["platform_contracts"] = d.get("platforms", {}) or {}
    return out
