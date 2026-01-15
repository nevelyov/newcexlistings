import requests
from typing import Optional, Dict, Any

BASE = "https://api.dexscreener.com/latest/dex"

def search(token: str) -> Optional[Dict[str, Any]]:
    q = (token or "").strip()
    if not q:
        return None

    r = requests.get(
        f"{BASE}/search",
        params={"q": q},
        timeout=30,
        headers={"User-Agent": "cex-listing-bot"},
    )
    if r.status_code >= 400:
        return None
    data = r.json() or {}
    pairs = data.get("pairs") or []
    return pairs[0] if pairs else None

def extract_contract_from_pair(pair: Dict[str, Any]) -> Optional[str]:
    if not pair:
        return None
    base = pair.get("baseToken") or {}
    addr = base.get("address")
    if isinstance(addr, str) and addr:
        return addr
    return None

def extract_pair_url(pair: Dict[str, Any]) -> Optional[str]:
    if not pair:
        return None
    u = pair.get("url")
    if isinstance(u, str) and u:
        return u
    return None
