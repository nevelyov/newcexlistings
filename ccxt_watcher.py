import ccxt
import time
import traceback
from typing import Optional, Dict, Any, List
from urllib.parse import quote_plus, urlparse

from utils.state2 import load_set, save_set
from utils.tg import send_telegram_message
from utils.parse import pick_best_contract, extract_contracts
from utils.coingecko import enrich

STATE_PATH = "data/seen_ccxt.json"

DEFAULT_SKIP = {
    "USDT", "USDC", "BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "TRX",
    "DOT", "AVAX", "MATIC", "OP", "ARB", "ATOM", "LTC", "BCH", "ETC"
}

def fmt_money(x):
    if x is None:
        return "n/a"
    try:
        x = float(x)
    except Exception:
        return "n/a"
    if x >= 1e9: return f"${x/1e9:.2f}B"
    if x >= 1e6: return f"${x/1e6:.2f}M"
    if x >= 1e3: return f"${x/1e3:.2f}K"
    return f"${x:.2f}"

def _domain_from_url(u: str) -> str:
    if not u:
        return ""
    try:
        p = urlparse(u)
        host = p.netloc or ""
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""

def _candidate_announcement_urls(www_url: str) -> List[str]:
    """
    Try a bunch of common announcement/support/news/blog paths automatically.
    This covers many small exchanges without manual mapping.
    """
    domain = _domain_from_url(www_url)
    if not domain:
        return []
    base = f"https://{domain}"
    paths = [
        "/announcement", "/announcements",
        "/support", "/support/announcement", "/support/announcements",
        "/help", "/help/announcement", "/help/announcements",
        "/blog", "/blogs",
        "/news", "/notice", "/notices",
        "/article", "/articles",
        "/updates", "/update",
        "/listing", "/listings",
        "/market/announcement", "/market/announcements",
    ]
    return [base + p for p in paths]

def guess_announcement_link(exchange_id: str, www_url: str, ticker: str) -> str:
    """
    Best effort:
    1) Known mappings for popular exchanges (nice direct link)
    2) If unknown, return a search link that usually lands on the listing post:
       - First try a likely announcements path (pick the best guess)
       - Then provide DuckDuckGo site search targeting listing keywords + ticker
    """
    ex = (exchange_id or "").lower()
    t = (ticker or "").upper().strip()

    known = {
        "binance": "https://www.binance.com/en/support/announcement",
        "okx": "https://www.okx.com/help/section/announcements-new-listings",
        "bybit": "https://announcements.bybit.com/en/?category=new_crypto",
        "kucoin": "https://www.kucoin.com/announcement/new-listings",
        "mexc": "https://www.mexc.com/announcements/new-listings",
        "gate": "https://www.gate.io/announcements/newlisted",
        "bitget": "https://www.bitget.com/support",
        "htx": "https://www.htx.com/en-in/support/",
        "kraken": "https://support.kraken.com/hc/en-us/sections/360012894412-New-coin-listings",
        "krakenfutures": "https://support.kraken.com/hc/en-us/sections/360012894412-New-coin-listings",
        "coinbase": "https://www.coinbase.com/blog",
        "bitfinex": "https://blog.bitfinex.com/category/announcements/",
        "bitrue": "https://support.bitrue.com/hc/en-us/categories/360000604593-Announcements",
        "digifinex": "https://support.digifinex.com/hc/en-us/categories/360000239354-Announcements",
        "coincatch": "https://www.coincatch.com/en/support",
    }
    if ex in known:
        return known[ex]

    # If unknown exchange: try to point to a likely "announcements" section first.
    # (We can't verify which one exists without extra HTTP checks; keep it lightweight.)
    candidates = _candidate_announcement_urls(www_url)
    if candidates:
        # best heuristic: announcements/support/blog first
        for preferred in ["/announcements", "/announcement", "/support", "/blog", "/news", "/notice"]:
            for c in candidates:
                if c.endswith(preferred):
                    return c

        # fallback: first candidate
        return candidates[0]

    # Ultimate fallback: DuckDuckGo site search (often less blocked than Google)
    domain = _domain_from_url(www_url)
    if domain:
        q = f"site:{domain} (will list OR listing OR listed OR launch OR spot) {t}" if t else f"site:{domain} will list"
        return "https://duckduckgo.com/?q=" + quote_plus(q)

    return "n/a"

def _safe_get_contract_from_currency(currency: dict) -> Optional[str]:
    if not currency:
        return None

    info = currency.get("info") or {}
    for k in ["contractAddress", "contract_address", "tokenAddress", "address", "contract"]:
        v = info.get(k)
        if isinstance(v, str) and v:
            return v

    networks = currency.get("networks") or {}
    candidates = []
    for _, obj in networks.items():
        if isinstance(obj, dict):
            inf = obj.get("info") or {}
            for k in ["contractAddress", "contract_address", "tokenAddress", "address", "contract"]:
                v = inf.get(k)
                if isinstance(v, str) and v:
                    candidates.append(v)

    return pick_best_contract(candidates) if candidates else None

def build_message(exchange_id: str, ticker: str, contract: Optional[str], ann_link: str, mc, vol) -> str:
    return "\n".join([
        "🆕 NEW (CCXT DETECTED)",
        f"Exchange: {exchange_id}",
        f"Ticker: {ticker}",
        f"Contract: {contract or 'n/a'}",
        f"24h Vol: {fmt_money(vol)}",
        f"MCap: {fmt_money(mc)}",
        f"Announcements: {ann_link}",
    ])

def run_ccxt_scan(
    shard_index: int = 0,
    shard_total: int = 4,
    max_exchanges_per_run: int = 35,
    skip_common_on_first_run: bool = True,
) -> None:
    seen = load_set(STATE_PATH)
    new_seen = set(seen)

    ids = ccxt.exchanges
    shard_ids = [eid for i, eid in enumerate(ids) if (i % shard_total) == shard_index]
    shard_ids = shard_ids[:max_exchanges_per_run]

    first_run = (len(seen) == 0)

    for eid in shard_ids:
        try:
            ex_class = getattr(ccxt, eid)
            ex = ex_class({"enableRateLimit": True, "timeout": 20000})

            try:
                ex.load_markets()
            except Exception:
                pass

            currencies: Dict[str, Any] = getattr(ex, "currencies", None) or {}
            if not currencies:
                continue

            www = ""
            try:
                www = (ex.urls.get("www") or "")
            except Exception:
                www = ""

            for code, c in currencies.items():
                ticker = (code or "").upper().strip()
                if not ticker:
                    continue

                if first_run and skip_common_on_first_run and ticker in DEFAULT_SKIP:
                    continue

                key = f"{eid}:{ticker}"
                if key in seen:
                    continue

                # contract from exchange metadata (rare)
                contract = _safe_get_contract_from_currency(c)

                # contract from raw info scan (sometimes)
                if not contract:
                    raw = str(c.get("info") or "")
                    candidates = extract_contracts(raw)
                    contract = pick_best_contract(candidates)

                # enrichment (best-effort; never crashes)
                mc = vol = None
                cg_contract = None
                try:
                    cg = enrich(ticker)
                    mc = cg.get("market_cap_usd")
                    vol = cg.get("volume_24h_usd")

                    if not contract:
                        plats = cg.get("platform_contracts") or {}
                        for _, addr in plats.items():
                            if addr:
                                cg_contract = addr
                                break
                except Exception:
                    pass

                contract_final = contract or cg_contract
                ann_link = guess_announcement_link(eid, www, ticker)

                send_telegram_message(
                    build_message(eid, ticker, contract_final, ann_link, mc, vol)
                )

                new_seen.add(key)
                time.sleep(0.7)

        except Exception:
            traceback.print_exc()
            continue

    if new_seen != seen:
        save_set(STATE_PATH, new_seen)
