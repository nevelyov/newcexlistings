import ccxt
import time
import traceback
from typing import Optional, Dict, Any
from urllib.parse import quote_plus

from utils.state2 import load_set, save_set
from utils.tg import send_telegram_message
from utils.parse import pick_best_contract, extract_contracts
from utils.coingecko import enrich

STATE_PATH = "data/seen_ccxt.json"

# Optional: skip ultra-common tickers to reduce noise on first run
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

def guess_announcement_link(exchange_id: str, www_url: str, ticker: str) -> str:
    """
    Best effort:
    - for known exchanges return their actual listing/announcement section
    - otherwise return a Google 'site:' query with the ticker included
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

    # fallback: Google "site:" search
    if www_url:
        try:
            domain = www_url.replace("https://", "").replace("http://", "").split("/")[0]
            # more flexible query
            q = f"site:{domain} (will list OR listing OR listed OR launch) {t}" if t else f"site:{domain} will list"
            return "https://www.google.com/search?q=" + quote_plus(q)
        except Exception:
            pass

    return "n/a"

def _safe_get_contract_from_currency(currency: dict) -> Optional[str]:
    """
    Some exchanges include contract info in currency metadata, but there's no standard.
    We'll try common keys and network objects.
    """
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
    """
    Scans CCXT exchanges (sharded).
    Listing signal (v1): "new currency code appeared in currencies()"
    Notes:
      - It's noisy on first run (will dump a lot). After that it becomes "only new".
      - Many exchanges do NOT expose contracts via API; we fallback to CoinGecko.
    """
    seen = load_set(STATE_PATH)
    new_seen = set(seen)

    ids = ccxt.exchanges
    shard_ids = [eid for i, eid in enumerate(ids) if (i % shard_total) == shard_index]
    shard_ids = shard_ids[:max_exchanges_per_run]

    first_run = (len(seen) == 0)

    for eid in shard_ids:
        try:
            ex_class = getattr(ccxt, eid)
            ex = ex_class({
                "enableRateLimit": True,
                "timeout": 20000,
            })

            # load markets first (some exchanges populate currencies after this)
            try:
                ex.load_markets()
            except Exception:
                # still try currencies if markets fails
                pass

            currencies: Dict[str, Any] = getattr(ex, "currencies", None) or {}

            # if currencies is empty, skip
            if not currencies:
                continue

            for code, c in currencies.items():
                ticker = (code or "").upper().strip()
                if not ticker:
                    continue

                # reduce noise on very first run
                if first_run and skip_common_on_first_run and ticker in DEFAULT_SKIP:
                    continue

                key = f"{eid}:{ticker}"
                if key in seen:
                    continue

                # contract attempt #1: exchange metadata
                contract = _safe_get_contract_from_currency(c)

                # contract attempt #2: scan raw info
                if not contract:
                    raw = str(c.get("info") or "")
                    candidates = extract_contracts(raw)
                    contract = pick_best_contract(candidates)

                # enrichment (mcap/vol + maybe platform contract)
                mc = vol = None
                cg_contract = None
                try:
                    cg = enrich(ticker)  # your coingecko is non-fatal now
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

                www = ""
                try:
                    www = (ex.urls.get("www") or "")
                except Exception:
                    www = ""

                ann_link = guess_announcement_link(eid, www, ticker)

                send_telegram_message(
                    build_message(eid, ticker, contract_final, ann_link, mc, vol)
                )

                new_seen.add(key)

                # small pause helps avoid bans / rate limits
                time.sleep(0.7)

        except Exception:
            traceback.print_exc()
            continue

    if new_seen != seen:
        save_set(STATE_PATH, new_seen)
