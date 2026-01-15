import ccxt
import time
import traceback
from typing import List, Dict, Optional
from utils.coingecko import enrich

from utils.state2 import load_set, save_set
from utils.tg import send_telegram_message
from utils.parse import pick_best_contract, extract_contracts

STATE_PATH = "data/seen_ccxt.json"

def _safe_get_contract_from_currency(currency: dict) -> Optional[str]:
    """
    Exchanges sometimes include contract info inside currency['info'].
    There's no standard field across exchanges, so we try common patterns.
    """
    if not currency:
        return None
    info = currency.get("info") or {}

    # common keys across some exchanges
    for k in ["contractAddress", "contract_address", "tokenAddress", "address", "contract"]:
        v = info.get(k)
        if isinstance(v, str) and v:
            return v

    # sometimes nested per network
    networks = currency.get("networks") or {}
    # networks can look like {"ERC20": {"info": {...}}}
    candidates = []
    for net, obj in networks.items():
        if isinstance(obj, dict):
            inf = obj.get("info") or {}
            for k in ["contractAddress", "contract_address", "tokenAddress", "address", "contract"]:
                v = inf.get(k)
                if isinstance(v, str) and v:
                    candidates.append(v)

    return pick_best_contract(candidates) if candidates else None

def _fmt(exchange: str, code: str, contract: Optional[str], url: str) -> str:
    return "\n".join([
        "🆕 NEW (CCXT DETECTED)",
        f"Exchange: {exchange}",
        f"Ticker: {code}",
        f"Contract: {contract or 'n/a'}",
        f"Link: {url}",
    ])

def run_ccxt_scan(shard_index: int = 0, shard_total: int = 4, max_exchanges_per_run: int = 40) -> None:
    """
    We shard the exchange list so GitHub Actions can handle it.
    shard_total=4 means 4 parallel jobs, each scans ~1/4 exchanges.
    """
    seen = load_set(STATE_PATH)
    new_seen = set(seen)

    ids = ccxt.exchanges  # huge list (~100+)
    # deterministic shard split
    shard_ids = [eid for i, eid in enumerate(ids) if (i % shard_total) == shard_index]
    shard_ids = shard_ids[:max_exchanges_per_run]

    for eid in shard_ids:
        try:
            ex_class = getattr(ccxt, eid)
            ex = ex_class({
                "enableRateLimit": True,
                "timeout": 20000,
            })

            # many exchanges support load_markets without auth
            markets = ex.load_markets()

            # currencies map may include extra metadata sometimes
            currencies = getattr(ex, "currencies", None) or {}

            # Baseline: record all base currencies we see on this exchange
            # "listing detection" = currency newly appears vs previous runs
            for code, c in currencies.items():
                key = f"{eid}:{code}"
                if key in seen:
                    continue

                # Try to extract contract
                contract = _safe_get_contract_from_currency(c)

                # extra fallback: scan raw info text for 0x... etc.
                if not contract:
                    raw = str(c.get("info") or "")
                    candidates = extract_contracts(raw)
                    contract = pick_best_contract(candidates)

                send_telegram_message(_fmt(eid, code, contract, ex.urls.get("www") or ""))

                new_seen.add(key)

            # polite pause
            time.sleep(1.0)

        except Exception:
            # never kill the whole job on one exchange
            traceback.print_exc()
            continue

    if new_seen != seen:
        save_set(STATE_PATH, new_seen)
