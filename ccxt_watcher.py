import ccxt
import time
import traceback
from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timezone

from utils.state2 import load_state, save_state
from utils.tg import send_telegram_message
from utils.parse import pick_best_contract, extract_contracts
from utils.coingecko import enrich, search_coin
from utils.dexscreener import (
    search as dex_search,
    extract_contract_from_pair,
    extract_pair_url,
)

STATE_PATH = "data/seen_ccxt.json"

# Optional: skip ultra-common tickers on first run (reduces spam)
DEFAULT_SKIP = {"USDT", "USDC", "BTC", "ETH", "BNB", "SOL"}

def _safe_get_contract_from_currency(currency: dict) -> Optional[str]:
    """Rare case: exchange returns contract info in currencies metadata."""
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

def resolve_contract_and_refs(
    ticker: str,
    currency_obj: dict
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Returns: (contract, coingecko_id, dexscreener_pair_url)
    Contract pipeline:
      1) Exchange metadata (rare)
      2) Scan exchange raw info for 0x...
      3) CoinGecko platforms + return CoinGecko ID
      4) DexScreener fallback: contract + pair url
    """
    t = (ticker or "").upper().strip()
    if not t:
        return None, None, None

    # 1) Exchange metadata
    contract = _safe_get_contract_from_currency(currency_obj)
    if contract:
        return contract, None, None

    # 2) Raw info scan
    raw = str((currency_obj or {}).get("info") or "")
    cands = extract_contracts(raw)
    contract = pick_best_contract(cands)
    if contract:
        return contract, None, None

    coingecko_id = None

    # 3) CoinGecko (ID + platforms)
    try:
        hit = search_coin(t)
        if hit and hit.get("id"):
            coingecko_id = hit["id"]

        cg = enrich(t)
        plats = cg.get("platform_contracts") or {}
        for _, addr in plats.items():
            if isinstance(addr, str) and addr:
                return addr, coingecko_id, None
    except Exception:
        pass

    # 4) DexScreener fallback
    try:
        pair = dex_search(t)
        addr = extract_contract_from_pair(pair)
        url = extract_pair_url(pair)
        if addr or url:
            return addr, coingecko_id, url
    except Exception:
        pass

    return None, coingecko_id, None

def build_message(
    exchange_id: str,
    ticker: str,
    contract: Optional[str],
    cg_id: Optional[str],
    dex_url: Optional[str],
    found_at: str
) -> str:
    ex_up = (exchange_id or "").upper()
    lines = [
        "🆕 NEW (CCXT DETECTED)",
        f"Exchange: {ex_up}",
        f"Ticker: {ticker}",
        f"Contract: {contract or 'n/a'}",
        f"Found: {found_at}",
    ]
    if cg_id:
        lines.append(f"CoinGecko ID: {cg_id}")
    if dex_url:
        lines.append(f"DexScreener: {dex_url}")
    return "\n".join(lines)

def run_ccxt_scan(
    shard_index: int = 0,
    shard_total: int = 4,
    max_exchanges_per_run: int = 35,
    skip_common_on_first_run: bool = True,
) -> None:
    # Load state dict: {"seen": { "EXCHANGE:TICKER": "timestamp", ... }}
    state = load_state(STATE_PATH)
    seen_map: Dict[str, str] = state["seen"]

    ids = ccxt.exchanges
    shard_ids = [eid for i, eid in enumerate(ids) if (i % shard_total) == shard_index]
    shard_ids = shard_ids[:max_exchanges_per_run]

    first_run = (len(seen_map) == 0)

    for eid in shard_ids:
        eid_up = (eid or "").upper()
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

            for code, c in currencies.items():
                ticker = (code or "").upper().strip()
                if not ticker:
                    continue

                if first_run and skip_common_on_first_run and ticker in DEFAULT_SKIP:
                    continue

                key = f"{eid_up}:{ticker}"
                if key in seen_map:
                    continue

                found_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                seen_map[key] = found_at  # store timestamp immediately (prevents duplicate spam)

                contract, cg_id, dex_url = resolve_contract_and_refs(ticker, c)

                send_telegram_message(
                    build_message(eid_up, ticker, contract, cg_id, dex_url, found_at)
                )

                time.sleep(0.6)

        except Exception:
            traceback.print_exc()
            continue

    # Save updated timestamps
    save_state(STATE_PATH, state)
