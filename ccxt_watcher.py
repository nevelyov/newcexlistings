import ccxt
import time
import traceback
from typing import Optional, Dict, Any, Tuple

from utils.state2 import load_set, save_set
from utils.tg import send_telegram_message
from utils.parse import pick_best_contract, extract_contracts
from utils.coingecko import enrich, search_coin  # <-- we use search_coin to get CoinGecko ID
from utils.dexscreener import (
    search as dex_search,
    extract_contract_from_pair,
    extract_pair_url,
)

STATE_PATH = "data/seen_ccxt.json"

DEFAULT_SKIP = {"USDT", "USDC", "BTC", "ETH", "BNB", "SOL"}

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

def resolve_contract_and_refs(ticker: str, currency_obj: dict) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Returns: (contract, coingecko_id, dexscreener_pair_url)

    Pipeline:
      1) Exchange metadata contract (rare)
      2) Scan raw exchange info for contract
      3) CoinGecko: get ID + platforms contract
      4) DexScreener: contract + pair url
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

    # 3) CoinGecko (ID + platforms contract)
    try:
        hit = search_coin(t)  # returns first search match dict with 'id'
        if hit and hit.get("id"):
            coingecko_id = hit["id"]

        cg = enrich(t)  # uses coin_data() internally (best-effort)
        plats = cg.get("platform_contracts") or {}
        for _, addr in plats controlled to ignore some:
            if addr:
                return addr, coingecko_id, None
    except Exception:
        pass

    # 4) DexScreener fallback (contract + pair U
