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
DEFAULT_SKIP = {"USDT", "USDC", "BTC", "ETH", "BNB", "SOL"}


def _as_dict(x) -> dict:
    return x if isinstance(x, dict) else {}


def _safe_get_contract_from_currency(currency: dict) -> Optional[str]:
    if not isinstance(currency, dict):
        return None

    info = _as_dict(currency.get("info"))
    for k in ["contractAddress", "contract_address", "tokenAddress", "address", "contract"]:
        v = info.get(k)
        if isinstance(v, str) and v:
            return v

    networks = currency.get("networks") if isinstance(currency.get("networks"), dict) else {}
    candidates = []
    for _, obj in networks.items():
        if isinstance(obj, dict):
            inf = _as_dict(obj.get("info"))
            for k in ["contractAddress", "contract_address", "tokenAddress", "address", "contract"]:
                v = inf.get(k)
                if isinstance(v, str) and v:
                    candidates.append(v)

    return pick_best_contract(candidates) if candidates else None


def resolve_contract_and_refs(ticker: str, currency_obj: dict) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    t = (ticker or "").upper().strip()
    if not t:
        return None, None, None

    currency_obj = currency_obj if isinstance(currency_obj, dict) else {}

    # 1) exchange metadata
    contract = _safe_get_contract_from_currency(currency_obj)
    if contract:
        return contract, None, None

    # 2) raw scan
    raw = str(currency_obj.get("info") or "")
    cands = extract_contracts(raw)
    contract = pick_best_contract(cands)
    if contract:
        return contract, None, None

    coingecko_id = None

    # 3) CoinGecko
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

    # 4) DexScreener
    try:
        pair = dex_search(t)
        addr = extract_contract_from_pair(pair)
        url = extract_pair_url(pair)
        if addr or url:
            return addr, coingecko_id, url
    except Exception:
        pass

    return None, coingecko_id, None


def _mdv2_escape(s: str) -> str:
    s = s or ""
    for ch in r"_*[]()~`>#+-=|{}.!":
        s = s.replace(ch, "\\" + ch)
    return s


def build_message(exchange_id: str, ticker: str, contract: Optional[str],
                  cg_id: Optional[str], dex_url: Optional[str], found_at: str) -> str:
    ex_up = _mdv2_escape((exchange_id or "").upper())
    t = _mdv2_escape((ticker or "").upper())
    fa = _mdv2_escape(found_at or "")

    if contract:
        c = f"`{_mdv2_escape(contract)}`"
    else:
        c = "n/a"

    lines = [
        "🆕 *NEW* \\(CCXT DETECTED\\)",
        f"*Exchange:* {ex_up}",
        f"*Ticker:* {t}",
        f"*Contract:* {c}",
        f"*Found:* {fa}",
    ]
    if cg_id:
        lines.append(f"*CoinGecko ID:* {_mdv2_escape(cg_id)}")
    if dex_url:
        lines.append(f"*DexScreener:* {_mdv2_escape(dex_url)}")
    return "\n".join(lines)


def run_ccxt_scan(
    shard_index: int = 0,
    shard_total: int = 4,
    max_exchanges_per_run: int = 35,
    skip_common_on_first_run: bool = True,
) -> None:
    # ---- HARD limits to always finish before GitHub timeout ----
    # 8.5 минут на шард (чтобы 4 шарда + install deps укладывались в 20 минут джобы)
    start = time.time()
    MAX_SECONDS = int(float(__import__("os").getenv("CCXT_SHARD_BUDGET_SECONDS", "510")))  # 8m30s
    MAX_EXCHANGE_SECONDS = int(float(__import__("os").getenv("CCXT_EXCHANGE_BUDGET_SECONDS", "25")))
    MAX_MSG_PER_SHARD = int(float(__import__("os").getenv("CCXT_MAX_MSG_PER_SHARD", "12")))

    state = load_state(STATE_PATH)
    seen_map: Dict[str, str] = state["seen"]

    ids = ccxt.exchanges
    shard_ids = [eid for i, eid in enumerate(ids) if (i % shard_total) == shard_index]
    shard_ids = shard_ids[:max_exchanges_per_run]

    first_run = (len(seen_map) == 0)
    sent = 0

    for eid in shard_ids:
        if time.time() - start > MAX_SECONDS:
            break
        if sent >= MAX_MSG_PER_SHARD:
            break

        ex_start = time.time()

        try:
            ex_class = getattr(ccxt, eid)
            ex = ex_class({
                "enableRateLimit": True,
                "timeout": 12000,  # меньше зависаний
            })

            # load_markets иногда долго — но мы ограничиваем бюджетом биржи
            try:
                ex.load_markets()
            except Exception:
                pass

            if time.time() - ex_start > MAX_EXCHANGE_SECONDS:
                continue

            currencies: Dict[str, Any] = getattr(ex, "currencies", None) or {}
            if not isinstance(currencies, dict) or not currencies:
                continue

            for code, ccy in currencies.items():
                if time.time() - start > MAX_SECONDS:
                    break
                if time.time() - ex_start > MAX_EXCHANGE_SECONDS:
                    break
                if sent >= MAX_MSG_PER_SHARD:
                    break

                ticker = (code or "").upper().strip()
                if not ticker:
                    continue

                if first_run and skip_common_on_first_run and ticker in DEFAULT_SKIP:
                    continue

                ex_up = (eid or "").upper()
                key = f"{ex_up}:{ticker}"
                if key in seen_map:
                    continue

                found_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                seen_map[key] = found_at  # сразу пишем, чтобы не спамить дублями

                contract, cg_id, dex_url = resolve_contract_and_refs(ticker, ccy)

                send_telegram_message(build_message(ex_up, ticker, contract, cg_id, dex_url, found_at))
                sent += 1

        except Exception:
            traceback.print_exc()
            continue

    save_state(STATE_PATH, state)
