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


def _guess_chain_from_contract(addr: Optional[str]) -> Optional[str]:
    if not addr or not isinstance(addr, str):
        return None
    a = addr.strip()
    if a.startswith("0x") and len(a) == 42:
        return "EVM"
    return None


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


def resolve_contract_and_refs(
    ticker: str,
    currency_obj: dict
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Returns: (contract, coingecko_id, dexscreener_pair_url, chain_guess)
    """
    t = (ticker or "").upper().strip()
    if not t:
        return None, None, None, None

    currency_obj = currency_obj if isinstance(currency_obj, dict) else {}

    # 1) exchange metadata
    contract = _safe_get_contract_from_currency(currency_obj)
    if contract:
        return contract, None, None, _guess_chain_from_contract(contract)

    # 2) raw scan
    raw = str(currency_obj.get("info") or "")
    cands = extract_contracts(raw)
    contract = pick_best_contract(cands)
    if contract:
        return contract, None, None, _guess_chain_from_contract(contract)

    coingecko_id = None

    # 3) CoinGecko
    try:
        hit = search_coin(t)
        if hit and hit.get("id"):
            coingecko_id = hit["id"]

        cg = enrich(t)
        plats = cg.get("platform_contracts") or {}
        for chain_key, addr in plats.items():
            if isinstance(addr, str) and addr:
                chain_guess = str(chain_key).upper() if chain_key else None
                return addr, coingecko_id, None, chain_guess
    except Exception:
        pass

    # 4) DexScreener
    try:
        pair = dex_search(t)
        addr = extract_contract_from_pair(pair)
        url = extract_pair_url(pair)

        chain_guess = None
        if isinstance(pair, dict):
            ch = pair.get("chainId") or pair.get("chain_id")
            if isinstance(ch, str) and ch:
                chain_guess = ch.upper()

        if addr or url:
            return addr, coingecko_id, url, (chain_guess or _guess_chain_from_contract(addr))
    except Exception:
        pass

    return None, coingecko_id, None, None


def _mdv2_escape(s: str) -> str:
    s = s or ""
    for ch in r"_*[]()~`>#+-=|{}.!":
        s = s.replace(ch, "\\" + ch)
    return s


def build_message(
    exchange_id: str,
    ticker: str,
    contract: Optional[str],
    cg_id: Optional[str],
    dex_url: Optional[str],
    chain: Optional[str],
    found_at: str
) -> str:
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
    ]
    if chain:
        lines.append(f"*Chain:* {_mdv2_escape(chain)}")
    lines += [
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
    # ---- HARD limits ----
    start = time.time()
    MAX_SECONDS = 7 * 60              # budget per shard
    MAX_EXCHANGE_SECONDS = 35         # budget per exchange

    # 🔥 важнейшее: лимит сообщений на шард (сильно снижает 429)
    MAX_MSG_PER_SHARD = int(os.getenv("CCXT_MAX_MSG_PER_SHARD", "8"))
    sent = 0

    state = load_state(STATE_PATH)
    seen_map: Dict[str, str] = state["seen"]

    ids = ccxt.exchanges
    shard_ids = [eid for i, eid in enumerate(ids) if (i % shard_total) == shard_index]
    shard_ids = shard_ids[:max_exchanges_per_run]

    first_run = (len(seen_map) == 0)

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
                "timeout": 12000,
            })

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

                key = f"{(eid or '').upper()}:{ticker}"
                if key in seen_map:
                    continue

                found_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                seen_map[key] = found_at

                contract, cg_id, dex_url, chain = resolve_contract_and_refs(ticker, ccy)

                # 🔥 анти-спам: шлём только если есть контракт ИЛИ Dex URL
                if not contract and not dex_url:
                    continue

                send_telegram_message(
                    build_message(eid, ticker, contract, cg_id, dex_url, chain, found_at),
                    parse_mode="MarkdownV2",
                    disable_web_page_preview=True,
                )
                sent += 1

        except Exception:
            traceback.print_exc()
            continue

    save_state(STATE_PATH, state)
