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


def _mdv2_escape(s: str) -> str:
    for ch in r"_*[]()~`>#+-=|{}.!":
        s = s.replace(ch, "\\" + ch)
    return s


def _safe_get_contract_and_chain_from_currency(currency: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Пытаемся достать contract (+chain/network, если есть) из ccxt currency object.
    Возвращает (contract, chain) или (None, None)
    """
    if not isinstance(currency, dict):
        return None, None

    info = _as_dict(currency.get("info"))

    # 1) simple keys in currency.info
    for k in ["contractAddress", "contract_address", "tokenAddress", "address", "contract"]:
        v = info.get(k)
        if isinstance(v, str) and v:
            # chain иногда лежит рядом
            chain = info.get("network") or info.get("chain") or info.get("chainName")
            if isinstance(chain, str) and chain.strip():
                return v, chain.strip()
            return v, None

    # 2) networks dict
    networks = currency.get("networks") if isinstance(currency.get("networks"), dict) else {}
    candidates = []
    for net_name, obj in networks.items():
        if not isinstance(obj, dict):
            continue
        inf = _as_dict(obj.get("info"))
        for k in ["contractAddress", "contract_address", "tokenAddress", "address", "contract"]:
            v = inf.get(k)
            if isinstance(v, str) and v:
                # net_name в ccxt часто = chain
                chain = None
                if isinstance(net_name, str) and net_name.strip():
                    chain = net_name.strip()
                # или в info
                alt = inf.get("network") or inf.get("chain") or inf.get("chainName")
                if isinstance(alt, str) and alt.strip():
                    chain = alt.strip()
                candidates.append((v, chain))

    if not candidates:
        return None, None

    # pick_best_contract выбирает адрес, но chain нужно сопоставить.
    addrs = [a for a, _ in candidates]
    best = pick_best_contract(addrs)
    for a, ch in candidates:
        if a == best:
            return a, ch
    return best, None


def resolve_contract_chain_and_refs(
    ticker: str,
    currency_obj: dict
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Returns: (contract, chain, coingecko_id, dex_url)
    chain — если удалось достать (например ETH/BSC/SOL etc), иначе None
    """
    t = (ticker or "").upper().strip()
    if not t:
        return None, None, None, None

    currency_obj = currency_obj if isinstance(currency_obj, dict) else {}

    # 1) exchange metadata (best)
    contract, chain = _safe_get_contract_and_chain_from_currency(currency_obj)
    if contract:
        return contract, chain, None, None

    # 2) raw scan of info
    raw = str(currency_obj.get("info") or "")
    cands = extract_contracts(raw)
    contract = pick_best_contract(cands)
    if contract:
        # chain тут обычно неизвестен
        return contract, None, None, None

    coingecko_id = None

    # 3) CoinGecko
    try:
        hit = search_coin(t)
        if hit and hit.get("id"):
            coingecko_id = hit["id"]

        cg = enrich(t)
        plats = cg.get("platform_contracts") or {}
        # plats: { "ethereum": "0x...", "binance-smart-chain": "0x..." ... }
        if isinstance(plats, dict):
            for ch, addr in plats.items():
                if isinstance(addr, str) and addr:
                    chain = ch if isinstance(ch, str) and ch else None
                    return addr, chain, coingecko_id, None
    except Exception:
        pass

    # 4) DexScreener
    try:
        pair = dex_search(t)
        addr = extract_contract_from_pair(pair)
        url = extract_pair_url(pair)
        # chain может быть внутри pair (зависит от твоей реализации utils.dexscreener)
        chain = None
        if isinstance(pair, dict):
            chain = pair.get("chainId") or pair.get("chain") or pair.get("network")
            if isinstance(chain, str):
                chain = chain.strip() or None
        return addr, chain, coingecko_id, url
    except Exception:
        pass

    return None, None, coingecko_id, None


def build_message(
    exchange_id: str,
    ticker: str,
    contract: Optional[str],
    chain: Optional[str],
    cg_id: Optional[str],
    dex_url: Optional[str],
    found_at: str
) -> str:
    ex_up = _mdv2_escape((exchange_id or "").upper())
    t = _mdv2_escape(ticker or "")
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

    # chain показываем только если есть
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
    # ---- HARD limits to always finish before GitHub timeout ----
    start = time.time()
    MAX_SECONDS = 6 * 60              # whole shard budget (6 min)
    MAX_EXCHANGE_SECONDS = 30         # budget per exchange (30 sec)

    state = load_state(STATE_PATH)
    seen_map: Dict[str, str] = state["seen"]

    ids = ccxt.exchanges
    shard_ids = [eid for i, eid in enumerate(ids) if (i % shard_total) == shard_index]
    shard_ids = shard_ids[:max_exchanges_per_run]

    first_run = (len(seen_map) == 0)

    for eid in shard_ids:
        if time.time() - start > MAX_SECONDS:
            break

        ex_start = time.time()

        try:
            ex_class = getattr(ccxt, eid)
            ex = ex_class({
                "enableRateLimit": True,
                "timeout": 12000,  # reduce hanging
            })

            # load_markets can hang — keep exchange budget
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

                contract, chain, cg_id, dex_url = resolve_contract_chain_and_refs(ticker, ccy)

                send_telegram_message(
                    build_message(eid, ticker, contract, chain, cg_id, dex_url, found_at),
                    parse_mode="MarkdownV2"
                )

        except Exception:
            traceback.print_exc()
            continue

    save_state(STATE_PATH, state)
