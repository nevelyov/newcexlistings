import re
from typing import Optional, List, Tuple

EVM_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
SOL_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")
TICKER_PARENS_RE = re.compile(r"\(([A-Za-z0-9\-\.]{1,15})\)")

def extract_ticker(title: str) -> Optional[str]:
    if not title:
        return None
    m = TICKER_PARENS_RE.search(title)
    if m:
        return m.group(1).upper()
    m2 = re.search(r"\b([A-Z0-9]{2,12})/USDT\b", title)
    if m2:
        return m2.group(1).upper()
    return None

def extract_contracts(text: str) -> List[str]:
    if not text:
        return []
    out = []
    out += EVM_RE.findall(text)

    low = text.lower()
    if "solana" in low or "spl" in low:
        out += SOL_RE.findall(text)

    seen = set()
    deduped = []
    for c in out:
        if c not in seen:
            seen.add(c)
            deduped.append(c)
    return deduped

def pick_best_contract(contracts: List[str]) -> Optional[str]:
    if not contracts:
        return None
    for c in contracts:
        if c.startswith("0x") and len(c) == 42:
            return c
    return contracts[0]

def summarize(title: str, body: str) -> Tuple[Optional[str], Optional[str]]:
    ticker = extract_ticker(title or "")
    contracts = extract_contracts((title or "") + "\n" + (body or ""))
    best = pick_best_contract(contracts)
    return ticker, best
