import time
import hashlib
import requests
import yaml
import os
from ccxt_watcher import run_ccxt_scan
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from utils.state import load_seen, save_seen
from utils.tg import send_telegram_message
from utils.parse import summarize
from utils.coingecko import enrich


HEADERS = {"User-Agent": "Mozilla/5.0 (cex-listing-bot)"}

def stable_id(exchange: str, url: str, title: str) -> str:
    base = f"{exchange}|{url}|{title}".encode("utf-8")
    return hashlib.sha256(base).hexdigest()[:24]

def fetch_html(url: str) -> str:
    r = requests.get(url, timeout=30, headers=HEADERS)
    r.raise_for_status()
    return r.text

def parse_listing_links(cfg: dict) -> list[dict]:
    html = fetch_html(cfg["url"])
    soup = BeautifulSoup(html, "lxml")

    items = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(" ", strip=True)

        if cfg.get("link_contains") and (cfg["link_contains"] not in href):
            continue

        if href.startswith("/"):
            href = urljoin(cfg["url"], href)

        if not text:
            continue

        low = text.lower()
        kws = [k.lower() for k in cfg.get("keywords_any", [])]
        if kws and not any(k in low for k in kws):
            continue

        items.append({"title": text, "url": href})

    seen_urls = set()
    out = []
    for it in items:
        if it["url"] in seen_urls:
            continue
        seen_urls.add(it["url"])
        out.append(it)
    return out[:40]

def fetch_detail_text(url: str) -> str:
    try:
        html = fetch_html(url)
    except Exception:
        return ""
    soup = BeautifulSoup(html, "lxml")
    return soup.get_text("\n", strip=True)[:20000]

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

def run():
    with open("config/exchanges.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    exchanges = cfg["exchanges"]

    seen = load_seen()
    new_seen = set(seen)
    messages = []

    for ex in exchanges:
        if ex.get("type") != "html":
            continue

        ex_name = ex["name"]
        try:
            links = parse_listing_links(ex)
        except Exception:
            continue

        for it in links:
            sid = stable_id(ex_name, it["url"], it["title"])
            if sid in seen:
                continue

            detail_text = fetch_detail_text(it["url"])
            ticker, contract = summarize(it["title"], detail_text)

            mc = vol = None
            cg_contract_hint = None
            if ticker:
                cg = enrich(ticker)
                mc = cg.get("market_cap_usd")
                vol = cg.get("volume_24h_usd")
                plats = cg.get("platform_contracts") or {}
                for chain, addr in plats.items():
                    if addr:
                        cg_contract_hint = f"{chain}:{addr}"
                        break

            contract_final = contract or (cg_contract_hint.split(":",1)[1] if cg_contract_hint and ":" in cg_contract_hint else None)

            msg = []
            msg.append("🆕 NEW LISTING")
            msg.append(f"Exchange: {ex_name}")
            msg.append(f"Ticker: {ticker or 'n/a'}")
            msg.append(f"Contract: {contract_final or 'n/a'}")
            if cg_contract_hint and not contract:
                msg.append(f"Contract source: CoinGecko ({cg_contract_hint.split(':',1)[0]})")
            msg.append(f"24h Vol: {fmt_money(vol)}")
            msg.append(f"MCap: {fmt_money(mc)}")
            msg.append(f"Link: {it['url']}")
            messages.append("\n".join(msg))

            new_seen.add(sid)

    for m in messages[:20]:
        send_telegram_message(m)
        time.sleep(1)

    if new_seen != seen:
        save_seen(new_seen)

if __name__ == "__main__":
    run()
