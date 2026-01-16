import os
import json
import hashlib
import requests
import yaml
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from ccxt_watcher import run_ccxt_scan
from utils.state import load_seen, save_seen
from utils.tg import send_telegram_message
from utils.parse import summarize
from utils.coingecko import enrich

HEADERS = {"User-Agent": "Mozilla/5.0 (cex-listing-bot)"}
PENDING_HTML_PATH = "data/pending_html.json"


def stable_id(exchange: str, url: str, title: str) -> str:
    base = f"{exchange}|{url}|{title}".encode("utf-8")
    return hashlib.sha256(base).hexdigest()[:24]


def _msg_id(text: str, parse_mode: str) -> str:
    base = f"{parse_mode}||{text}".encode("utf-8")
    return hashlib.sha256(base).hexdigest()[:24]


def _load_json_list(path: str) -> list[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            x = json.load(f)
        if isinstance(x, list):
            return [i for i in x if isinstance(i, dict) and i.get("text")]
    except Exception:
        pass
    return []


def _save_json_list(path: str, items: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _pending_add_unique(pending: list[dict], text: str, parse_mode: str, max_items: int = 200) -> None:
    pid = _msg_id(text, parse_mode)
    ids = set([x.get("id") for x in pending if x.get("id")])
    if pid in ids:
        return
    pending.append({"id": pid, "text": text, "parse_mode": parse_mode})
    if len(pending) > max_items:
        pending[:] = pending[-max_items:]


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
    if x >= 1e9:
        return f"${x/1e9:.2f}B"
    if x >= 1e6:
        return f"${x/1e6:.2f}M"
    if x >= 1e3:
        return f"${x/1e3:.2f}K"
    return f"${x:.2f}"


def _html_escape(s: str) -> str:
    if s is None:
        return ""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _flush_pending_html(pending: list[dict], max_to_send: int = 2) -> list[dict]:
    if not pending:
        return pending

    still = []
    sent = 0
    for item in pending:
        if sent >= max_to_send:
            still.append(item)
            continue
        text = item.get("text") or ""
        ok = send_telegram_message(text, parse_mode="HTML", disable_web_page_preview=True)
        if ok:
            sent += 1
        else:
            still.append(item)
    return still


def run_announcements_scan(max_messages: int) -> None:
    with open("config/exchanges.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    exchanges = cfg.get("exchanges", []) or []

    seen = load_seen()
    new_seen = set(seen)

    pending = _load_json_list(PENDING_HTML_PATH)
    pending = _flush_pending_html(pending, max_to_send=2)

    sent = 0

    for ex in exchanges:
        if sent >= max_messages:
            break
        if ex.get("type") != "html":
            continue

        ex_name = (ex.get("name") or "").strip()
        if not ex_name:
            continue

        try:
            links = parse_listing_links(ex)
        except Exception:
            continue

        for it in links:
            if sent >= max_messages:
                break

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

            contract_final = contract
            if not contract_final and cg_contract_hint and ":" in cg_contract_hint:
                contract_final = cg_contract_hint.split(":", 1)[1]

            lines = []
            lines.append("🆕 <b>NEW LISTING</b>")
            lines.append(f"<b>Exchange:</b> {_html_escape(ex_name.upper())}")
            lines.append(f"<b>Ticker:</b> {_html_escape((ticker or 'n/a').upper())}")
            if contract_final:
                lines.append(f"<b>Contract:</b> <code>{_html_escape(contract_final)}</code>")
            else:
                lines.append("<b>Contract:</b> n/a")
            lines.append(f"<b>24h Vol:</b> {_html_escape(fmt_money(vol))}")
            lines.append(f"<b>MCap:</b> {_html_escape(fmt_money(mc))}")
            lines.append(f"<b>Link:</b> {_html_escape(it['url'])}")

            msg = "\n".join(lines)

            ok = send_telegram_message(msg, parse_mode="HTML", disable_web_page_preview=True)
            if ok:
                sent += 1
            else:
                _pending_add_unique(pending, msg, "HTML", max_items=200)

            new_seen.add(sid)

    if new_seen != seen:
        save_seen(new_seen)

    _save_json_list(PENDING_HTML_PATH, pending)


def main():
    shard_index = int(os.getenv("SHARD_INDEX", "0"))
    shard_total = int(os.getenv("SHARD_TOTAL", "4"))

    run_ccxt_scan(shard_index=shard_index, shard_total=shard_total)

    if shard_index == 0:
        html_max = int(os.getenv("HTML_MAX_MSG", "4"))
        run_announcements_scan(max_messages=html_max)


if __name__ == "__main__":
    main()
