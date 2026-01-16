import os
import time
import random
import requests
from typing import List

API = "https://api.telegram.org"

# локальный лимит на процесс (в Actions это ОДИН шард = ОДИН процесс)
_MIN_INTERVAL_SECONDS = float(os.getenv("TG_MIN_INTERVAL", "0.35"))  # дефолт безопаснее чем 0.25
_LAST_SEND_TS = 0.0


def _parse_chat_ids() -> List[str]:
    """
    Поддержка:
      - TG_CHAT_ID="123"
      - TG_CHAT_IDS="-1001, -1002, 123"
    """
    ids = []
    one = (os.getenv("TG_CHAT_ID") or "").strip()
    many = (os.getenv("TG_CHAT_IDS") or "").strip()

    if one:
        ids.append(one)

    if many:
        for part in many.split(","):
            p = part.strip()
            if p:
                ids.append(p)

    # unique preserve order
    out = []
    seen = set()
    for x in ids:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def _sleep_for_rate_limit():
    global _LAST_SEND_TS
    now = time.time()
    wait = (_LAST_SEND_TS + _MIN_INTERVAL_SECONDS) - now
    if wait > 0:
        time.sleep(wait)
    _LAST_SEND_TS = time.time()


def send_telegram_message(
    text: str,
    parse_mode: str = "MarkdownV2",
    disable_web_page_preview: bool = True,
    max_retries: int = 6,
) -> None:
    token = (os.getenv("TG_BOT_TOKEN") or "").strip()
    if not token:
        return

    chat_ids = _parse_chat_ids()
    if not chat_ids:
        return

    url = f"{API}/bot{token}/sendMessage"

    for chat_id in chat_ids:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": disable_web_page_preview,
        }

        attempt = 0
        while attempt < max_retries:
            attempt += 1
            _sleep_for_rate_limit()

            try:
                r = requests.post(url, json=payload, timeout=25)
            except Exception:
                # network error
                backoff = min(2 ** attempt, 20) + random.uniform(0, 0.4)
                time.sleep(backoff)
                continue

            if r.status_code == 200:
                break

            # Telegram 429
            if r.status_code == 429:
                retry_after = 3
                try:
                    j = r.json()
                    retry_after = int(j.get("parameters", {}).get("retry_after", retry_after))
                except Exception:
                    pass
                time.sleep(min(retry_after + 1, 60))
                continue

            # Other HTTP errors: retry a bit, then give up (don’t crash bot)
            backoff = min(2 ** attempt, 20) + random.uniform(0, 0.4)
            time.sleep(backoff)
