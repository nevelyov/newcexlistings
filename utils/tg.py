import os
import time
import requests
from typing import List, Optional

API = "https://api.telegram.org"

# мягкий лимит по сообщениям (чтобы не ловить 429)
_MIN_INTERVAL_SECONDS = float(os.getenv("TG_MIN_INTERVAL", "0.25"))  # 0.25 сек = 4 msg/sec
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

        # retry loop per chat_id
        attempt = 0
        while True:
            attempt += 1
            _sleep_for_rate_limit()

            try:
                r = requests.post(url, json=payload, timeout=25)
            except Exception:
                if attempt >= max_retries:
                    break
                time.sleep(min(2 ** attempt, 20))
                continue

            # OK
            if r.status_code == 200:
                break

            # Telegram rate limit
            if r.status_code == 429:
                retry_after = 3
                try:
                    j = r.json()
                    retry_after = int(j.get("parameters", {}).get("retry_after", retry_after))
                except Exception:
                    pass

                if attempt >= max_retries:
                    break

                # ждём сколько сказал Telegram (и чуть сверху)
                time.sleep(min(retry_after + 1, 60))
                continue

            # другие ошибки — не валим весь бот, просто уходим
            if attempt >= max_retries:
                break
            time.sleep(min(2 ** attempt, 20))
