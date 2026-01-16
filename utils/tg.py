import os
import time
import random
import requests
from typing import List

API = "https://api.telegram.org"

# Ставь в workflow TG_MIN_INTERVAL="1.2" или "1.5" для почти нулевых 429.
_MIN_INTERVAL_SECONDS = float(os.getenv("TG_MIN_INTERVAL", "1.2"))

# небольшая "дробилка" (jitter), чтобы 4 шарда не били одновременно
_JITTER_SECONDS = float(os.getenv("TG_JITTER", "0.35"))

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
    """
    Глобальный лимитер на процесс.
    В GH Actions у каждого шарда свой процесс -> лимит применяется на шард.
    Jitter уменьшает синхронные пики от 4 шардов.
    """
    global _LAST_SEND_TS
    now = time.time()

    jitter = random.uniform(0.0, _JITTER_SECONDS) if _JITTER_SECONDS > 0 else 0.0
    earliest = _LAST_SEND_TS + _MIN_INTERVAL_SECONDS + jitter

    wait = earliest - now
    if wait > 0:
        time.sleep(wait)

    _LAST_SEND_TS = time.time()


def send_telegram_message(
    text: str,
    parse_mode: str = "MarkdownV2",
    disable_web_page_preview: bool = True,
    max_retries: int = 8,
) -> None:
    """
    Best-effort sender:
    - процессный rate limit + jitter
    - retries with backoff
    - respects Telegram 429 retry_after
    - НЕ падает с исключениями
    """
    token = (os.getenv("TG_BOT_TOKEN") or "").strip()
    if not token:
        return
    if not isinstance(text, str) or not text.strip():
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
                # network/backoff
                backoff = min(2 ** attempt, 30) + random.uniform(0.0, 0.5)
                time.sleep(backoff)
                continue

            if r.status_code == 200:
                break

            if r.status_code == 429:
                # Telegram tells exact seconds to wait
                retry_after = 3
                try:
                    j = r.json()
                    retry_after = int(j.get("parameters", {}).get("retry_after", retry_after))
                except Exception:
                    pass

                # поднимем локальный "последний send", чтобы следующие отправки тоже сдвинулись
                global _LAST_SEND_TS
                _LAST_SEND_TS = time.time() + retry_after

                time.sleep(min(retry_after + 1, 90))
                continue

            # другие HTTP ошибки: чуть подождать и повторить
            backoff = min(2 ** attempt, 30) + random.uniform(0.0, 0.5)
            time.sleep(backoff)
