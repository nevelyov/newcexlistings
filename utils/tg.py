import os
import time
import requests

def _send_one(token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }

    for attempt in range(6):
        r = requests.post(url, json=payload, timeout=30)

        if r.status_code == 429:
            try:
                data = r.json()
                retry_after = int(data.get("parameters", {}).get("retry_after", 3))
            except Exception:
                retry_after = 3
            time.sleep(retry_after + 1)
            continue

        if r.status_code >= 500:
            time.sleep(2 + attempt)
            continue

        if r.status_code >= 400:
            return

        return

def send_telegram_message(text: str) -> None:
    token = os.environ["TG_BOT_TOKEN"]
    chat_ids_raw = os.getenv("TG_CHAT_IDS") or os.getenv("TG_CHAT_ID") or ""
    chat_ids = [x.strip() for x in chat_ids_raw.split(",") if x.strip()]
    if not chat_ids:
        return

    for cid in chat_ids:
        _send_one(token, cid, text)
        time.sleep(0.2)
