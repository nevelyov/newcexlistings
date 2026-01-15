import os
import time
import requests

def send_telegram_message(text: str) -> None:
    token = os.environ["TG_BOT_TOKEN"]
    chat_id = os.environ["TG_CHAT_ID"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}

    # retry a few times on rate limits / transient errors
    for attempt in range(6):
        r = requests.post(url, json=payload, timeout=30)

        # Telegram rate limit
        if r.status_code == 429:
            try:
                data = r.json()
                retry_after = int(data.get("parameters", {}).get("retry_after", 3))
            except Exception:
                retry_after = 3
            time.sleep(retry_after + 1)
            continue

        # transient server errors
        if r.status_code >= 500:
            time.sleep(2 + attempt)
            continue

        # other 4xx: don't crash the whole bot
        if r.status_code >= 400:
            return

        return
