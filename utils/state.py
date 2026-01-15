import json
from pathlib import Path
from typing import Set

STATE_PATH = Path("data/seen.json")

def load_seen() -> Set[str]:
    if not STATE_PATH.exists():
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(json.dumps({"seen_ids": []}, indent=2))
    data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return set(data.get("seen_ids", []))

def save_seen(seen: Set[str]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(
        json.dumps({"seen_ids": sorted(list(seen))}, indent=2),
        encoding="utf-8"
    )
