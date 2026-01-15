import json
from pathlib import Path
from typing import Set

def load_set(path: str) -> Set[str]:
    p = Path(path)
    if not p.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"seen": []}, indent=2), encoding="utf-8")
    data = json.loads(p.read_text(encoding="utf-8"))
    return set(data.get("seen", []))

def save_set(path: str, s: Set[str]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"seen": sorted(list(s))}, indent=2), encoding="utf-8")
