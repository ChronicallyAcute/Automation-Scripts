"""Persistent preferences and recent-folder list (JSON, best-effort)."""
from __future__ import annotations
import json

from .. import config


def load_prefs() -> dict:
    try:
        with open(config.PREFS_FILE, encoding="utf-8") as f:
            d = json.load(f)
            return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def save_prefs(prefs: dict) -> None:
    try:
        with open(config.PREFS_FILE, "w", encoding="utf-8") as f:
            json.dump(prefs, f, indent=2)
    except Exception:
        pass


def load_recent() -> list[str]:
    try:
        with open(config.RECENT_FILE, encoding="utf-8") as f:
            d = json.load(f)
            return ([p for p in d if isinstance(p, str)]
                    if isinstance(d, list) else [])
    except Exception:
        return []


def save_recent(folders: list[str]) -> None:
    try:
        with open(config.RECENT_FILE, "w", encoding="utf-8") as f:
            json.dump(folders[:10], f, indent=2)
    except Exception:
        pass
