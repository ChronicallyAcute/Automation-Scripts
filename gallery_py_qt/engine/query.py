"""Tiny search query language for the gallery.

Compiles a search string into a predicate over an `info` dict describing an
item.  Tokens are ANDed together:

    holiday                 filename contains "holiday"
    tag:BT                  carries tag BT
    fav:yes   fav:no        is / isn't a favourite
    type:image|gif|video    media class (img/vid accepted)
    w>1920  h<=1080         pixel dimensions
    beach tag:WAM type:gif  combine freely (all must hold)

Bare words are filename substrings (all must match).  Unknown keys fall back
to substring so a stray colon never yields zero results silently.  Every
predicate reads only already-cached data (dims, tags, favourite, media class),
so evaluating it per item during filtering stays cheap.
"""
from __future__ import annotations
import operator
import re

_TOKEN = re.compile(r"^([A-Za-z]+)(<=|>=|<|>|=|:)(.+)$")
_CMP = {"<": operator.lt, ">": operator.gt, "<=": operator.le,
        ">=": operator.ge, "=": operator.eq, ":": operator.eq}
_NUMERIC = {"w": "w", "width": "w", "h": "h", "height": "h"}
_TRUEISH = {"yes", "y", "true", "1", "on", "fav"}


def _num(val: str):
    try:
        return float(val)
    except ValueError:
        return None


def _build(key: str, op: str, val: str):
    key = key.lower()
    if key in _NUMERIC:
        field, num, cmp = _NUMERIC[key], _num(val), _CMP[op]
        if num is None:
            return None
        def pred(info, field=field, num=num, cmp=cmp):
            v = info.get(field)
            return bool(v) and v > 0 and cmp(v, num)
        return pred
    if key in ("fav", "favorite", "favourite"):
        want = val.strip().lower() in _TRUEISH
        return lambda info: bool(info.get("fav")) == want
    if key == "type":
        v = val.strip().lower()
        v = {"img": "image", "vid": "video", "videos": "video",
             "images": "image", "gifs": "gif"}.get(v, v)
        return lambda info: info.get("type") == v
    if key == "tag":
        want = val.strip().lower()
        return lambda info: want in info.get("tags", ())
    return None


def compile_query(text: str):
    """Return predicate(info)->bool, or None for an empty query."""
    text = (text or "").strip()
    if not text:
        return None
    preds = []
    substrings = []
    for tok in text.split():
        m = _TOKEN.match(tok)
        p = _build(*m.groups()) if m else None
        if p is not None:
            preds.append(p)
        else:
            substrings.append(tok.lower())
    if substrings:
        preds.append(lambda info: all(s in info.get("name", "")
                                      for s in substrings))
    if not preds:
        return None
    return lambda info: all(p(info) for p in preds)
