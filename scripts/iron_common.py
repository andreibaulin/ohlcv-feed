#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IRON common helpers (stdlib-only).

Goals:
- Deterministic extraction via JSON Pointer (RFC 6901)
- sha256 for provenance / fail-closed verification
"""

from __future__ import annotations
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())

def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))

def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # compact but stable (sorted keys)
    path.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True), encoding="utf-8")

def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")

def json_pointer_get(doc: Any, pointer: str) -> Any:
    """
    RFC 6901: pointer like "/a/b/0".
    Supports "~1" => "/", "~0" => "~".
    """
    if pointer == "" or pointer == "/":
        return doc
    if not pointer.startswith("/"):
        raise ValueError(f"JSON pointer must start with '/': {pointer}")
    parts = pointer.lstrip("/").split("/")
    cur = doc
    for raw in parts:
        part = raw.replace("~1", "/").replace("~0", "~")
        if isinstance(cur, list):
            try:
                idx = int(part)
            except ValueError as e:
                raise KeyError(f"Expected list index at '{part}' in pointer {pointer}") from e
            try:
                cur = cur[idx]
            except IndexError as e:
                raise KeyError(f"Index out of range at '{part}' in pointer {pointer}") from e
        elif isinstance(cur, dict):
            if part not in cur:
                raise KeyError(f"Key '{part}' not found in pointer {pointer}")
            cur = cur[part]
        else:
            raise KeyError(f"Cannot traverse into non-container at '{part}' in pointer {pointer}")
    return cur

def coerce_number(x: Any) -> float:
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        return float(x.strip())
    raise TypeError(f"Not a number: {type(x)}")

def safe_float_eq(a: Any, b: Any, eps: float = 1e-12) -> bool:
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(float(a) - float(b)) <= eps
    return a == b
