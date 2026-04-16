from __future__ import annotations

import re
from typing import Any, Optional
from urllib.parse import urlparse

SOFT404_PATTERNS = [
    r"\bpage not found\b",
    r"\b404\b",
    r"\bthe page you requested\b",
    r"\bdoes not exist\b",
    r"\bnot be found\b",
]
SOFT404_RE = re.compile("|".join(SOFT404_PATTERNS), re.IGNORECASE)


def is_url(value: Any) -> bool:
    return isinstance(value, str) and value.strip().lower().startswith(("http://", "https://"))


def detect_soft404(text: str) -> bool:
    if not isinstance(text, str) or not text:
        return False
    return bool(SOFT404_RE.search(text))


def is_html_content_type(content_type: str) -> bool:
    ct = (content_type or "").lower()
    return "text/html" in ct or "application/xhtml" in ct or ct == ""


def normalize_homepage(url: str) -> Optional[str]:
    if not isinstance(url, str) or not url.strip():
        return None
    parsed = urlparse(url.strip())
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}/"
