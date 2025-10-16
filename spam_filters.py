"""
Shared spam filtering logic (used by fetch and the pruning utility).
"""
import re
from typing import Dict, Any

from config import (
    ASSET_ALLOWED_PATTERN,
    SPAM_TLDS,
    SPAM_KEYWORDS,
    EXCLUDE_ZERO_VALUE,
    DUST_WEI_THRESHOLDS,
)

_ASSET_ALLOWED_RE = re.compile(ASSET_ALLOWED_PATTERN)
# Build a permissive "looks like a URL/domain" regex with our TLDs
_TLD_ALT = "|".join([re.escape(t) for t in SPAM_TLDS])
_URL_LIKE_RE = re.compile(
    rf"(https?://|www\.)|([a-z0-9-]{{1,63}}\.(?:{_TLD_ALT})(?:[/?#].*)?)",
    flags=re.IGNORECASE,
)

def _safe_lower(s):
    return (s or "").lower()

def _value_is_zero(e: Dict[str, Any]) -> bool:
    v = e.get("value")
    try:
        # Alchemy 'value' is typically a float or str convertible to float
        return float(v) == 0.0
    except Exception:
        return False

def _under_dust(e: Dict[str, Any], chain: str) -> bool:
    threshold = DUST_WEI_THRESHOLDS.get(chain)
    if not threshold:
        return False
    if (e.get("category") or "").lower() != "external":
        return False
    raw = ((e.get("rawContract") or {}).get("value"))  # hex string like "0x..."
    if not raw:
        return False
    try:
        wei = int(raw, 16)
        return wei < threshold
    except Exception:
        return False

def _asset_has_url_or_keywords(asset: str) -> bool:
    a = _safe_lower(asset)
    if not a:
        return False
    if any(k in a for k in (kw.lower() for kw in SPAM_KEYWORDS)):
        return True
    return bool(_URL_LIKE_RE.search(a))

def _asset_is_weird(asset: str) -> bool:
    # Reject assets that don't pass the conservative allowlist
    if not asset:
        return False  # allow blank; most chains label native as "ETH" anyway
    return not bool(_ASSET_ALLOWED_RE.match(asset))

def is_spam_event(e: Dict[str, Any], chain: str) -> bool:
    """
    Returns True if the event looks like spam according to configured rules.
    """
    asset = e.get("asset") or ""
    if EXCLUDE_ZERO_VALUE and _value_is_zero(e):
        return True
    if _asset_has_url_or_keywords(asset):
        return True
    if _asset_is_weird(asset):
        return True
    if _under_dust(e, chain):
        return True
    return False
