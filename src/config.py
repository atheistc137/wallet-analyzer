"""
Central configuration for chains, categories, spam filters, and env.
"""

import os
from dataclasses import dataclass
from typing import Dict, List, Optional

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency fallback
    def load_dotenv(*_args, **_kwargs):  # type: ignore
        return False

load_dotenv()

ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY", "").strip()
if not ALCHEMY_API_KEY:
    raise RuntimeError("Missing ALCHEMY_API_KEY. Set it in environment or a .env file.")

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "").strip()
if not COINGECKO_API_KEY:
    raise RuntimeError("Missing COINGECKO_API_KEY. Set it in environment or a .env file.")

# Alchemy mainnet RPC base URLs
ALCHEMY_RPC_BY_CHAIN: Dict[str, str] = {
    "ethereum": f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}",
    "base": f"https://base-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}",
    "arbitrum": f"https://arb-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}",
}

# We ONLY want "external" (native) and "erc20" transfers (no NFT/internal)
FETCH_CATEGORIES: List[str] = ["external", "erc20"]

# Transfers API paging and behavior
MAX_COUNT_HEX = hex(200)  # 0xc8
DEFAULT_ORDER = "asc"
FROM_BLOCK = "0x0"

# Networking
REQUEST_TIMEOUT_SECONDS = 30
RETRY_STATUS = {429, 500, 502, 503, 504}

# SQLite
DEFAULT_DB_PATH = os.getenv("TXN_DB_PATH", "transactions.sqlite3")

# -------------------------------
# Spam filter configuration
# -------------------------------

# 1) Asset allowlist pattern: conservative symbol characters; reject spaces/emoji/etc.
ASSET_ALLOWED_MAX_LEN = 16
ASSET_ALLOWED_PATTERN = r"^[A-Za-z0-9+_.-]{1," + str(ASSET_ALLOWED_MAX_LEN) + r"}$"

# 2) Asset strings containing URLs / domains / obvious promos (case-insensitive)
SPAM_TLDS: List[str] = [
    # common + spammy TLDs (extend if needed):
    "com","xyz","top","site","info","io","co","org","net","app","club","vip","quest","art",
    "shop","trade","fun","pro","lol","best","guru","work","ltd","loan","click","gift","today",
    "party","online","cloud","web","in","me","biz","store","live","space","social","link",
    "zip","mov","page"
]
SPAM_KEYWORDS: List[str] = [
    "http", "https", "www.", "t.me", "telegram", "discord", "twitter", "x.com",
    "airdrop", "claim", "free", "bonus", "gift"
]

# 3) Zero-value filter
EXCLUDE_ZERO_VALUE = True  # safest way to cut noise

# 4) Optional dust thresholds for native/external transfers (in wei). Use None to disable.
# Example (commented out): 0.00001 ETH = 1e14 wei
DUST_WEI_THRESHOLDS: Dict[str, Optional[int]] = {
    "ethereum": None,   # e.g., set to 100000000000000 if you want to prune native dust on ETH
    "base": None,
    "arbitrum": None,
}

@dataclass(frozen=True)
class Chains:
    ETHEREUM: str = "ethereum"
    BASE: str = "base"
    ARBITRUM: str = "arbitrum"

CHAINS = [Chains.ETHEREUM, Chains.BASE, Chains.ARBITRUM]

COINGECKO_PLATFORMS_BY_CHAIN: Dict[str, str] = {
    Chains.ETHEREUM: "ethereum",
    Chains.BASE: "base",
    Chains.ARBITRUM: "arbitrum-one",
}
