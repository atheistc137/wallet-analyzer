"""
CLI entrypoint:
- validates an EVM address
- queries Alchemy Transfers API for ETH/Base/Arbitrum
- ONLY stores external/native + ERC20 transfers (filters out NFT & internal)
- applies spam filtering before insert (URLs, weird symbols, zero value, optional dust)
- paginates both inbound and outbound
- stores results in SQLite
"""

import re
import sys
import time
from typing import Dict, Iterable, List, Optional

import requests
from requests.adapters import HTTPAdapter, Retry

from config import (
    ALCHEMY_RPC_BY_CHAIN,
    FETCH_CATEGORIES,          # ['external', 'erc20']
    CHAINS,
    DEFAULT_ORDER,
    FROM_BLOCK,
    MAX_COUNT_HEX,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_STATUS,
)
from db import init_db, upsert_events
from spam_filters import is_spam_event

ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
ALLOWED_CATEGORIES = {"external", "erc20"}  # safety net filter


def _make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.8,
        status_forcelist=list(RETRY_STATUS),
        allowed_methods=["POST"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"Content-Type": "application/json"})
    return s


def _params_template(address: str, direction: str, categories: List[str]) -> Dict:
    """
    Build the params object for alchemy_getAssetTransfers.
    direction: 'to' (incoming) or 'from' (outgoing)
    """
    p = {
        "fromBlock": FROM_BLOCK,
        "toBlock": "latest",
        "category": categories,     # asks Alchemy only for 'external' & 'erc20'
        "order": DEFAULT_ORDER,
        "withMetadata": True,
        "excludeZeroValue": True,   # tighter: cuts a lot of noise upfront
        "maxCount": MAX_COUNT_HEX,
    }
    if direction == "to":
        p["toAddress"] = address
    else:
        p["fromAddress"] = address
    return p


def _fetch_all_transfers_for_direction(
    session: requests.Session,
    rpc_url: str,
    params_obj: Dict,
) -> List[Dict]:
    """
    Loop `pageKey` to fetch all pages for one direction on one chain.
    """
    out: List[Dict] = []
    page_key: Optional[str] = None

    while True:
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "alchemy_getAssetTransfers",
            "params": [params_obj | ({"pageKey": page_key} if page_key else {})],
        }
        resp = session.post(rpc_url, json=body, timeout=REQUEST_TIMEOUT_SECONDS)
        if resp.status_code == 429:
            time.sleep(1.5)
            continue
        if resp.status_code >= 400:
            raise RuntimeError(f"Alchemy error {resp.status_code}: {resp.text[:300]}")

        data = resp.json()
        result = (data or {}).get("result") or {}
        transfers = result.get("transfers", [])
        out.extend(transfers)
        page_key = result.get("pageKey")
        if not page_key:
            break
    return out


def _unique_by_tx_and_unique_id(events: Iterable[Dict]) -> List[Dict]:
    seen = set()
    deduped = []
    for e in events:
        key = (e.get("hash"), e.get("uniqueId") or "")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(e)
    return deduped


def _filter_allowed_categories(events: Iterable[Dict]) -> List[Dict]:
    out = []
    for e in events:
        cat = (e.get("category") or "").lower()
        if cat in ALLOWED_CATEGORIES:
            out.append(e)
    return out


def _apply_spam_filters(events: Iterable[Dict], chain: str) -> List[Dict]:
    return [e for e in events if not is_spam_event(e, chain)]


def fetch_all_for_chain(address: str, chain: str) -> List[Dict]:
    """
    Fetch all transfers (in + out) for one chain; return normalized, de-spammed list.
    """
    rpc_url = ALCHEMY_RPC_BY_CHAIN[chain]
    session = _make_session()

    incoming = _fetch_all_transfers_for_direction(session, rpc_url, _params_template(address, "to", FETCH_CATEGORIES))
    outgoing = _fetch_all_transfers_for_direction(session, rpc_url, _params_template(address, "from", FETCH_CATEGORIES))

    merged = _unique_by_tx_and_unique_id([*incoming, *outgoing])
    merged = _filter_allowed_categories(merged)
    merged = _apply_spam_filters(merged, chain)
    return merged


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python fetch_and_store.py <EVM_ADDRESS>")
        return 2

    address = sys.argv[1].strip()
    if not ADDRESS_RE.match(address):
        print("Error: please provide a valid 0x-prefixed 40-hex EVM address.")
        return 2

    init_db()

    total = 0
    for chain in CHAINS:
        try:
            print(f"[{chain}] fetching transfers (external + erc20 only, spam filtered)...")
            events = fetch_all_for_chain(address, chain)
            n = upsert_events(chain, events)
            total += n
            print(f"[{chain}] fetched {len(events)} events, inserted {n}.")
        except Exception as e:
            print(f"[{chain}] ERROR: {e}")

    print(f"Done. Attempted to insert {total} rows. See transactions.sqlite3")
    return 0


if __name__ == "__main__":
    sys.exit(main())
