"""
Tiny SQLite helper: initialize schema and upsert rows.
"""

import json
import sqlite3
from typing import Dict, Any, Iterable

from config import DEFAULT_DB_PATH

_SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chain TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    unique_id TEXT NOT NULL DEFAULT '',    -- non-null so UNIQUE can use the column directly
    block_number INTEGER,
    block_timestamp TEXT,
    from_address TEXT,
    to_address TEXT,
    asset TEXT,
    value REAL,                            -- normalized 'value' from Alchemy
    raw_value_wei TEXT,                    -- hex from rawContract.value when present
    category TEXT,
    contract_address TEXT,
    erc721_token_id TEXT,
    erc1155_metadata TEXT,
    raw_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(chain, tx_hash, unique_id)
);

-- optional denylist tables for pruning
CREATE TABLE IF NOT EXISTS denylist_addresses (
    address TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS denylist_contracts (
    address TEXT PRIMARY KEY
);
"""

_INSERT = """
INSERT OR IGNORE INTO transactions
(chain, tx_hash, unique_id, block_number, block_timestamp, from_address, to_address,
 asset, value, raw_value_wei, category, contract_address, erc721_token_id, erc1155_metadata, raw_json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def _connect(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    with _connect(db_path) as conn:
        conn.executescript(_SCHEMA)


def upsert_events(chain: str, events: Iterable[Dict[str, Any]], db_path: str = DEFAULT_DB_PATH) -> int:
    """
    Insert many events; returns count of attempted inserts.
    Uses INSERT OR IGNORE to be idempotent across re-runs.
    """
    events = list(events)
    if not events:
        return 0

    rows = []
    for e in events:
        block_num_hex = e.get("blockNum")
        block_number = int(block_num_hex, 16) if block_num_hex else None
        raw_contract = e.get("rawContract") or {}
        rows.append((
            chain,
            e.get("hash"),
            e.get("uniqueId") or "",  # ensure non-null for UNIQUE
            block_number,
            e.get("metadata", {}).get("blockTimestamp") or e.get("blockTimestamp"),
            e.get("from"),
            e.get("to"),
            e.get("asset"),
            e.get("value"),
            raw_contract.get("value"),
            e.get("category"),
            raw_contract.get("address"),
            e.get("erc721TokenId"),
            json.dumps(e.get("erc1155Metadata")) if e.get("erc1155Metadata") else None,
            json.dumps(e, ensure_ascii=False),
        ))

    with _connect(db_path) as conn:
        conn.executemany(_INSERT, rows)
    return len(rows)
