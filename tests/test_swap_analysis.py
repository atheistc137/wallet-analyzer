from datetime import datetime, timezone
from decimal import Decimal
import os
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

os.environ.setdefault("ALCHEMY_API_KEY", "test-key")
os.environ.setdefault("COINGECKO_API_KEY", "test-key")

from db import init_db
from swap_analysis import SwapAnalyzer


class FakeCoinGeckoClient:
    def __init__(self) -> None:
        self.current_price = 30000.0
        self.contract_map = {
            ("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): "usd-coin",
        }
        self.prices = {
            ("usd-coin", "2024-01-01"): 1.0,
            ("bitcoin", "2024-01-01"): 25000.0,
            ("usd-coin", "2024-01-02"): 1.0,
            ("ethereum", "2024-01-02"): 2000.0,
            ("bitcoin", "2024-01-02"): 26000.0,
        }

    def get_current_price(self, coin_id: str) -> float:
        assert coin_id == "bitcoin"
        return self.current_price

    def get_coin_id_by_contract(self, platform_id: str, contract_address: str) -> str:
        key = (platform_id, contract_address.lower())
        if key not in self.contract_map:
            raise AssertionError(f"Unexpected contract lookup: {key}")
        return self.contract_map[key]

    def get_price_at_timestamp(self, coin_id: str, timestamp: datetime) -> float:
        key = (coin_id, timestamp.astimezone(timezone.utc).strftime("%Y-%m-%d"))
        if key not in self.prices:
            raise AssertionError(f"Unexpected price lookup: {key}")
        return self.prices[key]


@pytest.fixture()
def temp_db(tmp_path):
    db_path = tmp_path / "transactions.sqlite3"
    init_db(str(db_path))
    return str(db_path)


def insert_transaction(conn, **kwargs):
    columns = (
        "chain",
        "tx_hash",
        "unique_id",
        "block_number",
        "block_timestamp",
        "from_address",
        "to_address",
        "asset",
        "value",
        "raw_value_wei",
        "category",
        "contract_address",
        "erc721_token_id",
        "erc1155_metadata",
        "raw_json",
    )
    values = tuple(kwargs.get(col) for col in columns)
    conn.execute(
        f"INSERT INTO transactions ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
        values,
    )


def test_swap_analysis_marks_and_values(temp_db):
    wallet = "0xWallet"
    with sqlite3.connect(temp_db) as conn:
        insert_transaction(
            conn,
            chain="ethereum",
            tx_hash="0xswap1",
            unique_id="out-1",
            block_number=1,
            block_timestamp="2024-01-01T00:00:00.000Z",
            from_address=wallet,
            to_address="0xDex",
            asset="USDC",
            value=1000.0,
            raw_value_wei=None,
            category="erc20",
            contract_address="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            erc721_token_id=None,
            erc1155_metadata=None,
            raw_json="{}",
        )
        insert_transaction(
            conn,
            chain="ethereum",
            tx_hash="0xswap1",
            unique_id="in-1",
            block_number=1,
            block_timestamp="2024-01-01T00:00:00.000Z",
            from_address="0xDex",
            to_address=wallet,
            asset="TOKEN",
            value=500.0,
            raw_value_wei=None,
            category="erc20",
            contract_address="0xToken",
            erc721_token_id=None,
            erc1155_metadata=None,
            raw_json="{}",
        )

        insert_transaction(
            conn,
            chain="ethereum",
            tx_hash="0xswap2",
            unique_id="out-eth",
            block_number=2,
            block_timestamp="2024-01-02T00:00:00.000Z",
            from_address=wallet,
            to_address="0xDex2",
            asset="ETH",
            value=0.1,
            raw_value_wei=None,
            category="external",
            contract_address=None,
            erc721_token_id=None,
            erc1155_metadata=None,
            raw_json="{}",
        )
        insert_transaction(
            conn,
            chain="ethereum",
            tx_hash="0xswap2",
            unique_id="out-usdc",
            block_number=2,
            block_timestamp="2024-01-02T00:00:00.000Z",
            from_address=wallet,
            to_address="0xDex2",
            asset="USDC",
            value=100.0,
            raw_value_wei=None,
            category="erc20",
            contract_address="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            erc721_token_id=None,
            erc1155_metadata=None,
            raw_json="{}",
        )
        insert_transaction(
            conn,
            chain="ethereum",
            tx_hash="0xswap2",
            unique_id="in-2",
            block_number=2,
            block_timestamp="2024-01-02T00:00:00.000Z",
            from_address="0xDex2",
            to_address=wallet,
            asset="TOKEN2",
            value=750.0,
            raw_value_wei=None,
            category="erc20",
            contract_address="0xToken2",
            erc721_token_id=None,
            erc1155_metadata=None,
            raw_json="{}",
        )

        # Non-swap inbound transfer
        insert_transaction(
            conn,
            chain="ethereum",
            tx_hash="0xnoswap",
            unique_id="gift",
            block_number=3,
            block_timestamp="2024-01-03T00:00:00.000Z",
            from_address="0xFriend",
            to_address=wallet,
            asset="ETH",
            value=0.5,
            raw_value_wei=None,
            category="external",
            contract_address=None,
            erc721_token_id=None,
            erc1155_metadata=None,
            raw_json="{}",
        )
        conn.commit()

    analyzer = SwapAnalyzer(wallet, db_path=temp_db, cg_client=FakeCoinGeckoClient())
    summary = analyzer.analyze()

    assert summary["swaps_detected"] == Decimal(2)
    assert float(summary["btc_amount"]) == pytest.approx(0.0515384615)
    assert summary["btc_price_current"] == Decimal("30000")
    assert float(summary["btc_value_usd"]) == pytest.approx(1546.153845)

    with sqlite3.connect(temp_db) as conn:
        conn.row_factory = sqlite3.Row
        first_incoming = conn.execute(
            "SELECT * FROM transactions WHERE tx_hash = ? AND unique_id = ?",
            ("0xswap1", "in-1"),
        ).fetchone()
        assert first_incoming["is_swap"] == 1
        assert first_incoming["swap_spent_asset"] == "USDC"
        assert first_incoming["swap_spent_amount"] == pytest.approx(1000.0)
        assert first_incoming["swap_spent_usd"] == pytest.approx(1000.0)
        assert first_incoming["swap_btc_price_at_purchase"] == pytest.approx(25000.0)
        assert first_incoming["swap_btc_amount"] == pytest.approx(0.04)
        assert first_incoming["swap_btc_price_current"] == pytest.approx(30000.0)
        assert first_incoming["swap_btc_value_usd"] == pytest.approx(1200.0)

        second_incoming = conn.execute(
            "SELECT * FROM transactions WHERE tx_hash = ? AND unique_id = ?",
            ("0xswap2", "in-2"),
        ).fetchone()
        assert second_incoming["is_swap"] == 1
        assert second_incoming["swap_spent_asset"] == "MULTI"
        assert second_incoming["swap_spent_amount"] is None
        assert second_incoming["swap_spent_usd"] == pytest.approx(300.0)
        assert second_incoming["swap_btc_amount"] == pytest.approx(0.0115384615)
        assert second_incoming["swap_btc_value_usd"] == pytest.approx(346.153845)

        outgoing = conn.execute(
            "SELECT * FROM transactions WHERE tx_hash = ? AND unique_id = ?",
            ("0xswap1", "out-1"),
        ).fetchone()
        assert outgoing["is_swap"] == 1
        assert outgoing["swap_spent_asset"] is None

        noswap = conn.execute(
            "SELECT * FROM transactions WHERE tx_hash = ?",
            ("0xnoswap",),
        ).fetchone()
        assert noswap["is_swap"] == 0
