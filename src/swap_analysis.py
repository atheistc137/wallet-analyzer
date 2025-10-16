"""Detect swap transactions and enrich them with USD/BTC valuations."""

from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from config import (
    COINGECKO_API_KEY,
    COINGECKO_PLATFORMS_BY_CHAIN,
    DEFAULT_DB_PATH,
)
from coingecko import CoinGeckoClient, CoinGeckoError
from db import init_db


@dataclass
class SpentComponent:
    asset: str
    amount: Decimal
    usd_price: Decimal

    @property
    def usd_value(self) -> Decimal:
        return self.amount * self.usd_price


SYMBOL_TO_COINGECKO_ID: Dict[str, str] = {
    "ETH": "ethereum",
    "WETH": "weth",
    "USDC": "usd-coin",
    "USDT": "tether",
    "DAI": "dai",
    "WBTC": "wrapped-bitcoin",
    "BTC": "bitcoin",
}


def _parse_timestamp(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts).astimezone(timezone.utc)
    except ValueError:
        return None


class SwapAnalyzer:
    def __init__(
        self,
        wallet_address: str,
        db_path: str = DEFAULT_DB_PATH,
        cg_client: Optional[CoinGeckoClient] = None,
    ) -> None:
        if not wallet_address:
            raise ValueError("wallet_address is required")
        self.wallet_address = wallet_address.lower()
        self.db_path = db_path
        self.cg_client = cg_client or CoinGeckoClient(COINGECKO_API_KEY)

    def analyze(self) -> Dict[str, Decimal]:
        init_db(self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            grouped = self._group_transactions(conn)
            if not grouped:
                return {
                    "swaps_detected": Decimal(0),
                    "btc_amount": Decimal(0),
                    "btc_value_usd": Decimal(0),
                    "btc_price_current": Decimal(0),
                }

            btc_price_current = Decimal(
                str(self.cg_client.get_current_price("bitcoin"))
            )

            total_btc_amount = Decimal(0)
            swaps_detected = 0

            for tx_hash, rows in grouped.items():
                outgoing = [r for r in rows if self._is_outgoing(r)]
                incoming = [r for r in rows if self._is_incoming(r)]

                if not outgoing or not incoming:
                    continue

                timestamp = self._tx_timestamp(rows) or datetime.utcnow().replace(
                    tzinfo=timezone.utc
                )
                try:
                    spent_components = self._build_spent_components(outgoing, timestamp)
                except CoinGeckoError as exc:
                    print(f"[swap-analysis] {tx_hash} skipped: {exc}")
                    spent_components = []

                if spent_components:
                    try:
                        btc_price_at_purchase = Decimal(
                            str(
                                self.cg_client.get_price_at_timestamp(
                                    "bitcoin", timestamp
                                )
                            )
                        )
                    except CoinGeckoError as exc:
                        print(f"[swap-analysis] {tx_hash} BTC price lookup failed: {exc}")
                        btc_price_at_purchase = None

                    total_usd_spent = sum((c.usd_value for c in spent_components), Decimal(0))
                    btc_amount = (
                        (total_usd_spent / btc_price_at_purchase)
                        if btc_price_at_purchase and btc_price_at_purchase > 0
                        else None
                    )
                else:
                    btc_price_at_purchase = None
                    total_usd_spent = None
                    btc_amount = None

                swaps_detected += 1

                self._mark_outgoing(conn, outgoing)
                self._enrich_incoming(
                    conn,
                    incoming,
                    spent_components,
                    total_usd_spent,
                    btc_price_at_purchase,
                    btc_amount,
                    btc_price_current,
                )

                if btc_amount:
                    total_btc_amount += btc_amount

            total_btc_value = total_btc_amount * btc_price_current

            return {
                "swaps_detected": Decimal(swaps_detected),
                "btc_amount": total_btc_amount,
                "btc_value_usd": total_btc_value,
                "btc_price_current": btc_price_current,
            }

    def _group_transactions(self, conn: sqlite3.Connection) -> Dict[str, List[sqlite3.Row]]:
        cur = conn.execute(
            """
            SELECT id, chain, tx_hash, block_timestamp, from_address, to_address,
                   asset, value, category, contract_address
            FROM transactions
            ORDER BY block_timestamp
            """
        )
        grouped: Dict[str, List[sqlite3.Row]] = defaultdict(list)
        for row in cur.fetchall():
            grouped[row["tx_hash"]].append(row)
        return grouped

    def _is_incoming(self, row: sqlite3.Row) -> bool:
        to_addr = (row["to_address"] or "").lower()
        return to_addr == self.wallet_address

    def _is_outgoing(self, row: sqlite3.Row) -> bool:
        from_addr = (row["from_address"] or "").lower()
        return from_addr == self.wallet_address

    def _tx_timestamp(self, rows: Sequence[sqlite3.Row]) -> Optional[datetime]:
        for row in rows:
            ts = _parse_timestamp(row["block_timestamp"])
            if ts:
                return ts
        return None

    def _resolve_coin_id(
        self,
        chain: str,
        asset: Optional[str],
        contract_address: Optional[str],
    ) -> str:
        symbol = (asset or "").upper()
        if contract_address:
            platform = COINGECKO_PLATFORMS_BY_CHAIN.get(chain.lower())
            if not platform:
                raise CoinGeckoError(f"Unsupported chain for CoinGecko mapping: {chain}")
            return self.cg_client.get_coin_id_by_contract(platform, contract_address)
        if symbol in SYMBOL_TO_COINGECKO_ID:
            return SYMBOL_TO_COINGECKO_ID[symbol]
        raise CoinGeckoError(
            f"Unable to resolve CoinGecko id for asset {asset or 'unknown'}"
        )

    def _build_spent_components(
        self, outgoing: Iterable[sqlite3.Row], timestamp: datetime
    ) -> List[SpentComponent]:
        grouped: Dict[Tuple[str, Optional[str]], Decimal] = defaultdict(lambda: Decimal(0))
        chain_by_key: Dict[Tuple[str, Optional[str]], str] = {}

        for row in outgoing:
            value = row["value"]
            if value is None:
                continue
            amount = Decimal(str(value))
            if amount <= 0:
                continue
            key = (row["asset"], row["contract_address"])
            grouped[key] += amount
            chain_by_key[key] = row["chain"]

        components: List[SpentComponent] = []

        for (asset, contract_address), amount in grouped.items():
            chain = chain_by_key[(asset, contract_address)]
            coin_id = self._resolve_coin_id(chain, asset, contract_address)
            usd_price = Decimal(
                str(self.cg_client.get_price_at_timestamp(coin_id, timestamp))
            )
            components.append(SpentComponent(asset=asset or "", amount=amount, usd_price=usd_price))

        return components

    def _mark_outgoing(self, conn: sqlite3.Connection, outgoing: Iterable[sqlite3.Row]) -> None:
        ids = [row["id"] for row in outgoing]
        if not ids:
            return
        conn.executemany(
            "UPDATE transactions SET is_swap = 1 WHERE id = ?",
            [(row_id,) for row_id in ids],
        )

    def _enrich_incoming(
        self,
        conn: sqlite3.Connection,
        incoming: Iterable[sqlite3.Row],
        spent_components: Sequence[SpentComponent],
        total_usd_spent: Optional[Decimal],
        btc_price_at_purchase: Optional[Decimal],
        btc_amount: Optional[Decimal],
        btc_price_current: Decimal,
    ) -> None:
        spent_asset = None
        spent_amount = None

        if len(spent_components) == 1:
            spent_asset = spent_components[0].asset
            spent_amount = float(spent_components[0].amount)
        elif spent_components:
            spent_asset = "MULTI"

        usd_spent_float = float(total_usd_spent) if total_usd_spent is not None else None
        btc_price_purchase_float = (
            float(btc_price_at_purchase) if btc_price_at_purchase is not None else None
        )
        btc_amount_float = float(btc_amount) if btc_amount is not None else None
        btc_price_current_float = float(btc_price_current) if btc_amount is not None else None
        btc_value_float = (
            float(btc_amount * btc_price_current)
            if btc_amount is not None
            else None
        )

        params = []
        for row in incoming:
            params.append(
                (
                    spent_asset,
                    spent_amount,
                    usd_spent_float,
                    btc_price_purchase_float,
                    btc_amount_float,
                    btc_price_current_float,
                    btc_value_float,
                    row["id"],
                )
            )

        if not params:
            return

        conn.executemany(
            """
            UPDATE transactions
            SET is_swap = 1,
                swap_spent_asset = ?,
                swap_spent_amount = ?,
                swap_spent_usd = ?,
                swap_btc_price_at_purchase = ?,
                swap_btc_amount = ?,
                swap_btc_price_current = ?,
                swap_btc_value_usd = ?
            WHERE id = ?
            """,
            params,
        )


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Identify swap transactions and enrich them with USD/BTC metrics.",
    )
    parser.add_argument("wallet", help="Wallet address used when fetching transactions.")
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help="Path to the SQLite database (defaults to transactions.sqlite3).",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_cli().parse_args(argv)
    analyzer = SwapAnalyzer(args.wallet, db_path=args.db_path)
    summary = analyzer.analyze()
    print(
        "Detected {swaps_detected} swaps. BTC amount: {btc_amount:.8f}, current value: ${btc_value_usd:.2f} (BTC price ${btc_price_current:.2f}).".format(
            swaps_detected=int(summary["swaps_detected"]),
            btc_amount=float(summary["btc_amount"]),
            btc_value_usd=float(summary["btc_value_usd"]),
            btc_price_current=float(summary["btc_price_current"]),
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI glue
    raise SystemExit(main())
