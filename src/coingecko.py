"""Simple CoinGecko API client with light caching."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

try:  # pragma: no cover - import guard for optional dependency
    import requests
except ImportError as exc:  # pragma: no cover - optional dependency fallback
    requests = None  # type: ignore[assignment]
    _REQUESTS_IMPORT_ERROR = exc
else:  # pragma: no cover - executed when requests available
    _REQUESTS_IMPORT_ERROR = None


class CoinGeckoError(RuntimeError):
    """Raised when the CoinGecko API returns an error or malformed payload."""


@dataclass
class CoinGeckoClient:
    api_key: str
    base_url: str = "https://api.coingecko.com/api/v3"
    timeout: int = 30
    session: Optional[Any] = None
    _price_cache: Dict[Tuple[str, int], float] = field(default_factory=dict, init=False)
    _contract_cache: Dict[Tuple[str, str], str] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        if self.session is None:
            if requests is None:  # pragma: no cover - handled in tests via fake client
                raise ImportError(
                    "The 'requests' package is required to use CoinGeckoClient"
                ) from _REQUESTS_IMPORT_ERROR
            self.session = requests.Session()

    def _request(self, method: str, path: str, params: Optional[Dict] = None) -> Dict:
        url = f"{self.base_url}{path}"
        params = params or {}
        headers = {"accept": "application/json"}
        if self.api_key:
            headers["x-cg-pro-api-key"] = self.api_key
            params.setdefault("x_cg_pro_api_key", self.api_key)
        assert self.session is not None  # for type checkers
        response = self.session.request(method, url, params=params, timeout=self.timeout)
        if response.status_code >= 400:
            raise CoinGeckoError(
                f"CoinGecko API error {response.status_code}: {response.text[:300]}"
            )
        try:
            return response.json()
        except ValueError as exc:  # pragma: no cover - defensive, hard to trigger in tests
            raise CoinGeckoError("Failed to decode CoinGecko response as JSON") from exc

    def get_coin_id_by_contract(self, platform_id: str, contract_address: str) -> str:
        """Resolve a coin id from an on-chain contract address."""

        key = (platform_id, contract_address.lower())
        if key in self._contract_cache:
            return self._contract_cache[key]

        data = self._request("GET", f"/coins/{platform_id}/contract/{contract_address}")
        coin_id = data.get("id") if isinstance(data, dict) else None
        if not coin_id:
            raise CoinGeckoError(
                f"CoinGecko could not resolve coin id for {platform_id}:{contract_address}"
            )
        self._contract_cache[key] = coin_id
        return coin_id

    def get_price_at_timestamp(self, coin_id: str, timestamp: datetime) -> float:
        """Return the USD price for a token closest to the given timestamp."""

        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        else:
            timestamp = timestamp.astimezone(timezone.utc)

        unix_ts = int(timestamp.timestamp())
        cache_key = (coin_id, unix_ts)
        if cache_key in self._price_cache:
            return self._price_cache[cache_key]

        params = {
            "vs_currency": "usd",
            "from": unix_ts - 900,
            "to": unix_ts + 900,
        }
        data = self._request("GET", f"/coins/{coin_id}/market_chart/range", params=params)
        prices = data.get("prices") if isinstance(data, dict) else None
        if not prices:
            raise CoinGeckoError(
                f"CoinGecko price history missing for {coin_id} at {unix_ts}"
            )

        closest_price = min(
            prices,
            key=lambda pair: abs(int(pair[0] / 1000) - unix_ts),
        )
        price = float(closest_price[1])
        self._price_cache[cache_key] = price
        return price

    def get_current_price(self, coin_id: str) -> float:
        data = self._request(
            "GET",
            "/simple/price",
            params={"ids": coin_id, "vs_currencies": "usd"},
        )
        price = data.get(coin_id, {}).get("usd") if isinstance(data, dict) else None
        if price is None:
            raise CoinGeckoError(f"CoinGecko current price missing for {coin_id}")
        return float(price)
