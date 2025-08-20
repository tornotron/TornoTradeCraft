from typing import List, Iterable, Optional, Dict
from datetime import datetime
import pandas as pd
from tornotradingcraft.providers import DataProvider, SymbolInfo, Quote, OHLCV
from tornotradingcraft.utils import update_asset_file
from tornotradingcraft.utils.cache_crud import load_df_from_cache, save_df_to_cache
from tornotradingcraft.config import ASSETS_PATH

class YahooFinanceProvider(DataProvider):
    """Concrete implementation of DataProvider for Yahoo Finance."""

    @property
    def name(self) -> str:
        return "YahooFinance"
    
    def update_supported_symbols(self, filepath: str) -> None:
        """Update the cached list of supported symbols from Yahoo Finance.

        This method is optional; if not implemented, `get_supported_symbols`
        will return the cached list.
        """
        update_asset_file(file_path=filepath, parquet_name="YahooFinance")

    
    def get_supported_symbols(self) -> List[SymbolInfo]:

        """Fetch all supported symbols from Yahoo Finance.

        This method first attempts to load a cached pandas DataFrame using the
        package cache helpers. If the cache does not contain the DataFrame,
        it falls back to reading the provider's parquet file from the package
        `assets` directory (e.g. `YahooFinance.parquet`) and then saves the
        loaded DataFrame into the cache for future use. Each row of the
        DataFrame is mapped to a `SymbolInfo` instance.

        Returns:
            A list of `SymbolInfo` objects representing supported symbols.
        """

        assets_dir = ASSETS_PATH
        parquet_path = assets_dir / f"{self.name}.parquet"

        cache_key = f"{self.name}.parquet"

        # Try loading DataFrame from cache first
        df = load_df_from_cache(cache_key)

        if df is None:
            # Cache miss -> load from asset file on disk
            if not parquet_path.exists():
                raise FileNotFoundError(f"Assets file not found: {parquet_path}")

            df = pd.read_parquet(parquet_path)

            # Save to cache for future reads (best-effort)
            try:
                save_df_to_cache(cache_key, df)
            except Exception:
                # Swallow caching errors; reading from assets succeeded so continue
                pass

        # At minimum we require the 'ticker' column; other columns are optional
        # and will be set to None if missing.
        required_cols = {"Ticker"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns in {parquet_path}: {missing}")

        symbols: List[SymbolInfo] = []
        for rec in df.to_dict(orient="records"):
            ticker = rec.get("Ticker")
            if ticker is None:
                # skip rows without a ticker
                continue

            # Map available columns to SymbolInfo fields. 
            si = SymbolInfo(
                symbol=str(ticker),
                name=rec.get("Name", None),
                exchange=rec.get("Exchange", None),
                category_name=rec.get("Category Name", None),
                country=rec.get("Country", None),
                currency=rec.get("Currency", None)
            )
            symbols.append(si)

        return symbols

    def get_symbol_info(self, symbol: str) -> SymbolInfo:
        """Fetch metadata for a given symbol."""
        # Implementation to fetch symbol info from Yahoo Finance API
        raise NotImplementedError("get_symbol_info is not implemented for YahooFinanceProvider")

    def get_quote(self, symbol: str) -> Quote:
        """Fetch the latest quote for a given symbol."""
        # Implementation to fetch latest quote from Yahoo Finance API
        raise NotImplementedError("get_quote is not implemented for YahooFinanceProvider")

    def fetch_bulk_quotes(self, symbols: Iterable[str]) -> Dict[str, Quote]:
        """Return latest quotes for multiple symbols as a dict keyed by symbol."""
        raise NotImplementedError

    def get_historical_prices(
        self,
        symbol: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        interval: str = "1d",
    ) -> List[OHLCV]:
        """Return historical OHLCV bars for the symbol between start and end.

        Interval is provider-specific (e.g. "1d", "1h", "1m").
        """
        raise NotImplementedError

    def get_historical_data(self, symbol: str, start: datetime, end: datetime) -> List[OHLCV]:
        """Fetch historical OHLCV data for a given symbol."""
        # Implementation to fetch historical data from Yahoo Finance API
        raise NotImplementedError("get_historical_data is not implemented for YahooFinanceProvider")

    def search_symbols(self, query: str) -> Iterable[SymbolInfo]:
        """Search for symbols matching a query."""
        # Implementation to search symbols using Yahoo Finance API
        raise NotImplementedError("search_symbols is not implemented for YahooFinanceProvider")