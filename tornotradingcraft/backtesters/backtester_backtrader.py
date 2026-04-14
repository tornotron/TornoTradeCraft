import pandas as pd
import backtrader as bt

from pandas import DataFrame
from typing import Any, Optional

from tornotradingcraft.backtesters.backtester import Backtester
from tornotradingcraft.providers.providers import DataProvider
from tornotradingcraft.utils import safe_call


class BacktesterBacktrader(Backtester):
    """Backtester implementation using backtrader.

    This class provides helpers to convert Yahoo/Provider DataFrames into a
    format consumable by Backtrader and a thin `run` wrapper. The `run`
    implementation intentionally keeps dependencies optional so importing this
    module won't fail if `backtrader` is not installed; errors will be raised
    at runtime when `run` is invoked.
    """
    
    @property
    def name(self) -> str:
        return "backtrader"

    def get_bt_compatible_formatted_data(self, provider: DataProvider, data: DataFrame, to_btfeed: bool = False, **kwargs) -> DataFrame:
        """Normalize DataFrame according to provider metadata/type.

        Currently supports providers that identify themselves with a `name`
        attribute equal to 'YahooFinance' (case-insensitive) or provider
        classes named 'YahooFinanceProvider'. For unknown providers the method
        will attempt a best-effort normalization by lowercasing typical OHLCV
        column names.

        If `to_btfeed` is True this returns a backtrader feed object (line).
        Returns an object suitable to be consumed by `run` (for backtrader this
        will typically be a Cerebro-compatible OHLCV line object).
        Otherwise it returns the normalized DataFrame.
        """
        # Try provider.name if present
        provider_name = safe_call(getattr, provider, "name", None)

        if provider_name and isinstance(provider_name, str) and provider_name.lower() == "yahoofinance":
            return self._prepare_data_from_yahoo(data.copy(), to_btfeed=to_btfeed)
        # Generic fallback: delegate to helper which also handles DatetimeIndex -> 'time'
        return self._prepare_data_from_generic_provider(data, to_btfeed=to_btfeed, **kwargs)

    def _prepare_data_from_generic_provider(self, data: DataFrame, to_btfeed: bool = False, **kwargs) -> DataFrame:
        """Prepare a generic provider DataFrame.

        - Normalizes common OHLCV column names to lowercase.
        - If the DataFrame uses a DatetimeIndex and `to_bt` is False, the index
          will be reset and the resulting first column renamed to 'time'
          (matching YahooFinance provider convert behaviour).
        - If `to_bt` is True, attempts to convert the normalized DataFrame to
          a backtrader PandasData feed (optional dependency).
        """
        df = data.copy()

        # When returning a plain DataFrame for non-backtrader use, expose the
        # datetime as a column if present. For backtrader feeds, keep the
        # index as DatetimeIndex (bt expects an index).
        has_dt_index = isinstance(df.index, pd.DatetimeIndex)
        if has_dt_index and not to_btfeed:
            df = df.reset_index()
            # after reset_index, the datetime index becomes the first column
            df = df.rename(columns={df.columns[0]: "time"})

        # Map common OHLCV-like columns to lowercase
        col_map = {}
        for c in df.columns:
            lc = c.strip().lower()
            if lc in {"open", "high", "low", "close", "volume"}:
                col_map[c] = lc

        df = df.rename(columns=col_map)

        required = ["open", "high", "low", "close", "volume"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required OHLCV columns after normalization: {missing}")

        if to_btfeed:
            try:
                import backtrader.feeds as btfeeds

                bt_data = btfeeds.PandasData(dataname=df[required])
                return bt_data
            except Exception as e:  # pragma: no cover - optional dependency
                raise RuntimeError(
                    "backtrader is required to convert to bt feed: %s" % e
                )

        # Return columns; include 'time' first if present (useful for charting)
        if "time" in df.columns:
            return df[["time"] + required]

        return df[required]

    def _prepare_data_from_yahoo(self, data: DataFrame, to_btfeed: bool = True, **kwargs) -> Any:
        """Prepare a provider DataFrame for backtrader.

        If `to_bt` is True this returns a backtrader feed object (PandasData).
        Otherwise it returns the normalized DataFrame.
        """
        df = data.copy()
        df = df.rename(columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        })
        df = df[["open", "high", "low", "close", "volume"]]

        if to_btfeed:
            try:
                import backtrader.feeds as btfeeds

                bt_data = btfeeds.PandasData(dataname=df)
                return bt_data
            except Exception as e:  # pragma: no cover - optional dependency
                raise RuntimeError(
                    "backtrader is required to convert to bt feed: %s" % e
                )

        return df

    def run(self, prepared_data: Any, strategy: Any = None, **kwargs) -> Any:
        """Run a backtrader backtest using the prepared data.

        Parameters
        - prepared_data: Either a backtrader feed (when produced with to_btfeed=True)
          or a pandas DataFrame with columns ['open','high','low','close','volume']
          (and optional 'time' column). If a DataFrame is provided this method
          will convert it to a backtrader PandasData feed.
        - strategy: A backtrader Strategy class (not an instance). If provided,
          it will be added to Cerebro.
        - kwargs: Optional keyword args to control the run:
            * cash (float): starting cash (default 100000.0)
            * commission (float): commission (fraction, default 0.0)
            * sizer (callable): a backtrader sizer class to add (optional)
            * sizer_kwargs (dict): kwargs for the sizer
            * analyzers (dict): mapping of analyzer class -> name to add

        Returns a dict with keys:
        - 'cerebro': the Cerebro instance
        - 'results': the raw results returned by cerebro.run()
        """
        try:
            import backtrader as bt  # optional dependency
            import backtrader.feeds as btfeeds
        except Exception as e:  # pragma: no cover - optional dependency
            raise RuntimeError("backtrader is required to run backtests: %s" % e)

        # Defaults
        cash = float(kwargs.get("cash", 100000.0))
        commission = float(kwargs.get("commission", 0.0))
        sizer = kwargs.get("sizer", None)
        sizer_kwargs = kwargs.get("sizer_kwargs", {}) or {}
        analyzers = kwargs.get("analyzers", {}) or {}

        cerebro = bt.Cerebro()

        # Broker settings
        cerebro.broker.setcash(cash)
        safe_call(cerebro.broker.setcommission, commission=commission)

        # Add strategy if provided
        if strategy is not None:
            cerebro.addstrategy(strategy)

        # Add sizer if provided
        if sizer is not None:
            try:
                cerebro.addsizer(sizer, **sizer_kwargs)
            except Exception:
                # fall back to adding without kwargs
                cerebro.addsizer(sizer)

        # Add analyzers
        for analyzer_cls, name in analyzers.items():
            safe_call(cerebro.addanalyzer, analyzer_cls, _name=name)

        # Prepare data: if it's not already a bt feed, convert from DataFrame
        data_feed = prepared_data
        if not isinstance(prepared_data, btfeeds.PandasData):
            # Assume it's a DataFrame-like
            try:
                df = prepared_data.copy()
                # If 'time' column present, set as index (bt expects DatetimeIndex)
                if "time" in df.columns:
                    df = df.set_index(pd.to_datetime(df["time"]))
                    # drop duplicate column if exists
                    if "time" in df.columns:
                        df = df.drop(columns=[c for c in ["time"] if c in df.columns])

                # Ensure required columns
                required = ["open", "high", "low", "close", "volume"]
                missing = [c for c in required if c not in df.columns and c not in df.index.names]
                if missing:
                    raise ValueError("Missing required OHLCV columns: %s" % missing)

                data_feed = btfeeds.PandasData(dataname=df)
            except Exception as e:
                raise RuntimeError("Failed to convert prepared_data to backtrader feed: %s" % e)

        # Add the data feed(s)
        try:
            cerebro.adddata(data_feed)
        except Exception as e:
            raise RuntimeError("Failed to add data to cerebro: %s" % e)

        print("Starting Portfolio Value: %.2f" % cash)

        # Run cerebro
        results = cerebro.run()

        print("Final Portfolio Value: %.2f" % cerebro.broker.getvalue())

        # Store last run artifacts on the instance for later access
        # best-effort assignments
        safe_call(setattr, self, "last_cerebro", cerebro)
        safe_call(setattr, self, "last_results", results)

        return {"cerebro": cerebro, "results": results}

    def build_jupyter_chart(self, results: Any, width: int = 900, height: int = 600, **kwargs) -> Any:
        """Build and return a JupyterChart from backtrader results.

        Parameters
        - results: The results returned by `cerebro.run()` (list-like, where the
            strategy instance is typically at results[0]).
        - width (int): Chart width in pixels.
        - height (int): Chart height in pixels.

        Optional kwargs
        - indicator_attrs (dict): mapping of strategy attribute name -> column name
                to attach to the main dataframe. Example: {"movav": "sma"} will
                attach `strat.movav.array` as the column `sma` on the main DataFrame.
        - indicators (dict): mapping of dataframe column name -> plotting options
                Example: {"sma": {"color":"blue", "price_line":False}}

        Returns
        - chart: a `JupyterChart` instance. The chart is ready to be displayed
            using `chart.load()` in a Jupyter environment.

        Side effects
        - Persists helper artifacts on the instance: `last_df`, `last_trades_df`,
            `last_order_df`, and `last_chart`.

        Notes
        - This method imports `lightweight_charts.JupyterChart` lazily; the
            package must be installed to build the chart.
        - `indicator_attrs` must be provided if you want indicator columns to be
            attached before plotting. If `indicators` refers to columns that do
            not exist on the DataFrame they will be ignored.
        """
        try:
            from lightweight_charts import JupyterChart
        except Exception:  # pragma: no cover - optional dependency
            raise RuntimeError("lightweight_charts is required to build the chart")

        # Defensive access to strategy/results structure
        if not results:
            raise ValueError("Empty results provided to build_jupyter_chart")

        strat = results[0]

        # Build main dataframe (OHLCV + attached indicators)
        # Caller should supply `indicator_attrs` mapping if indicators are
        # desired, e.g. {"movav": "sma", "adx": "adx"}
        indicator_attrs = kwargs.pop("indicator_attrs", None)

        df = self._build_main_df(strat, indicator_attrs=indicator_attrs)

        # Build trades/orders via helpers and persist
        orders_df = self._build_orders_df(strat)

        # Build the chart
        chart = JupyterChart(width=width, height=height)
        if not df.empty:
            chart.set(df)

            # Allow flexible indicators via an `indicators` mapping where
            # keys are column names and values are option dicts, e.g.
            # {"sma": {"color":"blue","price_line":False}}
            indicators = kwargs.pop("indicators", None)

            if indicators:
                for col, opts in indicators.items():
                    # wrap indicator plotting in safe_call to avoid per-indicator try/except
                    def _plot_indicator(col=col, opts=opts):
                        if col in df.columns:
                            color = opts.get("color", "blue") if isinstance(opts, dict) else "blue"
                            price_line = opts.get("price_line", False) if isinstance(opts, dict) else False
                            line = chart.create_line(name=col, color=color, price_line=price_line)
                            df_line = df[["time", col]]
                            line.set(df_line)

                    safe_call(_plot_indicator)

        # Add buy/sell markers from the order data
        if not orders_df.empty:
            for _, order in orders_df.iterrows():
                # ignore marker failures
                safe_call(
                    chart.marker,
                    time=order["time"],
                    position=("below" if order["direction"] == "buy" else "above"),
                    shape=("arrow_up" if order["direction"] == "buy" else "arrow_down"),
                    color=("green" if order["direction"] == "buy" else "red"),
                )

        # persist chart
        safe_call(setattr, self, "last_chart", chart)

        return chart

    def _build_trades_df(self, strat: Any) -> pd.DataFrame:
        """Helper to construct trades DataFrame from a strategy instance.

        Also stores the DataFrame on `self.last_trades_df`.
        """
        trades_df = pd.DataFrame()
        closed_trades = getattr(strat, "trade_history", None)
        if closed_trades:
                try:
                    trades_list = []
                    for t in closed_trades:
                        trades_list.append(
                            {
                                "time": t.open_datetime().strftime("%Y-%m-%d"),
                                "direction": "buy" if t.size > 0 else "sell",
                                "price": t.price,
                            }
                        )
                    trades_df = pd.DataFrame(trades_list)
                except Exception:
                    trades_df = pd.DataFrame()

        safe_call(setattr, self, "last_trades_df", trades_df)

        return trades_df

    def _build_orders_df(self, strat: Any) -> pd.DataFrame:
        """Helper to construct orders DataFrame from a strategy instance.

        Also stores the DataFrame on `self.last_order_df`.
        """
        order_df = pd.DataFrame()
        orders = getattr(strat, "order_history", None)
        try:
            import backtrader as bt  # type: ignore
        except Exception:
            bt = None

        if orders:
            try:
                orders_list = []
                for o in orders:
                    created = None
                    try:
                        created = bt.num2date(o.created.dt).strftime("%Y-%m-%d")
                    except Exception:
                        created = None

                    price = None
                    try:
                        price = o.executed.price if o.executed else None
                    except Exception:
                        price = None

                    orders_list.append(
                        {
                            "time": created,
                            "direction": "buy" if getattr(o, "isbuy", lambda: False)() else "sell",
                            "price": price,
                            "status": getattr(o, "getstatusname", lambda: None)(),
                        }
                    )
                order_df = pd.DataFrame(orders_list)
            except Exception:
                order_df = pd.DataFrame()

        safe_call(setattr, self, "last_order_df", order_df)

        return order_df

    def _build_main_df(self, strat: Any, indicator_attrs: Optional[dict] = None) -> pd.DataFrame:
        """Build the main OHLCV DataFrame from a backtrader strategy instance.

        indicator_attrs: optional mapping of strategy attribute name -> column name
            e.g. {"movav": "sma", "other_indicator": "adx"}
        The method will attempt to attach these attributes from the strategy
        onto the returned DataFrame as new columns.
        """
        df = pd.DataFrame()
        try:
            # Build base OHLCV
            df = pd.DataFrame({
                "time": [
                    bt.num2date(x).strftime("%Y-%m-%d")
                    for x in strat.datas[0].datetime.array
                ],
                "open": strat.datas[0].open.array,
                "high": strat.datas[0].high.array,
                "low": strat.datas[0].low.array,
                "close": strat.datas[0].close.array,
                "volume": strat.datas[0].volume.array,
            })
        except Exception:
            # fallback empty df
            df = pd.DataFrame()

        # Attach indicators if requested
        if indicator_attrs and not df.empty:
            for attr_name, col_name in (indicator_attrs.items() if isinstance(indicator_attrs, dict) else []):
                try:
                    val = getattr(strat, attr_name, None)
                    if val is not None:
                        # Try common array-like interface
                        try:
                            df[col_name] = val.array
                        except Exception:
                            # fallback: try scalar/other
                            df[col_name] = val
                except Exception:
                    # ignore failures attaching specific indicators
                    pass

        safe_call(setattr, self, "last_df", df)

        return df


__all__ = ["BacktesterBacktrader"]
