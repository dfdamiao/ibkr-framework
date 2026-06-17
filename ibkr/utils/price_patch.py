"""
Price patch — yfinance fallback for stale IBKR/EU data.

Signal generators use yfinance for prices, but EU ETFs often have
1-day stale data. This patches missing/stale bars with IBKR historical.

Built fresh, standalone.
"""

import logging
import time
from datetime import datetime, timedelta

import pandas as pd

logger = logging.getLogger(__name__)


def patch_stale_prices(
    prices: dict[str, pd.Series] | dict[str, pd.DataFrame] | pd.DataFrame,
    target_date: datetime,
    contract_mapping: pd.DataFrame,
    skip_prompt: bool = False,
) -> dict[str, pd.Series] | dict[str, pd.DataFrame] | pd.DataFrame:
    """Patch stale yfinance prices with IBKR historical data.

    Supports 3 input formats:
      1. dict[ticker -> pd.Series] (Close only, most signal generators)
      2. dict[ticker -> pd.DataFrame] (OHLCV per symbol)
      3. pd.DataFrame (wide Close matrix: date index, ticker columns)

    Args:
        prices: Price data from yfinance
        target_date: Expected latest date (usually today or last trading day)
        contract_mapping: DataFrame with columns:
            strategy_ticker, ibkr_symbol, currency, exchange
        skip_prompt: If True, patch without user confirmation

    Returns:
        Same type as input, with stale tickers patched
    """
    target_ts = pd.Timestamp(target_date).normalize()

    # Build ticker -> ibkr info mapping
    ticker_map = {}
    for _, row in contract_mapping.iterrows():
        key = row.get("strategy_ticker", "")
        if key:
            ticker_map[key] = {
                "ibkr_symbol": row.get("ibkr_symbol", key),
                "currency": row.get("currency", "USD"),
                "exchange": row.get("exchange", "SMART"),
            }

    if isinstance(prices, pd.DataFrame):
        return _patch_wide_dataframe(prices, target_ts, ticker_map, skip_prompt)
    elif isinstance(prices, dict):
        first_val = next(iter(prices.values()), None)
        if isinstance(first_val, pd.DataFrame):
            return _patch_dict_dataframes(prices, target_ts, ticker_map, skip_prompt)
        return _patch_dict_series(prices, target_ts, ticker_map, skip_prompt)
    return prices


def _is_stale(data, target_ts: pd.Timestamp) -> bool:
    """Check if price data is missing or older than target date."""
    if data is None:
        return True
    if isinstance(data, pd.Series):
        if data.empty:
            return True
        last = pd.Timestamp(data.index[-1]).tz_localize(None)
        return last < target_ts
    if isinstance(data, pd.DataFrame):
        if data.empty:
            return True
        last = pd.Timestamp(data.index[-1]).tz_localize(None)
        return last < target_ts
    return True


def _fetch_ibkr_bar(
    ibkr_symbol: str,
    currency: str,
    exchange: str,
    target_date: datetime,
) -> pd.Series | None:
    """Fetch a single bar from IBKR historical data.

    Uses the bundled ConnectionManager to fetch a single historical bar.
    Falls back gracefully if IBKR connection unavailable.
    """
    try:
        from ibkr.core.connection import ConnectionManager
        from ibkr.core.contracts import build_stock
        from ibkr.config.exchanges import PAPER_PORT

        conn = ConnectionManager(port=PAPER_PORT)
        if not conn.connect_with_retry(client_id=96, max_attempts=1):
            logger.debug("Cannot connect to TWS for price patch")
            return None

        try:
            contract = build_stock(ibkr_symbol, exchange="SMART", currency=currency)
            # Request 5 days of daily bars
            end_dt = (target_date + timedelta(days=1)).strftime("%Y%m%d %H:%M:%S")
            conn.reqHistoricalData(
                reqId=9999,
                contract=contract,
                endDateTime=end_dt,
                durationStr="5 D",
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=1,
                formatDate=1,
                keepUpToDate=False,
                chartOptions=[],
            )
            time.sleep(3)  # Wait for bars

            # Historical data comes via callback — check if we got data
            if hasattr(conn, '_historical_data') and 9999 in conn._historical_data:
                bars = conn._historical_data[9999]
                if bars:
                    last_bar = bars[-1]
                    return pd.Series({
                        "Open": last_bar.open,
                        "High": last_bar.high,
                        "Low": last_bar.low,
                        "Close": last_bar.close,
                        "Volume": last_bar.volume,
                    })
        finally:
            conn.disconnect_gracefully()

    except ImportError:
        logger.debug("IBKR connection not available for price patch")
    except Exception as e:
        logger.debug(f"IBKR price fetch failed for {ibkr_symbol}: {e}")

    return None


def _patch_dict_series(
    prices: dict[str, pd.Series],
    target_ts: pd.Timestamp,
    ticker_map: dict,
    skip_prompt: bool,
) -> dict[str, pd.Series]:
    """Patch dict of {ticker -> pd.Series(Close)}."""
    stale_tickers = [t for t, s in prices.items() if _is_stale(s, target_ts)]
    if not stale_tickers:
        return prices

    logger.info(f"Found {len(stale_tickers)} stale tickers: {stale_tickers}")

    if not skip_prompt:
        response = input(
            f"Patch {len(stale_tickers)} stale tickers from IBKR? [Y/n] "
        ).strip().lower()
        if response == "n":
            logger.info("Skipping price patch")
            return prices

    for ticker in stale_tickers:
        info = ticker_map.get(ticker, {})
        ibkr_symbol = info.get("ibkr_symbol", ticker)
        currency = info.get("currency", "USD")
        exchange = info.get("exchange", "SMART")

        bar = _fetch_ibkr_bar(ibkr_symbol, currency, exchange, target_ts)
        if bar is not None:
            # Append the close price with target date
            new_entry = pd.Series(
                [bar["Close"]], index=[target_ts], name=ticker
            )
            prices[ticker] = pd.concat([prices[ticker], new_entry])
            logger.info(
                f"  Patched {ticker} ({ibkr_symbol}): "
                f"Close={bar['Close']}"
            )
            time.sleep(1)  # IBKR pacing
        else:
            logger.warning(f"  Could not patch {ticker}")

    return prices


def _patch_dict_dataframes(
    prices: dict[str, pd.DataFrame],
    target_ts: pd.Timestamp,
    ticker_map: dict,
    skip_prompt: bool,
) -> dict[str, pd.DataFrame]:
    """Patch dict of {ticker -> pd.DataFrame(OHLCV)}."""
    stale_tickers = [t for t, df in prices.items() if _is_stale(df, target_ts)]
    if not stale_tickers:
        return prices

    logger.info(f"Found {len(stale_tickers)} stale tickers: {stale_tickers}")

    if not skip_prompt:
        response = input(
            f"Patch {len(stale_tickers)} stale tickers from IBKR? [Y/n] "
        ).strip().lower()
        if response == "n":
            return prices

    for ticker in stale_tickers:
        info = ticker_map.get(ticker, {})
        ibkr_symbol = info.get("ibkr_symbol", ticker)
        currency = info.get("currency", "USD")
        exchange = info.get("exchange", "SMART")

        bar = _fetch_ibkr_bar(ibkr_symbol, currency, exchange, target_ts)
        if bar is not None:
            new_row = pd.DataFrame([bar], index=[target_ts])
            prices[ticker] = pd.concat([prices[ticker], new_row])
            logger.info(f"  Patched {ticker}: Close={bar['Close']}")
            time.sleep(1)

    return prices


def _patch_wide_dataframe(
    prices: pd.DataFrame,
    target_ts: pd.Timestamp,
    ticker_map: dict,
    skip_prompt: bool,
) -> pd.DataFrame:
    """Patch wide DataFrame (date index, ticker columns)."""
    if prices.empty:
        return prices

    last_date = pd.Timestamp(prices.index[-1]).tz_localize(None)
    if last_date >= target_ts:
        return prices  # Not stale

    # All columns are stale if the index is old
    stale_tickers = list(prices.columns)
    logger.info(
        f"Wide DataFrame stale (last={last_date.date()}, "
        f"target={target_ts.date()}), {len(stale_tickers)} tickers"
    )

    if not skip_prompt:
        response = input("Patch from IBKR? [Y/n] ").strip().lower()
        if response == "n":
            return prices

    new_row = {}
    for ticker in stale_tickers:
        info = ticker_map.get(ticker, {})
        ibkr_symbol = info.get("ibkr_symbol", ticker)
        currency = info.get("currency", "USD")
        exchange = info.get("exchange", "SMART")

        bar = _fetch_ibkr_bar(ibkr_symbol, currency, exchange, target_ts)
        if bar is not None:
            new_row[ticker] = bar["Close"]
            time.sleep(1)

    if new_row:
        new_df = pd.DataFrame([new_row], index=[target_ts])
        prices = pd.concat([prices, new_df])
        logger.info(f"  Patched {len(new_row)} tickers in wide DataFrame")

    return prices
