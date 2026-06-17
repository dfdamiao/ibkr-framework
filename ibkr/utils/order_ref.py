"""Order reference string builder.

Produces compact orderRef tags you can attach to IBKR orders and parse back
later (strategy code, asset, optional signal metric, optional stop).

Format examples:
    MR|IETC/SPY|Z:-1.22
    MOM|SPY|W:0.15
    MA200|AQWA|Above
    MR|IETC/SPY|Z:-1.22_SL:5%
"""

# Strategy name -> short code used in the orderRef tag. Extend with your own;
# unknown names fall back to the first 6 characters uppercased.
STRATEGY_CODES: dict[str, str] = {
    "mean_reversion": "MR",
    "momentum": "MOM",
    "rsi_mean_reversion": "RSI",
    "bollinger_band": "BB",
    "ma_crossover": "MAX",
    "donchian_breakout": "DON",
    "regime_switching": "REG",
    "cointegration": "COI",
    "ma_200": "MA200",
}


def build_order_ref(
    strategy_name: str,
    asset: str,
    metric_label: str = "",
    metric_value: str = "",
    sl_pct: float | None = None,
) -> str:
    """Build an orderRef string.

    Args:
        strategy_name: Strategy name (e.g. "mean_reversion")
        asset: Ticker or pair (e.g. "SPY" or "IETC/SPY")
        metric_label: Optional metric label (e.g. "Z", "R", "W", "Above")
        metric_value: Optional metric value (e.g. "-1.22", "1.05")
        sl_pct: Optional stop-loss percentage to append

    Returns:
        Formatted orderRef string
    """
    code = STRATEGY_CODES.get(strategy_name, strategy_name.upper()[:6])

    if metric_label and metric_value:
        ref = f"{code}|{asset}|{metric_label}:{metric_value}"
    elif metric_label:
        ref = f"{code}|{asset}|{metric_label}"
    else:
        ref = f"{code}|{asset}"

    if sl_pct is not None and sl_pct > 0:
        # Convert fractional to percentage for display
        pct = sl_pct * 100 if sl_pct < 1 else sl_pct
        ref += f"_SL:{pct:.0f}%"

    return ref
