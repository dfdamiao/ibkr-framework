"""
Base executor — the 3-phase non-blocking execution pipeline.

Replaces ~6,000 lines of duplicated code across 7 executor scripts.
Each strategy creates a thin subclass that overrides hook methods.

Pipeline:
    Phase 1: Submit all EXIT orders (5s per order)
    Phase 2: Submit all ENTRY orders (5s per order)
    Phase 3: Monitor all orders in parallel (2s polling, 10min timeout)

Reference: the IBKR TWS API documentation Section 17.2
"""

import json
import logging
import time
from datetime import datetime

import pandas as pd
from ibapi.order_cancel import OrderCancel

from ibkr.core.connection import ConnectionManager
from ibkr.core.contracts import from_csv_row, resolve_conid
from ibkr.execution.strategy_config import StrategyConfig
from ibkr.execution.order_router import SmartRouter
from ibkr.execution.fill_monitor import FillMonitor, FillResult
from ibkr.execution.bracket import BracketBuilder
from ibkr.utils.logging_setup import setup_logging
from ibkr.config.exchanges import PAPER_PORT

logger = logging.getLogger(__name__)

# Time between order submissions in Phase 1 & 2
ORDER_SUBMIT_DELAY = 5.0

# Maximum age of a signal before it's considered stale (1 trading day)
STALE_SIGNAL_DAYS = 1


class BaseExecutor:
    """Base class for all strategy executors.

    Subclasses MUST override:
        - calculate_shares(signal, price) -> int
        - get_order_ref(signal, action) -> str

    Subclasses MAY override:
        - build_position_row(signal, fill) -> dict
        - on_entry_fill(signal, fill) -> None
        - on_exit_fill(signal, fill) -> None
        - normalize_action(action_str) -> str
    """

    def __init__(self, config: StrategyConfig):
        self.config = config
        self.conn: ConnectionManager | None = None
        self.router: SmartRouter | None = None
        self.monitor: FillMonitor | None = None
        self.bracket: BracketBuilder | None = None

        # Data
        self.contract_mapping: dict[str, dict] = {}
        self.pending_signals: pd.DataFrame | None = None
        self.active_positions: pd.DataFrame | None = None

        # Tracking
        self.exit_order_ids: list[tuple[int, pd.Series]] = []
        self.entry_order_ids: list[tuple[int, pd.Series]] = []
        # parent entry orderId -> STP child orderId (bracket strategies only).
        # Lets _process_results backfill the child STP's permId onto the
        # active_positions row so the daily --trail / _cancel_stp_for_exit can
        # resolve and cancel the stop cross-session.
        self._bracket_child: dict[int, int] = {}
        self._last_removed_position: dict | None = None

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Execute the full pipeline: connect → load → execute → save."""
        setup_logging(self.config.name, self.config.log_dir)
        logger.info(f"{'='*60}")
        logger.info(
            f"EXECUTOR: {self.config.name} " f"(client_id={self.config.client_id})"
        )
        logger.info(f"{'='*60}")

        # HOLD guard — refuse to execute when portfolio_state.json::hold.active
        # is true. Set by the 2026-05-17 lockdown pending DSR re-cohort regen
        # + PORTFOLIO_WEIGHTING_METHODS.md fixes. Bypass by editing the JSON.
        if self._check_hold_active():
            return

        try:
            # Connect
            if not self.connect():
                logger.error("Connection failed — aborting")
                return

            # Pre-populate perm_id_map for STP cancel resolution
            if self.config.has_bracket_stp:
                logger.info("Loading open orders for STP resolution...")
                self.conn.get_all_open_orders_sync(timeout=10.0)
                self.conn.refresh_order_id()

            # Load data
            self.contract_mapping = self.load_contract_mapping()
            self.pending_signals = self.load_signals()
            self.active_positions = self.load_positions()

            if self.pending_signals.empty:
                logger.info("No pending signals — nothing to do")
                return

            # Separate exits and entries
            exits, entries = self._split_signals(self.pending_signals)
            logger.info(f"Signals: {len(exits)} exits, {len(entries)} entries")

            # Phase 1: Submit EXIT orders
            if not exits.empty:
                logger.info(f"\n{'='*40}")
                logger.info("PHASE 1: EXIT ORDERS")
                logger.info(f"{'='*40}")
                self._execute_exits(exits)

            # Phase 2: Submit ENTRY orders
            if not entries.empty:
                logger.info(f"\n{'='*40}")
                logger.info("PHASE 2: ENTRY ORDERS")
                logger.info(f"{'='*40}")
                self._execute_entries(entries)
                # Persist entry_perm_id NOW, before the long monitor: if the
                # process is killed mid-monitor (e.g. launcher timeout) the
                # PENDING row already carries its permId, so a re-run skips
                # re-submit (no double position) and the orchestrator can
                # resolve the fill by permId. (2026-06-13 XSD stranding.)
                self._persist_entry_perm_ids()

            # Phase 3: Monitor all orders
            all_order_ids = [oid for oid, _ in self.exit_order_ids] + [
                oid for oid, _ in self.entry_order_ids
            ]
            if all_order_ids:
                logger.info(f"\n{'='*40}")
                logger.info("PHASE 3: MONITORING ORDERS")
                logger.info(f"{'='*40}")
                results = self.monitor.monitor_all(all_order_ids)
                self._process_results(results)

            # Report any failed STP cancels
            if self.bracket:
                self.bracket.report_failed_cancels()

        except Exception as e:
            logger.error(f"Executor error: {e}", exc_info=True)
        finally:
            if self.conn:
                self.conn.disconnect_gracefully()
            logger.info(f"\n{'='*60}")
            logger.info("EXECUTOR COMPLETE")
            logger.info(f"{'='*60}")

    # ------------------------------------------------------------------
    # Hold guard
    # ------------------------------------------------------------------

    def _check_hold_active(self) -> bool:
        """Return True (and log) if portfolio_state.json::hold.active is True.

        Refuses to connect/trade. Bypass = edit JSON. Missing file or missing
        hold key = not held (executor proceeds normally).
        """
        path = self.config.portfolio_state_file
        if not path.exists():
            return False
        try:
            state = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError) as e:
            logger.error(
                f"portfolio_state.json unreadable ({e}) — refusing to "
                f"execute. Fix the file before re-running."
            )
            return True
        hold = state.get("hold") or {}
        if not hold.get("active"):
            return False
        reason = hold.get("reason", "(no reason recorded)")
        set_at = hold.get("set_at", "(unknown date)")
        logger.error("=" * 60)
        logger.error("EXECUTION BLOCKED — strategy is ON HOLD")
        logger.error("=" * 60)
        logger.error(f"  strategy : {self.config.name}")
        logger.error(f"  set_at   : {set_at}")
        logger.error(f"  reason   : {reason}")
        logger.error(f"  source   : {path}")
        logger.error(
            "  bypass   : edit portfolio_state.json and set "
            "hold.active = false (only after the hold reason is resolved)"
        )
        logger.error("=" * 60)
        return True

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        """Connect to TWS."""
        self.conn = ConnectionManager(port=PAPER_PORT)
        if not self.conn.connect_with_retry(client_id=self.config.client_id):
            return False

        self.router = SmartRouter(self.conn)
        self.monitor = FillMonitor(self.conn)
        if self.config.has_bracket_stp:
            self.bracket = BracketBuilder(self.conn)
        return True

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def load_signals(self) -> pd.DataFrame:
        """Load pending signals, filter stale and non-PENDING."""
        path = self.config.signals_file
        if not path.exists():
            logger.warning(f"Signals file not found: {path}")
            return pd.DataFrame()

        df = pd.read_csv(path)
        if df.empty:
            return df

        # Filter PENDING status only. A missing "status" column means the
        # on-disk header drifted from the generator's write schema (e.g. a
        # stale pre-seeded pending_signals.csv header). Refuse to treat the
        # misaligned rows as executable rather than silently submitting
        # scrambled orders.
        if "status" not in df.columns:
            logger.error(
                f"'status' column absent in {path} - header/schema drift; "
                f"refusing to execute {len(df)} unfiltered rows"
            )
            return pd.DataFrame()
        df = df[df["status"] == "PENDING"].copy()

        # Filter stale signals (> 1 trading day old)
        if "date" in df.columns:
            cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(
                days=STALE_SIGNAL_DAYS
            )
            dates = pd.to_datetime(df["date"], errors="coerce")
            stale = dates < cutoff
            if stale.any():
                for _, row in df[stale].iterrows():
                    asset = row.get(self.config.signal_column, "unknown")
                    logger.warning(
                        f"⚠️ STALE signal skipped: {asset} "
                        f"from {row.get('date', '?')}"
                    )
                df = df[~stale]

        logger.info(f"Loaded {len(df)} pending signals from {path}")
        return df

    def load_contract_mapping(self) -> dict[str, dict]:
        """Load contract mapping CSV into dict keyed by strategy_ticker."""
        path = self.config.contract_mapping_file
        if not path.exists():
            logger.warning(f"Contract mapping not found: {path}")
            return {}

        df = pd.read_csv(path)
        mapping = {}
        for _, row in df.iterrows():
            key = row.get("strategy_ticker", row.get("ibkr_symbol", ""))
            mapping[key] = row.to_dict()

        logger.info(f"Loaded {len(mapping)} contracts from {path}")
        return mapping

    def load_positions(self) -> pd.DataFrame:
        """Load active positions with backup before any modifications."""
        path = self.config.positions_file
        if not path.exists():
            logger.info(f"No active positions file: {path}")
            return pd.DataFrame()

        df = pd.read_csv(path)

        # Create timestamped backup
        if not df.empty:
            self.config.backup_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup = self.config.backup_dir / f"active_positions_{ts}.csv"
            df.to_csv(backup, index=False)
            logger.debug(f"Position backup: {backup}")

        logger.info(f"Loaded {len(df)} active positions")
        return df

    # ------------------------------------------------------------------
    # Signal processing
    # ------------------------------------------------------------------

    def _split_signals(
        self, signals: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Split signals into exits and entries."""
        col = self.config.action_column
        if col not in signals.columns:
            logger.error(f"Action column '{col}' not in signals")
            return pd.DataFrame(), pd.DataFrame()

        actions = signals[col].apply(self.normalize_action)

        exits = signals[actions == "SELL"]
        entries = signals[actions == "BUY"]
        return exits, entries

    def normalize_action(self, action: str) -> str:
        """Normalize action strings to BUY/SELL.

        Lesson learned: ma200 uses ENTER/EXIT, others use BUY/SELL.
        """
        action_upper = str(action).upper().strip()
        if action_upper in ("BUY", "ENTER", "ENTRY"):
            return "BUY"
        if action_upper in ("SELL", "EXIT"):
            return "SELL"
        return action_upper

    # ------------------------------------------------------------------
    # Phase 1: Exit orders
    # ------------------------------------------------------------------

    def _execute_exits(self, exits: pd.DataFrame) -> None:
        """Submit EXIT orders (Phase 1)."""
        for idx, signal in exits.iterrows():
            asset = signal.get(self.config.signal_column, "unknown")
            logger.info(f"\nEXIT: {asset}")

            # Cancel STP if bracket strategy
            if self.config.has_bracket_stp and self.bracket:
                self._cancel_stp_for_exit(signal)

            # Build contract
            contract = self._resolve_contract(signal)
            if contract is None:
                continue

            # Get shares from active positions
            shares = self._get_exit_shares(signal)
            if shares <= 0:
                logger.warning(f"  ⚠️ No shares to exit for {asset} — skipped")
                continue

            # Build order ref
            order_ref = self.get_order_ref(signal, "SELL")

            # Submit via router
            csv_exchange = self._get_csv_exchange(signal)
            order_id = self.router.submit_with_fallback(
                contract, "SELL", shares, csv_exchange, order_ref
            )

            if order_id is not None:
                self.exit_order_ids.append((order_id, signal))
                logger.info(f"  ✅ Submitted EXIT orderId={order_id}")
            else:
                logger.error(f"  ❌ EXIT FAILED for {asset}")

            time.sleep(ORDER_SUBMIT_DELAY)

    def _cancel_stp_for_exit(self, signal: pd.Series) -> None:
        """Cancel existing STP order before selling position."""
        if self.active_positions is None or self.active_positions.empty:
            return

        asset = signal.get(self.config.signal_column, "")
        col = self.config.signal_column

        # Find matching position
        mask = (
            self.active_positions[col] == asset
            if col in self.active_positions.columns
            else pd.Series(dtype=bool)
        )
        if not mask.any():
            return

        pos = self.active_positions[mask].iloc[0]
        stp_order_id = pos.get("stop_order_id")
        perm_id = pos.get("stop_perm_id", 0)

        if pd.notna(stp_order_id) and int(stp_order_id) > 0:
            # Try cancel by permId first, fall back to orderId
            if perm_id and int(perm_id) > 0:
                self.bracket.cancel_stop_order(int(perm_id))
            else:
                logger.info(
                    f"  STP cancel: no permId, trying orderId={int(stp_order_id)}"
                )
                try:
                    self.conn.cancelOrder(int(stp_order_id), OrderCancel())
                    time.sleep(1.0)
                except Exception as e:
                    logger.warning(f"  STP cancel failed: {e}")

    # ------------------------------------------------------------------
    # Phase 2: Entry orders
    # ------------------------------------------------------------------

    def _execute_entries(self, entries: pd.DataFrame) -> None:
        """Submit ENTRY orders (Phase 2)."""
        for idx, signal in entries.iterrows():
            asset = signal.get(self.config.signal_column, "unknown")

            # Idempotent re-run: if a prior run already submitted this
            # signal (perm_id persisted) the order is still WORKING at
            # IBKR — skip re-submit. Manually flip the row's status to
            # FAILED to retry.
            prior_perm = signal.get("entry_perm_id")
            if pd.notna(prior_perm) and str(prior_perm).strip() not in ("", "0"):
                logger.info(
                    f"\n⚠️ ENTRY {asset}: already submitted in prior run "
                    f"(entry_perm_id={prior_perm}) — skipping re-submit"
                )
                continue

            logger.info(f"\nENTRY: {asset}")

            # Build contract
            contract = self._resolve_contract(signal)
            if contract is None:
                continue

            # Calculate shares
            shares = self.calculate_shares(signal, 0.0)
            if shares <= 0:
                sizing = {
                    k: signal.get(k)
                    for k in (
                        "weight",
                        "allocated_dollars",
                        "position_dollars",
                        "numerator_price",
                        "shares",
                    )
                    if k in signal.index
                }
                logger.warning(
                    f"  ⚠️ Invalid share count for {asset} "
                    f"(shares<=0) — skipped; sizing inputs: {sizing}"
                )
                continue

            # Build order ref
            order_ref = self.get_order_ref(signal, "BUY")

            if self.config.has_bracket_stp and self.bracket:
                # Bracket order (entry + STP child).
                # Two modes for the STP price:
                #   1. stop_price_column: absolute price pre-computed by
                #      signal_generator (e.g. TSMOM's ATR stop_level)
                #   2. stop_loss_column: percentage; STP price computed
                #      as estimated_price * (1 - sl_pct) by BracketBuilder
                abs_stop = None
                if self.config.stop_price_column:
                    raw = signal.get(self.config.stop_price_column)
                    if raw is not None and pd.notna(raw):
                        try:
                            abs_stop = float(raw)
                        except (TypeError, ValueError):
                            abs_stop = None

                sl_col = self.config.stop_loss_column
                sl_pct = float(signal.get(sl_col, 0.05)) if sl_col else 0.05
                # Normalize to fractional
                if sl_pct > 1:
                    sl_pct = sl_pct / 100

                est_price = float(
                    signal.get(
                        "numerator_price",
                        signal.get("execution_price", signal.get("price", 0)),
                    )
                )

                parent_id, child_id = self.bracket.submit_bracket_entry(
                    contract=contract,
                    action="BUY",
                    quantity=shares,
                    stop_loss_pct=sl_pct,
                    estimated_price=est_price,
                    order_ref=order_ref,
                    stop_price=abs_stop,
                )
                self.entry_order_ids.append((parent_id, signal))
                self._bracket_child[parent_id] = child_id
                logger.info(f"  ✅ Bracket: parent={parent_id} stp={child_id}")
            else:
                # Single-leg order via router
                csv_exchange = self._get_csv_exchange(signal)
                order_id = self.router.submit_with_fallback(
                    contract, "BUY", shares, csv_exchange, order_ref
                )
                if order_id is not None:
                    self.entry_order_ids.append((order_id, signal))
                    logger.info(f"  ✅ Submitted ENTRY orderId={order_id}")
                else:
                    logger.error(f"  ❌ ENTRY FAILED for {asset}")

            time.sleep(ORDER_SUBMIT_DELAY)

    def _persist_entry_perm_ids(self, *, settle: float = 3.0) -> None:
        """Persist entry_perm_id to each submitted entry's PENDING row BEFORE
        the long monitor.

        If the process is killed mid-monitor (e.g. the launcher's per-script
        timeout, which left XSD a bare PENDING on 2026-06-13), the row already
        carries its permId — so a re-run hits the idempotent skip in
        _execute_entries (no double-submit) and the orchestrator can resolve the
        fill by permId rather than depending on orderRef. permId arrives via the
        openOrder/orderStatus callback shortly after placement, so poll the
        connection's order map briefly. Best-effort: an unresolved permId is left
        for _process_results to persist on terminal status (the prior behaviour).
        """
        if not self.entry_order_ids or self.conn is None:
            return
        pending = list(self.entry_order_ids)
        deadline = time.monotonic() + settle
        while pending:
            unresolved = []
            for order_id, signal in pending:
                perm = int(self.conn.orders.get(order_id, {}).get("permId", 0) or 0)
                if perm > 0:
                    self._persist_pending_perm(signal, perm)
                else:
                    unresolved.append((order_id, signal))
            pending = unresolved
            if not pending or time.monotonic() >= deadline:
                break
            time.sleep(0.2)
        if pending:
            assets = [str(s.get(self.config.signal_column, "?")) for _, s in pending]
            logger.warning(
                f"  entry permId unresolved pre-monitor for {assets}; "
                f"_process_results will persist on terminal status"
            )

    # ------------------------------------------------------------------
    # Sizing / cap preview (runs in BOTH dry-run and real via calculate_shares)
    # ------------------------------------------------------------------

    def _per_name_cap(self) -> float | None:
        """Per-name cap (e.g. 0.30) from the portfolio_state scheme's capNN
        suffix; None if absent. Cached. Pure read; never raises."""
        if getattr(self, "_cap_cache_set", False):
            return self._cap_cache
        cap: float | None = None
        try:
            import json
            import re

            p = self.config.portfolio_state_file
            if p.exists():
                scheme = json.loads(p.read_text()).get("scheme", "")
                m = re.search(r"cap(\d+)", scheme or "")
                if m:
                    cap = int(m.group(1)) / 100.0
        except Exception:
            cap = None
        self._cap_cache = cap
        self._cap_cache_set = True
        return cap

    def _log_cap_check(self, ticker: str, weight: float, cash_pool: float) -> None:
        """Explicit per-name sizing/cap line, logged in BOTH dry-run and real
        (calculate_shares is called on both paths). The 2026-06-03 bug deployed
        ~95% of NAV in one name because the persisted target_weight was
        uncapped; this surfaces the effective weight vs the cap loudly. Pure
        logging: never raises, never blocks an order."""
        try:
            dollars = float(weight) * float(cash_pool)
            pct = float(weight) * 100.0
            cap = self._per_name_cap()
            if cap is not None and float(weight) > cap + 1e-6:
                logger.warning(
                    "  !!! CAP CHECK %s: %.1f%% of pool ($%s) EXCEEDS cap "
                    "%.0f%% -- abort recommended (sizing bug, see test_22)",
                    ticker,
                    pct,
                    f"{dollars:,.0f}",
                    cap * 100.0,
                )
            else:
                cap_txt = f" (cap {cap * 100:.0f}%)" if cap is not None else ""
                logger.info(
                    "  CAP CHECK %s: %.1f%% of pool ($%s)%s OK",
                    ticker,
                    pct,
                    f"{dollars:,.0f}",
                    cap_txt,
                )
        except Exception:
            pass

    def _apply_cap(self, ticker: str, weight: float) -> float:
        """HARD-ENFORCE the per-name cap at sizing time: returns min(weight,
        cap). A mis-capped target_weight (the 2026-06-03 bug) therefore can
        NEVER over-deploy past the cap, in dry-run or live -- the order is sized
        to the cap, not the oversized weight. Logs an ERROR when it has to
        clamp. Fails open (returns weight unchanged) if the cap is unknown."""
        try:
            cap = self._per_name_cap()
            if cap is not None and float(weight) > cap + 1e-6:
                logger.error(
                    "  CAP ENFORCED %s: target_weight %.4f EXCEEDS cap %.2f -- "
                    "clamping to %.2f (sizing bug, see test_22); order sized to "
                    "the cap, NOT the oversized weight",
                    ticker,
                    float(weight),
                    cap,
                    cap,
                )
                return cap
        except Exception:
            pass
        return weight

    # ------------------------------------------------------------------
    # Phase 3: Process results
    # ------------------------------------------------------------------

    def _process_results(self, results: dict[int, FillResult]) -> None:
        """Process fill results — update positions and signal status."""
        # Process exit fills
        for order_id, signal in self.exit_order_ids:
            result = results.get(order_id)
            if result is None:
                continue

            asset = signal.get(self.config.signal_column, "unknown")

            if result.is_filled:
                # Log entry vs exit price (P&L visibility)
                self._last_removed_position = self._remove_position(signal)
                pos = self._last_removed_position or {}
                entry_price = float(pos.get("entry_price", 0))
                currency = pos.get("currency", self._get_currency(signal))
                ccy = "€" if currency == "EUR" else "$"
                pnl_pct = (
                    (result.avg_price - entry_price) / entry_price * 100
                    if entry_price > 0
                    else 0
                )
                pnl_val = (result.avg_price - entry_price) * result.filled_qty
                logger.info(
                    f"  EXIT {asset}: entry={ccy}{entry_price:.4f} "
                    f"-> exit={ccy}{result.avg_price:.4f} "
                    f"({int(result.filled_qty)} shares, "
                    f"P&L={ccy}{pnl_val:+,.2f} / {pnl_pct:+.2f}%)"
                )

                self.on_exit_fill(signal, result)
                self._update_signal_status(signal, "EXECUTED", result.avg_price)
                self._write_closed_trade(signal, result)
            elif result.is_partial or result.status == "Timeout":
                # Stuck PreSubmitted / EU overnight: mark PARTIAL_FILL so
                # resolve_fills.py picks it up next session. Marking FAILED
                # would drop it from reconciliation.
                self._update_signal_status(signal, "PARTIAL_FILL")
            else:
                self._update_signal_status(signal, "FAILED")

        # Process entry fills
        for order_id, signal in self.entry_order_ids:
            result = results.get(order_id)
            if result is None:
                continue

            asset = signal.get(self.config.signal_column, "unknown")

            if result.is_filled:
                # Log signal price vs fill price (slippage visibility)
                signal_price = float(
                    signal.get(
                        "numerator_price",
                        signal.get(
                            "execution_price",
                            signal.get("price", signal.get("close_t1", 0)),
                        ),
                    )
                )
                slippage_bps = (
                    (result.avg_price - signal_price) / signal_price * 10000
                    if signal_price > 0
                    else 0
                )
                currency = self._get_currency(signal)
                ccy = "€" if currency == "EUR" else "$"
                entry_value = result.avg_price * result.filled_qty
                logger.info(
                    f"  ENTRY {asset}: signal={ccy}{signal_price:.4f} "
                    f"-> fill={ccy}{result.avg_price:.4f} "
                    f"({int(result.filled_qty)} shares = "
                    f"{ccy}{entry_value:,.2f}) "
                    f"slippage={slippage_bps:+.1f} bps"
                )

                self.on_entry_fill(signal, result)
                position_row = self.build_position_row(signal, result)
                self._add_position(position_row)
                self._backfill_bracket_stp(order_id, signal)
                self._update_signal_status(signal, "EXECUTED", result.avg_price)
            elif result.is_partial:
                self._update_signal_status(signal, "PARTIAL_FILL")
            elif result.status == "Timeout":
                # Order still WORKING at IBKR — leave PENDING and
                # persist permId so the next run can either skip
                # (idempotent check above) or be reconciled by the
                # orchestrator.
                self._persist_pending_perm(signal, result.perm_id)
                logger.warning(
                    f"  ENTRY TIMEOUT for {asset} — order still WORKING "
                    f"at IBKR (permId={result.perm_id}). Status stays "
                    f"PENDING; re-run to reconcile once filled."
                )
            else:
                self._update_signal_status(signal, "FAILED")

    # ------------------------------------------------------------------
    # CSV management
    # ------------------------------------------------------------------

    def _add_position(self, row: dict) -> None:
        """Add a new position to active_positions."""
        new_row = pd.DataFrame([row])
        if self.active_positions is not None and not self.active_positions.empty:
            self.active_positions = pd.concat(
                [self.active_positions, new_row], ignore_index=True
            )
        else:
            self.active_positions = new_row
        self._save_positions()

    def _remove_position(self, signal: pd.Series) -> dict | None:
        """Remove a position from active_positions.

        Lesson learned: must return removed row for closed_trades.csv.
        """
        if self.active_positions is None or self.active_positions.empty:
            return None

        asset = signal.get(self.config.signal_column, "")
        col = self.config.signal_column

        if col not in self.active_positions.columns:
            return None

        mask = self.active_positions[col] == asset
        if not mask.any():
            return None

        removed = self.active_positions[mask].iloc[0].to_dict()
        self.active_positions = self.active_positions[~mask]
        self._save_positions()
        return removed

    def _save_positions(self) -> None:
        """Write active positions to CSV."""
        path = self.config.positions_file
        path.parent.mkdir(parents=True, exist_ok=True)
        if self.active_positions is not None:
            self.active_positions.to_csv(path, index=False)

    def _backfill_bracket_stp(self, parent_order_id: int, signal: pd.Series) -> None:
        """After a bracket entry fills, resolve the STP child's permId and
        write it onto the just-added active_positions row.

        Why: ``submit_bracket_entry`` returns the child's session orderId but
        it was previously discarded, so ``stop_order_id`` / ``stop_perm_id``
        stayed null forever. The daily ``--trail`` step and
        ``_cancel_stp_for_exit`` resolve the stop by permId (via
        ``conn.perm_id_map``); without it they can never cancel a drifted stop.
        No-op for single-leg strategies (they never populate ``_bracket_child``).
        """
        child_id = self._bracket_child.get(parent_order_id)
        if child_id is None:
            return  # not a bracket entry — nothing to backfill
        child_perm = (self.conn.orders.get(child_id, {}) or {}).get("permId", 0)
        try:
            child_perm = int(child_perm or 0)
        except (TypeError, ValueError):
            child_perm = 0
        if child_perm <= 0:
            logger.warning(
                f"  STP child orderId={child_id} has no permId yet — "
                f"stop_order_id/stop_perm_id left blank. Backfill via "
                f"stp_audit/--trail from a session holding the open-orders "
                f"snapshot before relying on trailing."
            )
            return
        if self.active_positions is None or self.active_positions.empty:
            return
        col = self.config.signal_column
        if col not in self.active_positions.columns:
            return
        asset = signal.get(col, "")
        mask = self.active_positions[col] == asset
        if not mask.any():
            return
        # Convention in this codebase: both columns carry the STP permId
        # (one component reads stop_order_id as a permId; another writes the
        # permId there on a trail resubmit).
        for c in ("stop_order_id", "stop_perm_id"):
            if c not in self.active_positions.columns:
                self.active_positions[c] = pd.NA
        self.active_positions.loc[mask, "stop_order_id"] = child_perm
        self.active_positions.loc[mask, "stop_perm_id"] = child_perm
        self._save_positions()
        logger.info(f"  STP backfilled: {asset} stop permId={child_perm}")

    def _update_signal_status(
        self,
        signal: pd.Series,
        new_status: str,
        fill_price: float | None = None,
    ) -> None:
        """Update signal status in pending_signals.csv.

        Lesson learned: write None not "" for null values.
        """
        path = self.config.signals_file
        if not path.exists():
            return

        df = pd.read_csv(path)
        asset = signal.get(self.config.signal_column, "")
        col = self.config.signal_column

        if col not in df.columns:
            return

        mask = (df[col] == asset) & (df["status"] == "PENDING")

        if mask.any():
            df.loc[mask, "status"] = new_status
            if fill_price is not None:
                df.loc[mask, "fill_price"] = fill_price
            df.loc[mask, "executed_at"] = datetime.now().isoformat()
            df.to_csv(path, index=False)

    def _persist_pending_perm(self, signal: pd.Series, perm_id: int) -> None:
        """Write entry_perm_id to a PENDING signal row so a subsequent
        run can reconcile or skip-resubmit via the idempotent check at
        the top of _execute_entries."""
        if not perm_id or int(perm_id) <= 0:
            return
        path = self.config.signals_file
        if not path.exists():
            return
        df = pd.read_csv(path)
        asset = signal.get(self.config.signal_column, "")
        col = self.config.signal_column
        if col not in df.columns:
            return
        mask = (df[col] == asset) & (df["status"] == "PENDING")
        if not mask.any():
            return
        if "entry_perm_id" not in df.columns:
            df["entry_perm_id"] = pd.NA
        df.loc[mask, "entry_perm_id"] = int(perm_id)
        df.to_csv(path, index=False)

    def _write_closed_trade(self, signal: pd.Series, result: FillResult) -> None:
        """Append closed trade record.

        Stores four raw prices per trade — yfinance entry-signal close,
        IBKR entry fill, yfinance exit-signal close, IBKR exit fill —
        plus entry + exit commissions. Slippage / execution-quality bps
        are computed downstream from these raw values, not here.
        """
        path = self.config.closed_trades_file
        asset = signal.get(self.config.signal_column, "unknown")

        # Get the position data for entry info
        removed_pos = self._last_removed_position or {}

        # yfinance close at EXIT signal time
        exit_signal_price = float(
            signal.get(
                "numerator_price",
                signal.get(
                    "execution_price", signal.get("price", signal.get("close_t1", 0))
                ),
            )
        )
        # yfinance close at ENTER signal time — preserved on the active
        # position row at entry, so it survives into closed_trades here.
        entry_signal_price = float(removed_pos.get("entry_signal_price", 0) or 0)

        entry_price = float(removed_pos.get("entry_price", 0))
        entry_shares = float(removed_pos.get("shares", 0))
        currency = removed_pos.get("currency", self._get_currency(signal))

        # Value calculations in local currency (USD or EUR)
        entry_value = entry_price * entry_shares if entry_price > 0 else None
        exit_value = result.avg_price * result.filled_qty
        pnl_gross = (
            (result.avg_price - entry_price) * result.filled_qty
            if entry_price > 0
            else None
        )

        # Net P&L = gross − entry_commission − exit_commission
        # Commissions are absolute (IBKR returns negative? — _f coerces; we
        # treat them as positive cost and subtract).
        try:
            entry_comm = float(removed_pos.get("entry_commission", 0) or 0)
        except (TypeError, ValueError):
            entry_comm = 0.0
        try:
            exit_comm = float(result.commission or 0)
        except (TypeError, ValueError):
            exit_comm = 0.0
        pnl_net = (
            (pnl_gross - abs(entry_comm) - abs(exit_comm))
            if pnl_gross is not None
            else None
        )

        row = {
            "date": datetime.now().isoformat(),
            self.config.signal_column: asset,
            "strategy": self.config.name,
            "currency": currency,
            # Entry data (from position that was removed)
            "entry_date": removed_pos.get("entry_date", ""),
            "entry_signal_price": (
                entry_signal_price if entry_signal_price > 0 else None
            ),
            "entry_price": entry_price,
            "entry_shares": entry_shares,
            "entry_value": round(entry_value, 2) if entry_value else None,
            "entry_order_id": removed_pos.get("entry_order_id", ""),
            "entry_commission": removed_pos.get("entry_commission", ""),
            # Exit data (from this fill)
            "exit_date": datetime.now().isoformat(),
            "exit_signal_price": (exit_signal_price if exit_signal_price > 0 else None),
            "exit_price": result.avg_price,
            "exit_shares": result.filled_qty,
            "exit_value": round(exit_value, 2),
            "exit_order_id": result.order_id,
            "exit_perm_id": result.perm_id,
            "exit_commission": result.commission,
            # P&L in local currency. `pnl_local` is NET (gross − commissions)
            # so downstream consumers (nav_history, monitoring renderer) use
            # the realized figure. `pnl_local_gross` retained for diagnostics
            # / slippage attribution.
            "days_held": self._calc_days_held(removed_pos),
            "pnl_local_gross": (round(pnl_gross, 2) if pnl_gross is not None else None),
            "pnl_local": round(pnl_net, 2) if pnl_net is not None else None,
            "pnl_pct": (
                ((result.avg_price - entry_price) / entry_price * 100)
                if entry_price > 0
                else None
            ),
            # This path only fires on a signal-driven EXIT fill (see run()), so an
            # unlabelled exit is a signal exit. Strategies that set an explicit
            # reason (e.g. "rebalance_exit") still take precedence.
            "exit_reason": signal.get("reason", signal.get("exit_reason", "signal")),
            "fill_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "CLOSED",
        }

        # Preserve strategy-specific identifiers from the active position
        # row (unit_id, num/den, pair/numerator/denominator, target_weight,
        # signal_strength, sharpe_score, method/method_param/mapping/stop_type,
        # stop_level, atr_at_entry, ratio_at_entry, sector, conviction,
        # regime_target, rank, weight, etc.). Live snapshot fields that are
        # stale at exit are denylisted.
        _EXIT_DENYLIST = {
            "current_close",
            "current_price",
            "unrealized_pnl",
            "unrealized_pnl_pct",
            "unrealized_pnl_dollars",
            "peak_price",
            "peak_close",
            "position_value",
            "shares",
            "last_synced_stop",
            "last_updated",
            "days_held",
            # entry_* fields already populated above by removed_pos.get(...)
            "entry_date",
            "entry_price",
            "entry_signal_price",
            "entry_value",
            "entry_order_id",
            "entry_commission",
            # numerator/denominator/pair/ticker → already in `row` for strategies
            # that emit them, otherwise auto-propagated below
        }
        for k, v in removed_pos.items():
            if k in row or k in _EXIT_DENYLIST:
                continue
            row[k] = v

        df = pd.DataFrame([row])
        # If the file already exists, union columns with the existing schema
        # so we never silently drop placeholder header columns or misalign
        # appended values. Header-only placeholder files (which is the
        # pre-kickoff state for all 9 strategies) auto-upgrade to the
        # BaseExecutor schema on first close.
        if path.exists() and path.stat().st_size > 0:
            try:
                existing = pd.read_csv(path)
            except (pd.errors.EmptyDataError, ValueError):
                existing = pd.DataFrame()
            if not existing.empty:
                df = pd.concat([existing, df], ignore_index=True, sort=False)
            else:
                # Header-only placeholder: add placeholder columns to the new
                # row's column set so the file schema unions cleanly without
                # the empty-concat FutureWarning.
                for c in existing.columns:
                    if c not in df.columns:
                        df[c] = pd.NA
            df.to_csv(path, index=False)
        else:
            df.to_csv(path, index=False)

    def _calc_days_held(self, pos: dict) -> int | None:
        """Calculate days held from entry_date to now."""
        entry_str = pos.get("entry_date", "")
        if not entry_str:
            return None
        try:
            entry_dt = pd.to_datetime(entry_str)
            return (pd.Timestamp.now() - entry_dt).days
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Contract resolution
    # ------------------------------------------------------------------

    def _resolve_contract(self, signal: pd.Series):
        """Resolve a contract from signal + contract mapping."""
        asset = signal.get(self.config.signal_column, "")

        # For pair signals (e.g. "IETC/SPY"), use the numerator
        if "/" in str(asset):
            ticker = str(asset).split("/")[0]
        else:
            ticker = str(asset)

        csv_row = self.contract_mapping.get(ticker)
        if csv_row is None:
            logger.error(f"  No contract mapping for '{ticker}'")
            return None

        contract = from_csv_row(csv_row)

        # Resolve conId if missing
        if contract.conId == 0:
            con_id = resolve_conid(self.conn, contract)
            if con_id == 0:
                logger.error(f"  Cannot resolve conId for {ticker}")
                return None
            contract.conId = con_id

        return contract

    def _get_currency(self, signal: pd.Series) -> str:
        """Get the trading currency for a signal's ticker from contract mapping."""
        asset = signal.get(self.config.signal_column, "")
        ticker = str(asset).split("/")[0] if "/" in str(asset) else str(asset)
        csv_row = self.contract_mapping.get(ticker, {})
        return csv_row.get("currency", "USD")

    def _get_csv_exchange(self, signal: pd.Series) -> str:
        """Get the direct exchange from contract mapping for fallback."""
        asset = signal.get(self.config.signal_column, "")
        ticker = str(asset).split("/")[0] if "/" in str(asset) else str(asset)
        csv_row = self.contract_mapping.get(ticker, {})
        return csv_row.get("exchange", "")

    def _get_exit_shares(self, signal: pd.Series) -> int:
        """Get share count for an exit order.

        Lesson learned: EXIT signals must read shares from
        active_positions, not from signal CSV.
        """
        # First try signal itself
        shares = signal.get("shares")
        if pd.notna(shares) and int(shares) > 0:
            return int(shares)

        # Fall back to active positions
        if self.active_positions is not None and not self.active_positions.empty:
            asset = signal.get(self.config.signal_column, "")
            col = self.config.signal_column
            if col in self.active_positions.columns:
                mask = self.active_positions[col] == asset
                if mask.any():
                    pos_shares = self.active_positions[mask].iloc[0].get("shares", 0)
                    if pd.notna(pos_shares) and int(pos_shares) > 0:
                        return int(pos_shares)

        logger.warning("  Could not determine exit shares")
        return 0

    # ------------------------------------------------------------------
    # Hook methods — override in subclasses
    # ------------------------------------------------------------------

    def calculate_shares(self, signal: pd.Series, price: float) -> int:
        """Calculate number of shares for an entry order.

        MUST be overridden by subclasses.

        Args:
            signal: Signal row from pending_signals.csv
            price: Current/estimated price (may be 0 if unknown)

        Returns:
            Number of whole shares to buy
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement calculate_shares()"
        )

    def get_order_ref(self, signal: pd.Series, action: str) -> str:
        """Build the orderRef string for an order.

        MUST be overridden by subclasses.

        Args:
            signal: Signal row
            action: "BUY" or "SELL"

        Returns:
            Formatted orderRef string
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_order_ref()"
        )

    def build_position_row(self, signal: pd.Series, fill: FillResult) -> dict:
        """Build a dict for a new active_positions.csv row.

        Override for strategy-specific columns.
        Includes currency and position value in local currency.
        """
        currency = self._get_currency(signal)
        value = fill.avg_price * fill.filled_qty
        return {
            self.config.signal_column: signal.get(self.config.signal_column, ""),
            # yfinance close at ENTER signal time — must persist on the active
            # row so _write_closed_trade (line ~724) can recover it at exit for
            # the entry-side slippage block. Same source precedence as
            # _execute_entries. Default-schema strategies relied on this and silently wrote null.
            "entry_signal_price": float(
                signal.get(
                    "numerator_price",
                    signal.get(
                        "execution_price",
                        signal.get("price", signal.get("close_t1", 0)),
                    ),
                )
            )
            or None,
            "entry_price": fill.avg_price,
            "shares": int(fill.filled_qty),
            "currency": currency,
            "entry_value": round(value, 2),
            "entry_date": datetime.now().isoformat(),
            "entry_order_id": fill.order_id,
            "entry_perm_id": fill.perm_id,
            "entry_commission": fill.commission,
        }

    def on_entry_fill(self, signal: pd.Series, fill: FillResult) -> None:
        """Hook called after an entry order fills. Override if needed."""
        pass

    def on_exit_fill(self, signal: pd.Series, fill: FillResult) -> None:
        """Hook called after an exit order fills. Override if needed."""
        pass
