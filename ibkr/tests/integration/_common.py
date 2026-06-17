"""Shared constants and helpers for ibkr TWS integration tests.

Test plan:
  - All tests use paper TWS on port 7497
  - Test client_id = 98 (registered as "report" in ibkr/config/client_ids.py)
  - Test symbol = AAPL (Apple), NASDAQ-listed, very liquid
  - AAPL is verified NOT present in any contract_mapping.csv across the repo
  - All order tests use quantity = 1 share (max ~$220 exposure at AAPL ~$220)

Pytest does NOT auto-collect these — files are named level*.py, not test_*.py.
Each level is run manually as a standalone script.
"""
from __future__ import annotations

import logging
import sys

TEST_CLIENT_ID = 98
TEST_SYMBOL = "AAPL"       # Apple — NOT in any strategy contract_mapping.csv
TEST_EXCHANGE = "SMART"
TEST_CURRENCY = "USD"
TEST_QUANTITY = 1          # Per Daniel's instruction: 1 share at a time

PAPER_PORT = 7497


def setup_logger(name: str) -> logging.Logger:
    """Stdout logger with consistent format for all integration tests."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%H:%M:%S",
        ))
        logger.addHandler(h)
    return logger


def banner(title: str) -> str:
    bar = "=" * 70
    return f"\n{bar}\n{title}\n{bar}"


class TestResult:
    """Tally PASS/FAIL across a level's checks. Final summary printed at end."""

    def __init__(self, level_name: str):
        self.level = level_name
        self.checks: list[tuple[str, bool, str]] = []

    def check(self, name: str, passed: bool, detail: str = "") -> None:
        marker = "PASS" if passed else "FAIL"
        self.checks.append((name, passed, detail))
        print(f"  [{marker}] {name}" + (f" — {detail}" if detail else ""))

    def report(self) -> bool:
        passed = sum(1 for _, p, _ in self.checks if p)
        total = len(self.checks)
        print(f"\n{self.level}: {passed}/{total} checks passed")
        return passed == total
