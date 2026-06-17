"""
Structured logging with per-strategy log files + console output.
"""

import logging
from datetime import datetime
from pathlib import Path


def setup_logging(
    strategy_name: str,
    log_dir: Path,
    level: int = logging.INFO,
) -> logging.Logger:
    """Configure logging for a strategy executor.

    Creates:
        - Console handler (INFO+)
        - File handler in log_dir with timestamped filename

    Args:
        strategy_name: Used for logger name and file prefix
        log_dir: Directory for log files (created if needed)
        level: Logging level (default INFO)

    Returns:
        Configured logger instance
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"execute_trades_{timestamp}.log"

    # Root logger config
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),
        ],
    )

    # Suppress verbose ibapi logging
    logging.getLogger("ibapi").setLevel(logging.WARNING)
    logging.getLogger("ibapi.client").setLevel(logging.WARNING)
    logging.getLogger("ibapi.wrapper").setLevel(logging.WARNING)

    logger = logging.getLogger(strategy_name)
    logger.info(f"Log file: {log_file}")
    return logger
