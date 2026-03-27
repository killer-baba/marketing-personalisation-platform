import logging
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from src.utils.config import LOG_LEVEL

# ── Log file location ─────────────────────────────────────────
LOG_DIR  = Path("logs")
LOG_FILE = LOG_DIR / "platform.log"


class _FlushHandler(logging.StreamHandler):
    """StreamHandler that flushes after every emit — fixes Windows PowerShell buffering."""
    def emit(self, record: logging.LogRecord) -> None:
        super().emit(record)
        self.flush()


def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger that writes to BOTH:
      - Console (stdout) with immediate flush
      - logs/platform.log (rotating, persisted)

    Format:
      2026-03-27 12:00:00 | INFO     | pipeline.ingest | message
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # already configured — avoid duplicate handlers

    level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(level)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── Handler 1: Console ────────────────────────────────────
    try:
        stream = open(sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1)
    except Exception:
        stream = sys.stderr

    console_handler = _FlushHandler(stream)
    console_handler.setLevel(level)
    console_handler.setFormatter(fmt)

    # ── Handler 2: File ───────────────────────────────────────
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(fmt)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.propagate = False

    return logger


@contextmanager
def log_duration(logger: logging.Logger, task_name: str):
    """
    Context manager that logs how long a block takes.

    Usage:
        with log_duration(logger, "embedding_generation"):
            ...
    """
    start = time.perf_counter()
    logger.info(f"[START] {task_name}")
    try:
        yield
    except Exception as exc:
        elapsed = (time.perf_counter() - start) * 1000
        logger.error(f"[FAILED] {task_name} | elapsed={elapsed:.1f}ms | error={exc}")
        raise
    else:
        elapsed = (time.perf_counter() - start) * 1000
        logger.info(f"[DONE]  {task_name} | elapsed={elapsed:.1f}ms")