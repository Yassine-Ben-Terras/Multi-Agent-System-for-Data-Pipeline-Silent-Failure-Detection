"""
Orchestrator — Signal Window

Implements a 5-minute tumbling window that groups incoming AnomalySignals
by pipeline_run_id. When the window closes (or a flush is triggered),
the Orchestrator correlates grouped signals into a single root cause.

Why tumbling window?
  A single upstream failure (e.g. schema drift) can produce N downstream
  symptoms (row count anomaly, quality failure, freshness violation).
  Without windowing, we'd page on-call N times for one incident.

Window lifecycle:
  - Signal arrives → added to its run_id bucket
  - Every 30s: sweep expired buckets (older than WINDOW_SECONDS)
  - Expired bucket → dispatched to correlator → ConfirmedIncident
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List

from config.schemas import AnomalySignal

logger = logging.getLogger(__name__)

WINDOW_SECONDS  = 300   # 5-minute tumbling window
SWEEP_INTERVAL  = 30    # check for expired buckets every 30s


@dataclass
class SignalBucket:
    """All signals received for one pipeline_run_id within the window."""
    run_id: str
    signals: List[AnomalySignal] = field(default_factory=list)
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def age_seconds(self) -> float:
        return (datetime.now(timezone.utc) - self.opened_at).total_seconds()

    def is_expired(self) -> bool:
        return self.age_seconds() >= WINDOW_SECONDS

    def anomaly_types(self) -> List[str]:
        return list({s.anomaly_type.value for s in self.signals})

    def highest_severity(self):
        from config.schemas import Severity
        order = [Severity.LOW, Severity.MEDIUM, Severity.HIGH, Severity.CRITICAL]
        return max(self.signals, key=lambda s: order.index(s.severity)).severity


class SignalWindow:
    """
    Thread-safe 5-minute tumbling window.
    Calls on_bucket_expired(bucket) when a bucket's window closes.
    """

    def __init__(self, on_bucket_expired: Callable[[SignalBucket], None]):
        self._buckets: Dict[str, SignalBucket] = {}
        self._lock = threading.Lock()
        self._on_expired = on_bucket_expired

    def add(self, signal: AnomalySignal) -> None:
        """Add a signal to its run_id bucket, creating the bucket if needed."""
        run_id = signal.pipeline_run_id
        with self._lock:
            if run_id not in self._buckets:
                self._buckets[run_id] = SignalBucket(run_id=run_id)
                logger.debug("bucket_opened | run_id=%s", run_id)
            self._buckets[run_id].signals.append(signal)
            logger.debug(
                "signal_added_to_bucket | run_id=%s type=%s severity=%s total=%s",
                run_id,
                signal.anomaly_type.value,
                signal.severity.value,
                len(self._buckets[run_id].signals),
            )

    def sweep(self) -> None:
        """
        Expire and dispatch all buckets older than WINDOW_SECONDS.
        Called periodically by the sweep thread.
        """
        with self._lock:
            expired = [
                bucket for bucket in self._buckets.values()
                if bucket.is_expired()
            ]
            for bucket in expired:
                del self._buckets[bucket.run_id]

        for bucket in expired:
            logger.info(
                "bucket_expired | run_id=%s signals=%s types=%s",
                bucket.run_id,
                len(bucket.signals),
                bucket.anomaly_types(),
            )
            try:
                self._on_expired(bucket)
            except Exception as e:
                logger.error("bucket_dispatch_error | run_id=%s error=%s", bucket.run_id, str(e))

    def flush_all(self) -> None:
        """Force-expire all buckets immediately. Used on shutdown."""
        with self._lock:
            all_buckets = list(self._buckets.values())
            self._buckets.clear()
        for bucket in all_buckets:
            try:
                self._on_expired(bucket)
            except Exception as e:
                logger.error("flush_error | run_id=%s error=%s", bucket.run_id, str(e))

    def start_sweep_thread(self) -> threading.Thread:
        """Start the background sweep thread. Returns the thread."""
        def _sweep_loop():
            while True:
                time.sleep(SWEEP_INTERVAL)
                self.sweep()

        t = threading.Thread(target=_sweep_loop, daemon=True, name="window-sweep")
        t.start()
        return t

    @property
    def active_buckets(self) -> int:
        with self._lock:
            return len(self._buckets)
