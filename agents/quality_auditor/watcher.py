"""
Quality Auditor — Artifact Watcher

Polls the metadata store (local path or S3) for new dbt run_results.json
files. Triggers the detector when a new artifact is found.

Strategy:
  - Track the last processed run_id in memory
  - On each poll, check if a newer artifact exists
  - Local mode: compare file mtime
  - S3 mode: compare LastModified from HeadObject

This decouples the Quality Auditor from Kafka for its trigger —
dbt runs are batch events, not streaming events.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# How often to check for new artifacts (seconds)
DEFAULT_POLL_INTERVAL = 30


class LocalArtifactWatcher:
    """
    Watches a local directory for new dbt run_results.json files.
    Suitable for development and on-premise deployments.

    Expected layout:
      {base_path}/latest/run_results.json   ← always the most recent run
    """

    def __init__(self, base_path: str, poll_interval: int = DEFAULT_POLL_INTERVAL):
        self._base_path     = Path(base_path)
        self._poll_interval = poll_interval
        self._last_mtime: Optional[float] = None
        self._artifact_path = self._base_path / "latest" / "run_results.json"

    def watch(self, on_new_artifact: Callable[[str], None]) -> None:
        """
        Blocking loop. Calls on_new_artifact(path) whenever a new artifact is detected.
        on_new_artifact receives the full file path as a string.
        """
        logger.info(
            "artifact_watcher_started",
            path=str(self._artifact_path),
            poll_interval=self._poll_interval,
        )

        while True:
            try:
                if self._artifact_path.exists():
                    mtime = self._artifact_path.stat().st_mtime

                    if self._last_mtime is None or mtime > self._last_mtime:
                        self._last_mtime = mtime
                        logger.info(
                            "new_artifact_detected",
                            path=str(self._artifact_path),
                            mtime=mtime,
                        )
                        on_new_artifact(str(self._artifact_path))
                else:
                    logger.debug(
                        "artifact_not_found",
                        path=str(self._artifact_path),
                    )
            except Exception as e:
                logger.error("artifact_watcher_error", error=str(e), exc_info=True)

            time.sleep(self._poll_interval)


class S3ArtifactWatcher:
    """
    Watches an S3 path for new dbt run_results.json files.
    Suitable for cloud deployments.

    Expected S3 layout:
      s3://{bucket}/dbt-artifacts/latest/run_results.json
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "dbt-artifacts/latest/run_results.json",
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        local_cache_dir: str = "/tmp/mas_artifacts",
    ):
        self._bucket         = bucket
        self._key            = prefix
        self._poll_interval  = poll_interval
        self._cache_dir      = Path(local_cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._last_etag: Optional[str] = None

        try:
            import boto3
            self._s3 = boto3.client("s3")
        except ImportError:
            raise ImportError("boto3 is required for S3ArtifactWatcher. Run: pip install boto3")

    def watch(self, on_new_artifact: Callable[[str], None]) -> None:
        logger.info(
            "s3_artifact_watcher_started",
            bucket=self._bucket,
            key=self._key,
            poll_interval=self._poll_interval,
        )

        while True:
            try:
                head = self._s3.head_object(Bucket=self._bucket, Key=self._key)
                etag = head["ETag"]

                if etag != self._last_etag:
                    self._last_etag = etag
                    local_path = self._download(head)
                    logger.info("new_s3_artifact_detected", etag=etag, local=local_path)
                    on_new_artifact(local_path)

            except self._s3.exceptions.NoSuchKey:
                logger.debug("s3_artifact_not_found", bucket=self._bucket, key=self._key)
            except Exception as e:
                logger.error("s3_watcher_error", error=str(e), exc_info=True)

            time.sleep(self._poll_interval)

    def _download(self, head: dict) -> str:
        """Download artifact to local cache and return path."""
        local_path = str(self._cache_dir / "run_results.json")
        self._s3.download_file(self._bucket, self._key, local_path)
        return local_path


def build_watcher(artifacts_path: str, poll_interval: int = DEFAULT_POLL_INTERVAL):
    """
    Factory: returns the appropriate watcher based on the artifacts_path prefix.
      s3://bucket/prefix → S3ArtifactWatcher
      /local/path        → LocalArtifactWatcher
    """
    if artifacts_path.startswith("s3://"):
        parts  = artifacts_path[5:].split("/", 1)
        bucket = parts[0]
        prefix = (parts[1] + "/run_results.json") if len(parts) > 1 else "run_results.json"
        return S3ArtifactWatcher(bucket=bucket, prefix=prefix, poll_interval=poll_interval)
    return LocalArtifactWatcher(base_path=artifacts_path, poll_interval=poll_interval)
