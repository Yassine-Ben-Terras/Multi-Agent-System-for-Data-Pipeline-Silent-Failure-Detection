#!/usr/bin/env python3
"""
dbt Artifact Publisher

Copies dbt artifacts (manifest.json, run_results.json, sources.json)
to the MAS metadata store after each dbt run.

Usage (local):
  python dbt_integration/publish_artifacts.py --target-dir ./dbt_integration/artifacts

Usage (CI/CD — after dbt run && dbt test):
  python dbt_integration/publish_artifacts.py --s3-bucket my-dbt-artifacts

Add to your Airflow DAG post-run step or GitHub Actions workflow.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("artifact-publisher")

DBT_TARGET_DIR   = Path(os.getenv("DBT_TARGET_DIR", "target"))
ARTIFACT_NAMES   = ["manifest.json", "run_results.json", "sources.json"]


def publish_local(target_dir: str) -> None:
    """Copy artifacts to a local directory (for dev and on-premise)."""
    dest_base  = Path(target_dir)
    dest_latest = dest_base / "latest"
    timestamp   = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    dest_run    = dest_base / "runs" / timestamp

    dest_latest.mkdir(parents=True, exist_ok=True)
    dest_run.mkdir(parents=True, exist_ok=True)

    for artifact in ARTIFACT_NAMES:
        src = DBT_TARGET_DIR / artifact
        if not src.exists():
            logger.warning("artifact not found — skipping: %s", src)
            continue

        # Copy to latest/ (overwrites previous)
        shutil.copy2(src, dest_latest / artifact)
        logger.info("copied %s → %s", artifact, dest_latest / artifact)

        # Copy to runs/{timestamp}/ (immutable history)
        shutil.copy2(src, dest_run / artifact)

    logger.info("artifacts published to %s", dest_latest)


def publish_s3(bucket: str) -> None:
    """Upload artifacts to S3 (for cloud deployments)."""
    try:
        import boto3
        s3 = boto3.client("s3")
    except ImportError:
        raise ImportError("boto3 required for S3 publish. Run: pip install boto3")

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    for artifact in ARTIFACT_NAMES:
        src = DBT_TARGET_DIR / artifact
        if not src.exists():
            logger.warning("artifact not found — skipping: %s", src)
            continue

        # Upload to latest/ (overwrites)
        key_latest = f"dbt-artifacts/latest/{artifact}"
        s3.upload_file(str(src), bucket, key_latest)
        logger.info("uploaded s3://%s/%s", bucket, key_latest)

        # Upload to runs/{timestamp}/ (immutable)
        key_run = f"dbt-artifacts/runs/{timestamp}/{artifact}"
        s3.upload_file(str(src), bucket, key_run)

    logger.info("artifacts published to s3://%s/dbt-artifacts/latest/", bucket)


def validate_artifacts() -> bool:
    """Quick sanity check — ensure manifest.json has expected structure."""
    manifest_path = DBT_TARGET_DIR / "manifest.json"
    if not manifest_path.exists():
        logger.error("manifest.json not found at %s", manifest_path)
        return False

    with open(manifest_path) as f:
        manifest = json.load(f)

    nodes     = manifest.get("nodes", {})
    exposures = manifest.get("exposures", {})
    model_count = sum(1 for n in nodes.values() if n.get("resource_type") == "model")

    logger.info(
        "manifest validated — models: %s, exposures: %s",
        model_count, len(exposures),
    )
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish dbt artifacts to MAS metadata store")
    parser.add_argument("--target-dir", default="./dbt_integration/artifacts",
                        help="Local destination directory (default: ./dbt_integration/artifacts)")
    parser.add_argument("--s3-bucket", default=None,
                        help="S3 bucket name for cloud deployments")
    parser.add_argument("--skip-validation", action="store_true",
                        help="Skip manifest.json validation")
    args = parser.parse_args()

    if not args.skip_validation:
        if not validate_artifacts():
            raise SystemExit(1)

    if args.s3_bucket:
        publish_s3(args.s3_bucket)
    else:
        publish_local(args.target_dir)

    logger.info("done")


if __name__ == "__main__":
    main()
