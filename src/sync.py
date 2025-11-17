from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Tuple
from urllib.parse import quote
from xml.etree import ElementTree as ET

import requests

LOGGER = logging.getLogger(__name__)

BUCKET_URL = "https://public.hub.geosphere.at/datahub"
RESOURCE_PREFIX = "resources/nwp-v1-1h-2500m/filelisting/"
LIST_QUERY = "list-type=2&max-keys=1000&delimiter=/"
DATA_DIR = Path("/data")
CHUNK_SIZE = 1024 * 1024
LOG_DIR = Path("/logs")
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
TIMEOUT = 60 * 10


def parse_listing(xml_text: str) -> Tuple[list[str], str | None]:
    """Return filenames and continuation token from a single S3 listing response."""

    root = ET.fromstring(xml_text)
    files: list[str] = []
    for entry in root.findall("s3:Contents", namespaces=S3_NS):
        key_node = entry.find("s3:Key", namespaces=S3_NS)
        if key_node is None or not key_node.text:
            continue
        key = key_node.text
        if not key.endswith(".nc"):
            continue
        if key.startswith(RESOURCE_PREFIX):
            files.append(key.split("/")[-1])
    is_truncated = root.findtext("s3:IsTruncated", default="false", namespaces=S3_NS)
    next_token = root.findtext("s3:NextContinuationToken", default=None, namespaces=S3_NS)
    if is_truncated.lower() != "true":
        next_token = None
    return files, next_token


def fetch_remote_files() -> list[str]:
    """Iterate through the paginated S3 listing and return all filenames."""

    LOGGER.debug("Fetching remote file listing...")
    files: list[str] = []
    continuation: str | None = None
    page = 1
    while True:
        url = f"{BUCKET_URL}/?{LIST_QUERY}&prefix={RESOURCE_PREFIX}"
        if continuation:
            url += f"&continuation-token={quote(continuation, safe='')}"
        LOGGER.debug(f"Fetching listing page {page}...")
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        batch, continuation = parse_listing(response.text)
        files.extend(batch)
        LOGGER.debug(f"Found {len(batch)} files on page {page} (total so far: {len(files)})")
        if not continuation:
            break
        page += 1
    files.sort(reverse=True)
    LOGGER.info(f"Finished fetching remote listing: {len(files)} total files")
    return files


def determine_missing_files(remote_files: Iterable[str]) -> list[str]:
    """Return a list of files that do not yet exist locally."""

    LOGGER.debug("Checking for missing files...")
    missing: list[str] = []
    total = 0
    for filename in remote_files:
        total += 1
        year = filename[4:8]
        month = filename[8:10]
        candidate = DATA_DIR / f"{year}_{month}" / filename
        if not candidate.exists():
            missing.append(filename)
    LOGGER.info(f"Checked {total} files: {len(missing)} missing, {total - len(missing)} already present")
    return missing


def download_file(filename: str, show_progress: bool = False) -> None:
    """Download a single NetCDF file into the appropriate monthly folder."""

    year = filename[4:8]
    month = filename[8:10]
    destination = DATA_DIR / f"{year}_{month}" / filename
    destination.parent.mkdir(parents=True, exist_ok=True)
    url = f"{BUCKET_URL}/{RESOURCE_PREFIX}{filename}"
    LOGGER.debug(f"Starting download: {filename}")
    try:
        with requests.get(url, timeout=(15, TIMEOUT), stream=True) as response:
            response.raise_for_status()
            content_length = response.headers.get("Content-Length")
            total_size = int(content_length) if content_length else None
            downloaded = 0
            last_logged = 0
            with destination.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        handle.write(chunk)
                        downloaded += len(chunk)
                        if show_progress and downloaded - last_logged >= 100 * 1024 * 1024:
                            if total_size:
                                percent = (downloaded / total_size) * 100
                                LOGGER.info(f"Downloading {filename}: {downloaded / (1024*1024*1024):.2f} GB / {total_size / (1024*1024*1024):.2f} GB ({percent:.1f}%)")
                            else:
                                LOGGER.info(f"Downloading {filename}: {downloaded / (1024*1024*1024):.2f} GB")
                            last_logged = downloaded
            LOGGER.info(f"Completed download: {filename} ({downloaded / (1024*1024*1024):.2f} GB)")
    except Exception as e:
        LOGGER.error(f"Error downloading file {filename}: {e}")
        if destination.exists():
            LOGGER.warning(f"Removing partially downloaded file: {filename}")
            destination.unlink(missing_ok=True)
        raise

def sync_once(show_progress: bool = False) -> list[str]:
    """Perform one synchronization pass and return the downloaded filenames."""

    LOGGER.debug("Starting synchronization...")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    remote_files = fetch_remote_files()
    if not remote_files:
        LOGGER.warning("No files found in remote listing.")
        return []

    missing_files = determine_missing_files(remote_files)
    if not missing_files:
        LOGGER.info(f"Local cache already has all {len(remote_files)} file(s). Nothing to download.")
        return []

    LOGGER.info(f"Starting download of {len(missing_files)} file(s)...")
    downloaded: list[str] = []
    for idx, filename in enumerate(missing_files, 1):
        LOGGER.debug(f"Downloading file {idx}/{len(missing_files)}: {filename}")
        download_file(filename, show_progress=show_progress)
        downloaded.append(filename)
    LOGGER.info(f"Successfully downloaded {len(downloaded)} file(s)")
    return downloaded


def configure_logging(run_start: datetime, debug: bool = False) -> None:
    """Send logs to stdout and the monthly log file."""

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{run_start:%Y_%m}.log"
    
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    stdout_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.handlers.clear()
    root_logger.addHandler(stdout_handler)
    root_logger.addHandler(file_handler)
    
    LOGGER.debug(f"Logging configured. Log file: {log_path}, debug mode: {debug}")


def log_run_summary(run_start: datetime, downloaded: list[str]) -> None:
    """Log summary of the synchronization run."""

    timestamp = run_start.astimezone(timezone.utc).isoformat(timespec="minutes")
    if downloaded:
        file_list = ",".join(downloaded)
    else:
        file_list = "none"
    summary = (
        f"run_start={timestamp} downloaded_count={len(downloaded)} files={file_list}"
    )
    LOGGER.info(summary)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Synchronize NetCDF files from remote repository")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging to stdout",
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="Show download progress updates",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_start = datetime.now(timezone.utc)
    configure_logging(run_start, debug=args.debug)
    LOGGER.info("=" * 60)
    LOGGER.info(f"Synchronization started at {run_start.isoformat()}")
    LOGGER.info("=" * 60)
    try:
        downloaded = sync_once(show_progress=args.progress)
        log_run_summary(run_start, downloaded)
        LOGGER.info("=" * 60)
        LOGGER.info("Synchronization completed successfully")
        LOGGER.info("=" * 60)
    except Exception as exc:
        LOGGER.exception(f"Synchronization aborted due to an error: {exc}")
        raise


if __name__ == "__main__":
    main()
