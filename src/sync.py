from __future__ import annotations

import logging
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
LOG_DIR = DATA_DIR / "logs"
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


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

    files: list[str] = []
    continuation: str | None = None
    while True:
        url = f"{BUCKET_URL}/?{LIST_QUERY}&prefix={RESOURCE_PREFIX}"
        if continuation:
            url += f"&continuation-token={quote(continuation, safe='')}"
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        batch, continuation = parse_listing(response.text)
        files.extend(batch)
        if not continuation:
            break
    files.sort(reverse=True)
    return files


def determine_missing_files(remote_files: Iterable[str]) -> list[str]:
    """Return a list of files that do not yet exist locally."""

    missing: list[str] = []
    for filename in remote_files:
        year = filename[4:8]
        month = filename[8:10]
        candidate = DATA_DIR / f"{year}_{month}" / filename
        if not candidate.exists():
            missing.append(filename)
    return missing


def download_file(filename: str) -> None:
    """Download a single NetCDF file into the appropriate monthly folder."""

    year = filename[4:8]
    month = filename[8:10]
    destination = DATA_DIR / f"{year}_{month}" / filename
    destination.parent.mkdir(parents=True, exist_ok=True)
    url = f"{BUCKET_URL}/{RESOURCE_PREFIX}{filename}"
    with requests.get(url, timeout=(15, 600), stream=True) as response:
        response.raise_for_status()
        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    handle.write(chunk)


def sync_once() -> list[str]:
    """Perform one synchronization pass and return the downloaded filenames."""

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    remote_files = fetch_remote_files()
    if not remote_files:
        LOGGER.warning("No files found in remote listing.")
        return []

    missing_files = determine_missing_files(remote_files)
    if not missing_files:
        LOGGER.debug(f"Local cache already has {len(remote_files)} file(s).")
        return []

    downloaded: list[str] = []
    for filename in missing_files:
        download_file(filename)
        downloaded.append(filename)
    return downloaded


def configure_logging(run_start: datetime) -> None:
    """Send logs to stdout and the monthly log file."""

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{run_start:%Y_%m}.log"
    handlers = [
        logging.StreamHandler(),
        logging.FileHandler(log_path, encoding="utf-8"),
    ]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        handlers=handlers,
    )


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


def main() -> None:
    run_start = datetime.now(timezone.utc)
    configure_logging(run_start)
    try:
        downloaded = sync_once()
    except Exception as exc:
        LOGGER.exception(f"Synchronization aborted due to an error: {exc}")
        raise
    log_run_summary(run_start, downloaded)


if __name__ == "__main__":
    main()
