from __future__ import annotations

import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import sync  # import after sys.path manipulation

FILENAME_RE = re.compile(r"^nwp_\d{10}\.nc$")


def test_remote_listing_contains_expected_files() -> None:
    """Fetch the live GeoSphere listing and validate its structure."""

    files = sync.fetch_remote_files()
    assert files, "Remote listing returned zero entries"
    assert files == sorted(files, reverse=True), "Files are no longer sorted"
    for name in files[:20]:
        # Sample a subset to keep the output manageable.
        assert (
            FILENAME_RE.match(name)
        ), f"Unexpected filename format from GeoSphere: {name}"