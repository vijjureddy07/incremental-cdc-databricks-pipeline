"""Batch discovery and numeric ordering."""

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

BATCH_DIR_PATTERN = re.compile(r"^batch_id=(\d+)$")


class BatchDiscoveryError(ValueError):
    """Raised when an invalid or malformed batch directory is encountered."""

    pass


@dataclass
class DiscoveredBatch:
    """Discovered batch with directory path and parsed batch metadata."""

    batch_id: int
    directory_path: Path
    table_files: Dict[str, Path]
    manifest_path: Optional[Path]
    manifest_data: Optional[Dict]


def discover_batches(base_cdc_dir: Path) -> List[DiscoveredBatch]:
    """Discover, validate, and numerically sort CDC batch directories.

    Guarantees:
    - Rejects malformed batch directory names (e.g. 'batch_id=abc', 'batch-1')
    - Sorts numerically so batch 9 precedes batch 10 (not lexical string order)
    """
    if not base_cdc_dir.exists():
        return []

    discovered: List[DiscoveredBatch] = []

    for entry in base_cdc_dir.iterdir():
        if not entry.is_dir():
            continue

        # Check directory naming convention
        match = BATCH_DIR_PATTERN.match(entry.name)
        if not match:
            # Raise clear error for malformed batch directories
            raise BatchDiscoveryError(
                f"Malformed batch directory found in '{base_cdc_dir}': '{entry.name}'. "
                f"Expected pattern: 'batch_id=<numeric>' (e.g., 'batch_id=000001')."
            )

        batch_id = int(match.group(1))

        # Discover table jsonl files
        table_files: Dict[str, Path] = {}
        for file_path in entry.glob("*.jsonl"):
            table_name = file_path.stem
            table_files[table_name] = file_path

        manifest_path = entry / "manifest.json"
        manifest_data = None
        if manifest_path.exists():
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest_data = json.load(f)
            except Exception as e:
                raise BatchDiscoveryError(
                    f"Corrupt manifest.json in batch directory '{entry.name}': {e}"
                )

        discovered.append(
            DiscoveredBatch(
                batch_id=batch_id,
                directory_path=entry,
                table_files=table_files,
                manifest_path=manifest_path if manifest_path.exists() else None,
                manifest_data=manifest_data,
            )
        )

    # Strictly numeric sort by batch_id
    discovered.sort(key=lambda b: b.batch_id)
    return discovered
