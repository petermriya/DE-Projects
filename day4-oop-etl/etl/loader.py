"""Loaders write transformed records to their destination file."""
import csv
import json
from pathlib import Path
from typing import Any
from .errors import LoadingError

SUPPORTED_SUFFIXES = (".csv", ".json")


class DataLoader:
    """Writes transformed records to a CSV or JSON file, picked by the file extension."""

    def __init__(self, destination: Path) -> None:
        self.destination = Path(destination)
        self.suffix = self.destination.suffix.lower()
        # Validate the extension up front so a bad destination fails before any work is done
        if self.suffix not in SUPPORTED_SUFFIXES:
            raise LoadingError(f"Unsupported file extension: {self.destination.suffix}. Use .csv or .json")

    def load(self, records: list[dict[str, Any]]) -> int:
        """Write the records and return how many were written."""
        if not records:
            return 0

        self.destination.parent.mkdir(parents=True, exist_ok=True)

        if self.suffix == ".csv":
            return self._write_csv(records)
        return self._write_json(records)

    def _write_csv(self, records: list[dict[str, Any]]) -> int:
        # Column headers come from the first record; all records share the same keys
        fieldnames = list(records[0].keys())

        with open(self.destination, mode="w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        return len(records)

    def _write_json(self, records: list[dict[str, Any]]) -> int:
        with open(self.destination, mode="w", encoding="utf-8") as f:
            json.dump(records, f, indent=2)

        return len(records)
