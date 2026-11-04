"""Extractors pull raw records out of a data source."""

from abc import ABC, abstractmethod
import csv
import requests
from typing import Any
from .errors import ExtractionError


class DataExtractor(ABC):
    """Common interface for every data source."""

    def __init__(self, source: str, name: str | None = None) -> None:
        self.source = source
        # Fall back to the class name when no friendly name is given
        self.name = name if name else self.__class__.__name__

    @abstractmethod
    def extract(self) -> list[dict[str, Any]]:
        """Return the raw records from the source."""
        pass

    def __repr__(self) -> str:
        # Readable form for logs and print statements
        return f"{self.__class__.__name__}(source='{self.source}')"


class CSVExtractor(DataExtractor):
    """Reads records from a CSV file."""

    def extract(self) -> list[dict[str, Any]]:
        try:
            with open(self.source, mode='r', encoding='utf-8') as f:
                return [dict(row) for row in csv.DictReader(f)]
        except FileNotFoundError:
            # Re-raise as ExtractionError so the pipeline only has to catch one error type
            raise ExtractionError(f"CSV file not found at path: {self.source}")
        except Exception as e:
            raise ExtractionError(f"Failed to read CSV file: {e}")

    def __repr__(self) -> str:
        return f"CSVExtractor(file='{self.source}')"


class APIExtractor(DataExtractor):
    """Reads records from a JSON HTTP endpoint."""

    def __init__(self, source: str, name: str | None = None, timeout: float = 10.0) -> None:
        super().__init__(source, name)
        self.timeout = timeout

    def extract(self) -> list[dict[str, Any]]:
        try:
            response = requests.get(self.source, timeout=self.timeout)
            # Turn HTTP error codes (404, 500, ...) into exceptions
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            # Connection problems, timeouts and bad status codes
            raise ExtractionError(f"API request failed: {e}")
        except ValueError:
            # Response body was not valid JSON
            raise ExtractionError("API did not return valid JSON.")
