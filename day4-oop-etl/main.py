"""
Day 4 - Object-Oriented ETL Framework

Entry point: sets up each pipeline and runs them one after another.
"""

import time
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any

# Import framework classes
from etl import (
    CSVExtractor, APIExtractor, DataTransformer, DataLoader, ETLError
)


@dataclass
class RunSummary:
    """Collects the numbers printed at the end of a pipeline run.

    `status` takes one of three values:
        "SUCCESS" -> everything extracted was valid and loaded
        "PARTIAL" -> some records were rejected but the run still loaded data
        "FAILED"  -> the source could not be read, nothing valid survived,
                     or the destination could not be written
    """
    pipeline_name: str
    extractor_name: str
    destination: str
    extracted: int = 0
    valid: int = 0
    rejected: int = 0
    loaded: int = 0
    duration: float = 0.0
    status: str = "SUCCESS"

    # Counts how many records were rejected for each distinct reason
    rejection_reasons: dict[str, int] = field(default_factory=dict)

    def report(self) -> str:
        """Format the console block shown for this pipeline."""
        if self.status == "FAILED":
            return (
                f"Pipeline: {self.pipeline_name}  "
                f"({self.extractor_name} -> {self.destination})\n"
                f"  Status    : FAILED"
            )

        lines = [
            f"Pipeline: {self.pipeline_name}  "
            f"({self.extractor_name} -> {self.destination})",
            f"  Extracted : {self.extracted}",
            f"  Valid     : {self.valid}",
            f"  Rejected  : {self.rejected}",
        ]
        # One line per rejection reason and how many times it happened,
        # ordered by frequency.
        for reason, count in sorted(
            self.rejection_reasons.items(), key=lambda item: -item[1]
        ):
            lines.append(f"   - {reason}: {count}")
        lines.append(f"  Loaded    : {self.loaded}")
        lines.append(f"  Duration  : {self.duration}s")
        lines.append(f"  Status    : {self.status}")
        return "\n".join(lines)


class Pipeline:
    """Runs extractor -> transformer -> loader regardless of the extractor."""

    # Toggle for pipelines that persist rejected records next to the clean
    # output.
    WRITES_REJECTS = False

    def __init__(self, name: str, extractor, transformer, loader) -> None:
        self.name = name
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        # Survivors of the last _transform() call; _load() reads them.
        self._valid_records: list[dict[str, Any]] = []

    def run(self) -> RunSummary:
        """Execute the pipeline and return a summary of what happened.

        Phase order matters. Extraction must succeed before transformation,
        and only surviving records reach the loader.
        """
        summary = RunSummary(
            pipeline_name=self.name,
            extractor_name=self.extractor.__class__.__name__,
            destination=str(self.loader.destination),
        )
        start_time = time.time()

        # 1. Extraction phase: read raw records from the source.
        raw_records: list[dict[str, Any]] | None = self._extract(summary)
        if raw_records is None:
            return summary

        # 2. Transformation phase: validate + clean, collect rejections.
        self._transform(raw_records, summary)

        # 3. Loading phase: write valid records (and rejected ones for the
        #    Orders pipeline). A loading failure also marks the run FAILED,
        #    but only after the numbers were recorded.
        self._load(summary)

        summary.duration = round(time.time() - start_time, 3)
        return summary

    def _extract(self, summary: RunSummary) -> list[dict[str, Any]] | None:
        """Run the extractor. Returns the raw records, or None on failure."""
        try:
            raw_records = self.extractor.extract()
            summary.extracted = len(raw_records)
            return raw_records
        except ETLError as exc:
            # A missing file, unreachable API or timeout fails this pipeline
            # cleanly while the other pipelines keep running.
            print(
                f"Error! Pipeline '{self.name}' failed during "
                f"extraction: {exc}."
            )
            summary.status = "FAILED"
            return None

    def _transform(
        self, raw_records: list[dict[str, Any]], summary: RunSummary
    ) -> None:
        """Validate and clean the raw records, updating the summary.

        The transformer keeps rejected records (with reasons) internally;
        both survivors and rejects are measured here.
        """
        valid_records = self.transformer.transform(raw_records)
        self._valid_records = valid_records   # _load() consumes these
        summary.valid = len(valid_records)
        summary.rejected = len(self.transformer.rejected_records)

        # Count rejections per reason so the report shows which quality
        # rule the data breaks most often.
        reason_counts: dict[str, int] = {}
        for rejected in self.transformer.rejected_records:
            # The transformer joined the reasons with "; "; split them back
            # apart to count each one individually.
            for reason in rejected["reasons"].split("; "):
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
        summary.rejection_reasons = reason_counts

        # Status roll-up:
        #   some valid + some rejected = PARTIAL (the run still delivered)
        if summary.rejected > 0 and summary.valid > 0:
            summary.status = "PARTIAL"
        #   zero valid with rejections = FAILED (nothing worth loading)
        elif summary.valid == 0 and summary.rejected > 0:
            summary.status = "FAILED"

    def _load(self, summary: RunSummary) -> None:
        """Write valid records, plus rejected ones for the Orders pipeline."""
        try:
            summary.loaded = self.loader.load(self._valid_records)
            if summary.rejected > 0 and self.WRITES_REJECTS:
                self._write_rejections()
        except ETLError as exc:
            print(f"Error! Pipeline '{self.name}' failed during loading: {exc}")
            summary.status = "FAILED"

    def _write_rejections(self) -> None:
        """Persist this pipeline's rejected records to orders_rejected.csv."""
        reject_loader = DataLoader(Path("output/orders_rejected.csv"))
        reject_loader.load(self.transformer.rejected_records)


def main():
    print("Starting ETL Framework...\n")

    # Pipeline 1: Orders CSV
    # Fields every order row must have.
    order_fields = [
        "order_id", "order_date", "customer_email", "city",
        "quantity", "unit_price_kes",
    ]

    orders_pipeline = Pipeline(
        name="Orders Ingestion",
        extractor=CSVExtractor(source="data/orders.csv", name="OrdersFile"),
        transformer=DataTransformer(required_fields=order_fields),
        loader=DataLoader(destination=Path("output/orders_clean.csv")),
    )
    # The Orders pipeline is the only one that persists rejected
    # records (to output/orders_rejected.csv) alongside the clean output.
    orders_pipeline.WRITES_REJECTS = True

    # Pipeline 2: Users API
    # Fields every API user record must have.
    user_fields = ["id", "name", "email"]

    users_pipeline = Pipeline(
        name="Users API Extraction",
        extractor=APIExtractor(
            source="https://jsonplaceholder.typicode.com/users",
            name="UsersAPI",
        ),
        transformer=DataTransformer(required_fields=user_fields),
        loader=DataLoader(destination=Path("output/users_clean.json")),
    )

    # Pipeline 3: Broken file (shows errors are handled cleanly, without
    # crashing the program).
    broken_pipeline = Pipeline(
        name="Broken Source",
        extractor=CSVExtractor(
            source="data/does_not_exist.csv", name="MissingFile"
        ),
        transformer=DataTransformer(required_fields=order_fields),
        loader=DataLoader(destination=Path("output/broken.csv")),
    )

    # Run all pipelines, printing each report as it finishes.
    pipelines = [orders_pipeline, users_pipeline, broken_pipeline]
    summaries: list[RunSummary] = []

    print("===========ETL Execution Summary===========")
    for pipeline in pipelines:
        summary = pipeline.run()
        summaries.append(summary)
        # Blank line after each report keeps the console readable.
        print(summary.report())
        print()

    # Grand totals: "succeeded" includes PARTIAL runs since data flowed.
    total_pipelines = len(summaries)
    succeeded = sum(
        1 for s in summaries if s.status in ("SUCCESS", "PARTIAL")
    )
    failed = sum(1 for s in summaries if s.status == "FAILED")

    print(
        f"Total pipelines: {total_pipelines} | "
        f"Succeeded: {succeeded} | Failed: {failed}"
    )


if __name__ == "__main__":
    main()
