"""
Day 3 - Incremental ETL Pipeline
Loads new JSON transaction batches into the CSV warehouse, one batch at a time.

The pipeline is idempotent: running it again with no new batches changes nothing.
"""
import csv
import json
import logging
import time
from datetime import datetime
from pathlib import Path

# Project file locations
WAREHOUSE_PATH = Path("data") / "transactions.csv"
BATCH_DIR = Path("incoming")
CHECKPOINT_PATH = Path("state") / "checkpoint.json"
LOG_PATH = Path("logs") / "etl.log"

KEY = "transaction_id"
COMPARED_FIELDS = ("customer_id", "merchant", "amount_kes", "status", "channel", "updated_at")
FIELDNAMES = [KEY, *COMPARED_FIELDS]
BUCKETS = ("new", "updated", "duplicate", "invalid")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def setup_logging(path: Path = LOG_PATH) -> logging.Logger:
    """Return a logger that writes to both the terminal and the log file."""
    path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("ETL_Logger")
    logger.setLevel(logging.INFO)

    # Drop old handlers so repeated calls don't print every line twice
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s %(levelname)-5s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    for handler in (logging.StreamHandler(), logging.FileHandler(path, mode="a", encoding="utf-8")):
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def strip_commas(amount):
    """Remove thousands separators from string amounts such as "3,450.00"."""
    return amount.replace(",", "") if isinstance(amount, str) else amount


def atomic_write(path: Path, write_fn, newline=None) -> None:
    """Write through a temp file and swap it in, so a crash never leaves a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = Path(str(path) + ".tmp")
    with open(temp_path, "w", encoding="utf-8", newline=newline) as f:
        write_fn(f)
    temp_path.replace(path)


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------

def read_checkpoint(path: Path = CHECKPOINT_PATH) -> dict:
    """Return the saved checkpoint, or a blank one if it is missing or corrupt."""
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            pass

    return {
        "last_run_at": None,
        "processed_batches": [],
        "high_watermark": None,
        "row_count": 0,
    }


def write_checkpoint(checkpoint: dict, path: Path = CHECKPOINT_PATH) -> None:
    """Save the checkpoint atomically."""
    atomic_write(path, lambda f: json.dump(checkpoint, f, indent=2))


# ---------------------------------------------------------------------------
# Warehouse
# ---------------------------------------------------------------------------

def load_existing(path: Path = WAREHOUSE_PATH) -> dict[str, dict]:
    """Read the warehouse CSV into a dict keyed by transaction_id."""
    if not path.exists():
        return {}

    existing = {}
    with open(path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            # Store amounts as floats so they compare cleanly later
            if row.get("amount_kes"):
                row["amount_kes"] = float(row["amount_kes"])
            existing[row[KEY]] = row

    return existing


def save_warehouse(state: dict[str, dict], path: Path = WAREHOUSE_PATH) -> None:
    """Write the full state back to CSV, sorted by transaction_id."""
    def write_rows(f):
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for txn_id in sorted(state):
            row = state[txn_id]
            writer.writerow({field: row.get(field) for field in FIELDNAMES})

    atomic_write(path, write_rows, newline="")


# ---------------------------------------------------------------------------
# Batches
# ---------------------------------------------------------------------------

def load_batch(path: Path) -> tuple[str, list[dict]]:
    """Read one JSON batch file and return (batch_id, transactions)."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("batch_id", "UNKNOWN"), data.get("transactions", [])
    except Exception as e:
        # Malformed batch: report it and let the caller skip it
        logging.error(f"Failed to read batch {path.name}: {e}")
        return None, []


def validate_record(record: dict) -> list[str]:
    """Return the reasons a record can't be loaded (an empty list means it is valid)."""
    reasons = []

    if not record.get(KEY):
        reasons.append("missing transaction_id")

    amount = record.get("amount_kes")
    if amount is None:
        reasons.append("missing amount")
    else:
        try:
            if float(strip_commas(amount)) < 0:
                reasons.append("negative amount")
        except ValueError:
            reasons.append("invalid amount (not a number)")

    return reasons


def comparable(field: str, value) -> str:
    """Turn a field value into a string that can be compared across sources."""
    text = str(value)
    # Amounts may arrive as 12500, 12500.0 or "12,500.00", so fix them to 2 decimals
    if field == "amount_kes" and text:
        text = f"{float(text.replace(',', '')):.2f}"
    return text


def is_same_record(old: dict, new: dict) -> bool:
    """True when every compared field matches between the two records."""
    return all(
        comparable(field, old.get(field, "")) == comparable(field, new.get(field, ""))
        for field in COMPARED_FIELDS
    )


def classify(existing: dict[str, dict], batch: list[dict]) -> dict[str, list[dict]]:
    """Sort batch records into the 'new', 'updated', 'duplicate' and 'invalid' buckets."""
    buckets = {name: [] for name in BUCKETS}
    seen_in_batch = set()

    def reject(record: dict, reason: str) -> None:
        record["_reasons"] = reason
        buckets["invalid"].append(record)

    for record in batch:
        problems = validate_record(record)
        if problems:
            reject(record, "; ".join(problems))
            continue

        txn_id = record[KEY]
        if txn_id in seen_in_batch:
            reject(record, "duplicate ID within batch")
            continue
        seen_in_batch.add(txn_id)

        old_record = existing.get(txn_id)
        if old_record is None:
            buckets["new"].append(record)
        elif is_same_record(old_record, record):
            buckets["duplicate"].append(record)
        elif record.get("updated_at", "") > old_record.get("updated_at", ""):
            buckets["updated"].append(record)
        else:
            # Changed but not newer: never let stale data overwrite the warehouse
            reject(record, "older version (updated_at is not newer)")

    return buckets


def upsert(existing: dict[str, dict], new: list[dict], updated: list[dict]) -> dict[str, dict]:
    """Return a merged copy of the state with new rows inserted and updated rows replaced."""
    merged = dict(existing)
    for record in new + updated:
        record["amount_kes"] = strip_commas(record["amount_kes"])
        merged[record[KEY]] = record
    return merged


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------

def run() -> dict:
    """Process every batch not yet in the checkpoint and return the run totals."""
    logger = setup_logging()
    start_time = time.time()
    logger.info("Run started")

    state = load_existing()
    checkpoint = read_checkpoint()
    logger.info(f"Existing rows loaded: {len(state)}")

    totals = dict.fromkeys(BUCKETS, 0)
    batches_processed = []
    batch_files = sorted(BATCH_DIR.glob("*.json")) if BATCH_DIR.exists() else []

    for file_path in batch_files:
        batch_id, transactions = load_batch(file_path)
        if not batch_id:
            continue

        if batch_id in checkpoint["processed_batches"]:
            logger.info(f"Batch {batch_id} already processed - skipping")
            continue

        logger.info(f"Batch {batch_id}: {len(transactions)} records read")
        buckets = classify(state, transactions)

        counts = {name: len(records) for name, records in buckets.items()}
        for name, count in counts.items():
            totals[name] += count
        logger.info(
            f"New: {counts['new']} | Updated: {counts['updated']} | "
            f"Duplicates: {counts['duplicate']} | Invalid: {counts['invalid']}"
        )

        for record in buckets["invalid"]:
            logger.warning(f"Invalid record {record.get(KEY, 'unknown')}: {record.get('_reasons', 'unknown error')}")

        state = upsert(state, buckets["new"], buckets["updated"])
        batches_processed.append(batch_id)

    # Save the warehouse first; the checkpoint only moves once the data is safely on disk
    save_warehouse(state)
    logger.info(f"Rows after load: {len(state)}")

    if batches_processed:
        checkpoint["processed_batches"].extend(batches_processed)
        checkpoint["last_run_at"] = datetime.now().isoformat()
        checkpoint["row_count"] = len(state)
        checkpoint["high_watermark"] = max((row.get("updated_at", "") for row in state.values()), default="")
        write_checkpoint(checkpoint)

    logger.info(f"Run finished in {round(time.time() - start_time, 3)}s")

    return {
        "new": totals["new"],
        "updated": totals["updated"],
        "duplicates": totals["duplicate"],
        "invalid": totals["invalid"],
    }


if __name__ == "__main__":
    run()
