# Day 3 - Incremental ETL Pipeline

An incremental loader that picks up JSON transaction batches from `incoming/` and merges them into a CSV warehouse at `data/transactions.csv`. A checkpoint file records which batches have already been loaded, so each batch is applied exactly once. Every record is classified before loading, and an update is only applied when it is newer than what the warehouse already holds.

Requirements: Python 3 (standard library only).

## How to run

From inside the project folder:

```bash
python incremental_etl.py
```

On each run the pipeline:

- picks up any JSON batches in `incoming/` that have not been loaded yet
- merges them into `data/transactions.csv`
- adds the loaded batch IDs to `state/checkpoint.json`
- appends a run log to `logs/etl.log`

The pipeline is idempotent: running it a second time straight after the first should change nothing. Everything printed to the terminal is also written to `logs/etl.log`.

### First run

```
2026-11-03 01:52:19 INFO  Run started
2026-11-03 01:52:19 INFO  Existing rows loaded: 20
2026-11-03 01:52:19 INFO  Batch BATCH-2026-10-21-01: 17 records read
2026-11-03 01:52:19 INFO  New: 7 | Updated: 0 | Duplicates: 6 | Invalid: 4
2026-11-03 01:52:19 WARNING Invalid record MP-50008: older version (updated_at is not newer)
2026-11-03 01:52:19 WARNING Invalid record None: missing transaction_id
2026-11-03 01:52:19 WARNING Invalid record MP-50021: duplicate ID within batch
2026-11-03 01:52:19 WARNING Invalid record MP-50026: negative amount
2026-11-03 01:52:19 INFO  Rows after load: 27
2026-11-03 01:52:19 INFO  Run finished in 0.003s
```

### Second run (nothing new to load)

```
2026-11-03 01:52:19 INFO  Run started
2026-11-03 01:52:19 INFO  Existing rows loaded: 27
2026-11-03 01:52:19 INFO  Batch BATCH-2026-10-21-01 already processed - skipping
2026-11-03 01:52:19 INFO  Rows after load: 27
2026-11-03 01:52:19 INFO  Run finished in 0.003s
```

### Run after deleting the checkpoint

```bash
rm state/checkpoint.json
python incremental_etl.py
```

```
2026-11-03 01:52:34 INFO  Run started
2026-11-03 01:52:34 INFO  Existing rows loaded: 27
2026-11-03 01:52:34 INFO  Batch BATCH-2026-10-21-01: 17 records read
2026-11-03 01:52:34 INFO  New: 0 | Updated: 0 | Duplicates: 13 | Invalid: 4
2026-11-03 01:52:34 WARNING Invalid record MP-50008: older version (updated_at is not newer)
2026-11-03 01:52:34 WARNING Invalid record None: missing transaction_id
2026-11-03 01:52:34 WARNING Invalid record MP-50021: duplicate ID within batch
2026-11-03 01:52:34 WARNING Invalid record MP-50026: negative amount
2026-11-03 01:52:34 INFO  Rows after load: 27
2026-11-03 01:52:34 INFO  Run finished in 0.003s
```

Without a checkpoint the batch is processed again, but because the records are already in the warehouse they all come out as duplicates or invalid. The row count stays at 27, so nothing is double counted.

## How records are classified

Each incoming record goes into exactly one bucket:

| Bucket | Rule |
|---|---|
| **Invalid** | Missing `transaction_id`, missing/negative/non-numeric amount, or an ID repeated within the same batch |
| **New** | The `transaction_id` does not exist in the warehouse yet |
| **Duplicate** | The ID exists and all compared fields are identical |
| **Updated** | The ID exists, some fields differ, and the incoming `updated_at` is newer |

A record that differs from the warehouse but has an older (or equal) `updated_at` is rejected as invalid, so stale data from an old batch can never overwrite fresher rows.

Amounts are normalised to two decimal places before comparison, so `12500`, `12500.0` and `"12,500.00"` are treated as the same value.

## Checkpoint

`state/checkpoint.json` holds the list of `batch_id`s already loaded; any batch listed there is skipped. It also records:

- the time of the last successful run
- the current warehouse row count
- the high watermark (latest `updated_at` in the warehouse)

If the file is missing or corrupt, the pipeline starts with an empty checkpoint.

Shape:

```json
{
  "last_run_at": "<ISO timestamp>",
  "processed_batches": ["<batch_id>", "..."],
  "high_watermark": "<latest updated_at loaded>",
  "row_count": 0
}
```

## Safe writes

Both the warehouse and the checkpoint are written to a `.tmp` file first and then swapped into place, so a crash mid-write can't leave a corrupted file behind.

The warehouse is always saved before the checkpoint. If the script stops between the two, the next run simply re-processes that batch; its records are already present, so they are classed as duplicates and the final result is still correct.

## Logging

Output goes to the terminal and is appended to `logs/etl.log`, one block per run. Each block contains:

- run start and finish (with duration)
- number of existing rows loaded
- per-batch counts of new, updated, duplicate and invalid records
- the ID and reason for every invalid record

Because the log is appended to, it grows with every run.

## Data format

Warehouse columns (sorted by `transaction_id`, one row per ID):

`transaction_id`, `customer_id`, `merchant`, `amount_kes`, `status`, `channel`, `updated_at`

All columns except `transaction_id` are compared when deciding whether a record has changed.

Incoming amounts can be numbers or strings with thousands separators (e.g. `"3,450.00"`); both are handled.

## Notes

- Each batch file needs a `batch_id` and a `transactions` array.
- A batch file with malformed JSON is logged as an error and skipped.
