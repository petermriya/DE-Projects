# Day 2 - Multi-File Transaction Pipeline

A small Python pipeline that takes the daily Soko Fresh transaction CSVs, validates every row, and splits them into clean and rejected outputs. Clean rows are merged into one consolidated file, bad rows are saved with the reason they failed, and a short text report summarises the run.

Requirements: Python 3.8+ (standard library only, no extra packages).

## How to run

From the repository root:

```bash
cd day2-multi-file-pipeline
python pipeline.py --input data/ --output output/
```

Outputs:

- `output/consolidated_transactions.csv` — all valid transactions
- `output/rejected_transactions.csv` — invalid rows plus the reason(s)
- `output/summary.txt` — counts and totals for the run

## Step by step

1. Open a terminal inside `day2-multi-file-pipeline/`.
2. Make sure the daily CSV files are in `data/`.
3. Run `python pipeline.py --input data/ --output output/`.
4. Look at the console for the run summary.
5. Open the generated files in `output/` to review the results.

Both `--input` and `--output` can point to any other folder if needed.

## Validation rules

Every row is checked for:

- empty values in any required column
- `quantity` or `unit_price` that is not a positive number
- a `transaction_id` that was already seen earlier in the run

Required columns: `transaction_id`, `date`, `branch`, `customer_id`, `product`, `quantity`, `unit_price`, `payment_method`.

If any check fails, the row goes to the rejected file. The source filename and row number are kept so the row can be traced back to the original CSV.

## Handling the Day 3 schema change

From 2026-10-03 the POS system exports `qty` instead of `quantity`. The pipeline renames `qty` back to `quantity` on load, so the Day 3 file is processed normally. If a required column is missing and no alias exists for it, that file is skipped entirely.

## Duplicates

A set of seen transaction IDs is kept for the whole run. The first occurrence of an ID is accepted, and any repeat (in the same file or a later one) is rejected as a duplicate.

Files are read in filename order, which is also date order.

## Cleaning steps

Valid rows are cleaned before being written:

- leading/trailing whitespace is removed from text fields
- `quantity` and `unit_price` are cast to integers
- a new `line_total` column is added (quantity × unit price)

Files without a `payment_ref` column are still processed; the field is just left empty.

## Rejected rows

`rejected_transactions.csv` contains the source file, row number, transaction ID and a `reasons` column. Multiple reasons are separated by semicolons, e.g. `missing customer_id; invalid quantity (not a number)`.

## Run summary

`summary.txt` reports:

- number of files found and processed
- number of files with column problems
- rows read, accepted and rejected
- rejection counts by reason
- total revenue (KES)
- timestamp of when the run completed
