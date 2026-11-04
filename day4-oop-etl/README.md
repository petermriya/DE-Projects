# Day 4 - Object-Oriented ETL Framework

A lightweight ETL framework built from reusable Python classes. It pulls orders from a local CSV file and users from a public REST API, validates and cleans both, and writes the results to `output/`. Records that fail validation are kept in a separate file along with the reason they were rejected. Each pipeline is isolated, so if one source fails the others still finish.

## Getting started

```bash
cd day4-oop-etl
pip install -r requirements.txt
python main.py
```

The only external dependency is `requests`. Output files are overwritten on every run, so the script can be re-run as many times as needed.

## Pipelines

`main()` sets up three pipelines and runs them one after another:

| Pipeline | Reads from | Writes to |
|---|---|---|
| Orders Ingestion | `data/orders.csv` | `output/orders_clean.csv` |
| Users API Extraction | `jsonplaceholder.typicode.com/users` | `output/users_clean.json` |
| Broken Source | `data/does_not_exist.csv` | — (fails on purpose) |

The Broken Source pipeline points at a file that doesn't exist. It is there to show error handling in action: the pipeline is marked FAILED, the error is printed, and the program carries on with the rest.

### Expected console output

```
===========ETL Execution Summary===========
Pipeline: Orders Ingestion  (CSVExtractor -> output/orders_clean.csv)
  Extracted : 25
  Valid     : 20
  Rejected  : 5
   - missing customer_email: 1
   - Invalid quantity, it must be a number.: 1
   - Price must be positive.: 1
   - Invalid email format.: 1
   - duplicate order_id 'ORD-7004': 1
  Loaded    : 20
  Duration  : 0.001s
  Status    : PARTIAL

Pipeline: Users API Extraction  (APIExtractor -> output/users_clean.json)
  Extracted : 10
  Valid     : 10
  Rejected  : 0
  Loaded    : 10
  Duration  : 0.133s
  Status    : SUCCESS

Error! Pipeline 'Broken Source' failed during extraction: CSV file not found at path: data/does_not_exist.csv.
Pipeline: Broken Source  (CSVExtractor -> output/broken.csv)
  Status    : FAILED

Total pipelines: 3 | Succeeded: 2 | Failed: 1
```

In short: 25 orders come in, 20 pass and 5 are rejected (one of them a repeat of `ORD-7004`). All 10 API users load successfully, and the broken pipeline fails as intended.

## Output files

| File | What's inside |
|---|---|
| `output/orders_clean.csv` | Valid orders, with an extra `line_total_kes` column |
| `output/orders_rejected.csv` | Rejected orders plus a `reasons` column (only created when something was rejected) |
| `output/users_clean.json` | Valid users from the API, with `id` renamed to `order_id` |

## Project structure

All framework code lives in the `etl/` package, with one responsibility per module:

- **`errors.py`** — custom exceptions, all inheriting from `ETLError`. The pipeline only catches `ETLError`, so new error types can be added later without touching pipeline code.
- **`extractors.py`** — `DataExtractor` is the abstract base class; every subclass must implement `extract()`. `CSVExtractor` reads a local file and `APIExtractor` calls an HTTP endpoint (10-second timeout). Any read failure is re-raised as `ExtractionError`.
- **`transformer.py`** — `DataTransformer` checks every record for required fields, then validates numbers and email addresses. Clean records are returned; bad ones are collected in `rejected_records` with a `reasons` field. A bad row never raises an exception, since one bad row shouldn't stop the whole pipeline.
- **`loader.py`** — `DataLoader` writes CSV or JSON depending on the destination's extension. An unsupported extension raises `LoadingError` as soon as the loader is created, before any data is processed. `load()` returns how many records were written.
- **`main.py`** — `Pipeline` connects an extractor, transformer and loader and runs the extract → transform → load phases. Each phase handles its own errors and fills in a `RunSummary`, which is printed to the console.

`Pipeline` doesn't care which kind of extractor or loader it is given; it just calls the same methods on them. That's the main benefit of the class design: the actual source and destination are decided in `main()`, not hard-coded into the pipeline.

## Order validation rules

An order is rejected if any of the following apply:

- a required field is missing or blank (`order_id`, `order_date`, `customer_email`, `city`, `quantity`, `unit_price_kes`)
- `customer_email` doesn't contain an `@`
- `quantity` isn't a number, or is zero/negative
- `unit_price_kes` isn't a number, or is zero/negative
- the `order_id` has already appeared earlier in the run (the first one is kept, later copies are rejected as duplicates)

Numbers written with thousands separators, like `"1,150"`, are accepted; the commas are stripped before parsing.

During cleaning, whitespace is trimmed, city names are converted to Title Case, and a `line_total_kes` column (quantity × unit price) is added to each order.

Rejected orders are saved to `output/orders_rejected.csv` exactly as they arrived, plus a `reasons` column. At the moment only the Orders pipeline writes a rejects file.

## Design decisions

- **Extending sources:** a new data source only needs a new extractor subclass and a new pipeline in `main()`; nothing else changes.
- **Fail early on bad destinations:** `DataLoader` accepts only `.csv` and `.json`. Anything else raises `LoadingError` when the pipeline is built, not halfway through a run.
- **Predictable API data:** `APIExtractor` expects the endpoint to return a JSON list, so the transformer always gets something it can iterate over.
- **One shared transformer:** order-specific checks only run when those fields exist, which lets the users pipeline reuse the same `DataTransformer` class.
- **Rejects file is opt-in:** only the Orders pipeline writes rejected records. Any other pipeline can do the same by setting `Pipeline.WRITES_REJECTS = True`.
- **Run status:** `RunSummary.status` is one of:
  - `SUCCESS` — every extracted record was valid and loaded
  - `PARTIAL` — some records were rejected, but valid data was still loaded
  - `FAILED` — the source couldn't be read, no valid records remained, or the output couldn't be written
