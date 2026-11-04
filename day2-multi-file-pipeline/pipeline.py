"""
Day 2 - Multi-File Transaction Pipeline
Validates, cleans and consolidates daily Soko Fresh transaction files.

Run with: python pipeline.py --input data/ --output output/
"""
import csv
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Setup basic logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

REQUIRED_COLUMNS = {
    "transaction_id", "date", "branch", "customer_id",
    "product", "quantity", "unit_price", "payment_method",
}

# We will map the known alias "qty" to "quantity" based on the POS upgrade issue.
COLUMN_ALIASES = {
    "qty": "quantity"
}


def discover_files(folder: Path, pattern: str = "transactions_*.csv") -> list[Path]:
    """Return the matching CSV files in a stable (date) order using pathlib."""
    # Ensure the folder exists
    if not folder.exists() or not folder.is_dir():
        return []
    
    # Find all files matching the pattern
    files = list(folder.glob(pattern))
    
    # Sort the files by name (which starts with the date)
    files.sort()
    return files


def read_transactions(path: Path) -> tuple[list[str], list[dict]]:
    """Read one file and return (header, rows). Every row should remember its source file."""
    header = []
    rows = []
    
    # Open the file with utf-8 encoding to prevent weird character issues
    with open(path, mode='r', encoding='utf-8') as f:
        reader = csv.reader(f)
        
        try:
            # The first row is the header
            raw_header = next(reader)
            
            # Clean the header (strip whitespace and map aliases if necessary)
            header = []
            for col in raw_header:
                col = col.strip()
                if col in COLUMN_ALIASES:
                    col = COLUMN_ALIASES[col]
                header.append(col)
                
        except StopIteration:
            # If the file is empty, return empty lists
            return [], []
        
        # Read the rest of the rows
        row_num = 2 # Start at 2 because row 1 is the header
        for raw_row in reader:
            if not raw_row or all(cell.strip() == "" for cell in raw_row):
                row_num += 1
                continue # Skip completely empty lines
                
            # Combine header and row data into a dictionary
            row_dict = dict(zip(header, raw_row))
            
            # Inject metadata for tracking
            row_dict["_source_file"] = path.name
            row_dict["_row_num"] = row_num
            
            rows.append(row_dict)
            row_num += 1
            
    return header, rows


def validate_columns(header: list[str], required: set[str] = REQUIRED_COLUMNS) -> set[str]:
    """Return the set of required columns missing from the header (empty set means valid)."""
    # Convert header to a set and compare with required columns
    header_set = set(header)
    
    # Set difference finds items in required that are NOT in header
    missing = required - header_set
    return missing


def find_problems(row: dict, seen_ids: set[str]) -> list[str]:
    """Return a list of human-readable reasons this row is invalid (empty list means valid).
    
    Must cover: blank required values, non-numeric or non-positive quantity/price,
    and transaction IDs already seen in this run (including in earlier files).
    """
    problems = []
    
    # 1. Check for blank required fields
    for col in REQUIRED_COLUMNS:
        val = row.get(col, "")
        # If the value is empty or just spaces, it's a problem
        if not val or val.strip() == "":
            problems.append(f"missing {col}")
            
    # 2. Check transaction_id duplicates
    txn_id = row.get("transaction_id", "").strip()
    if txn_id and txn_id in seen_ids:
        problems.append(f"duplicate ID ({txn_id})")
        
    # 3. Check numeric fields (quantity and unit_price)
    # Only check if they aren't empty (to avoid duplicate "missing" errors)
    qty_str = row.get("quantity", "").strip()
    if qty_str:
        try:
            qty = int(qty_str)
            if qty <= 0:
                problems.append("invalid quantity (must be > 0)")
        except ValueError:
            problems.append("invalid quantity (not a number)")
            
    price_str = row.get("unit_price", "").strip()
    if price_str:
        try:
            price = int(price_str)
            if price <= 0:
                problems.append("invalid unit_price (must be > 0)")
        except ValueError:
            problems.append("invalid unit_price (not a number)")

    return problems


def clean_row(row: dict) -> dict:
    """Return a normalised copy of a valid row (types converted, whitespace stripped)."""
    cleaned = {}
    
    # 1. Strip whitespace from all keys
    for key, value in row.items():
        if isinstance(value, str):
            cleaned[key] = value.strip()
        else:
            cleaned[key] = value
            
    # 2. Convert numeric fields to integers
    cleaned["quantity"] = int(cleaned["quantity"])
    cleaned["unit_price"] = int(cleaned["unit_price"])
    
    # 3. Compute line_total
    cleaned["line_total"] = cleaned["quantity"] * cleaned["unit_price"]
    
    # 4. Handle optional payment_ref (ensure it exists, even if blank)
    if "payment_ref" not in cleaned:
        cleaned["payment_ref"] = ""
        
    return cleaned


def write_csv(rows: list[dict], path: Path, fieldnames: list[str]) -> None:
    """Write rows to CSV, creating parent folders as needed."""
    # Make sure the output directory exists
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # Use csv.DictWriter to handle the dictionaries easily
    with open(path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in rows:
            # Create a filtered copy of the row to only include the requested fieldnames
            # This prevents errors if the row has extra metadata like _row_num
            filtered_row = {k: v for k, v in row.items() if k in fieldnames}
            writer.writerow(filtered_row)


def build_summary(stats: dict) -> str:
    """Format the processing summary shown in the expected-output resource."""
    lines = []
    lines.append("=== Processing summary ===")
    lines.append(f"Files discovered:        {stats['files_discovered']}")
    lines.append(f"Files processed:         {stats['files_processed']}")
    
    # Format the missing columns if any exist
    if stats["files_with_issues"] > 0:
        issue_str = ", ".join([f"{f}: missing {c}" for f, c in stats["column_issues"]])
        lines.append(f"Files with column issues:{stats['files_with_issues']}  ({issue_str})")
    else:
        lines.append(f"Files with column issues:{stats['files_with_issues']}")
        
    lines.append(f"Rows read:               {stats['rows_read']}")
    lines.append(f"Rows accepted:           {stats['rows_accepted']}")
    lines.append(f"Rows rejected:           {stats['rows_rejected']}")
    
    # Breakdown of rejection reasons
    lines.append(f"  - missing values:      {stats['rej_missing']}")
    lines.append(f"  - invalid numbers:     {stats['rej_invalid_num']}")
    lines.append(f"  - duplicate IDs:       {stats['rej_dupes']}")
    
    lines.append(f"Total revenue (KES):     {stats['total_revenue']:,}")
    lines.append(f"Run finished at:         {datetime.now().isoformat()}")
    
    return "\n".join(lines)


def run_pipeline(input_dir: Path, output_dir: Path) -> dict:
    """Run every step and return the statistics used by build_summary()."""
    
    # Initialize statistics dictionary
    stats = {
        "files_discovered": 0, "files_processed": 0, "files_with_issues": 0,
        "column_issues": [], "rows_read": 0, "rows_accepted": 0, "rows_rejected": 0,
        "rej_missing": 0, "rej_invalid_num": 0, "rej_dupes": 0,
        "total_revenue": 0
    }
    
    accepted_rows = []
    rejected_rows = []
    seen_ids = set() # Keeps track of transaction IDs across all files
    
    # 1. Discover Files
    files = discover_files(input_dir)
    stats["files_discovered"] = len(files)
    
    if not files:
        logging.warning("No files found to process.")
        return stats
        
    for file_path in files:
        # 2. Read File
        header, rows = read_transactions(file_path)
        if not header:
            continue
            
        # 3. Validate Columns
        missing_cols = validate_columns(header)
        if missing_cols:
            stats["files_with_issues"] += 1
            missing_str = ", ".join(sorted(list(missing_cols)))
            stats["column_issues"].append((file_path.name, missing_str))
            logging.warning(f"Skipping {file_path.name} due to missing columns.")
            continue # Reject the whole file if columns are wrong
            
        stats["files_processed"] += 1
        
        # 4 & 5. Validate Rows and Clean Data
        for row in rows:
            stats["rows_read"] += 1
            
            # Find problems
            problems = find_problems(row, seen_ids)
            
            if problems:
                # Row is REJECTED
                stats["rows_rejected"] += 1
                
                # Tally reasons for the summary
                if any("missing" in p for p in problems): stats["rej_missing"] += 1
                if any("invalid" in p for p in problems): stats["rej_invalid_num"] += 1
                if any("duplicate" in p for p in problems): stats["rej_dupes"] += 1
                
                # Create rejected record (convert all values to string for CSV safety)
                rej_record = dict(row)
                rej_record["reasons"] = "; ".join(problems)
                rejected_rows.append(rej_record)
                
            else:
                # Row is ACCEPTED
                stats["rows_accepted"] += 1
                
                # Add to seen IDs so subsequent files catch duplicates
                seen_ids.add(row["transaction_id"].strip())
                
                # Clean row
                clean = clean_row(row)
                accepted_rows.append(clean)
                
                # Accumulate revenue
                stats["total_revenue"] += clean["line_total"]

    # 6. Write Consolidated CSV
    consolidated_fields = [
        "transaction_id", "date", "branch", "customer_id", "product", 
        "quantity", "unit_price", "line_total", "payment_method", "payment_ref", "source_file"
    ]
    
    # Rename _source_file to source_file for the final output
    for row in accepted_rows:
        row["source_file"] = row.pop("_source_file")
        
    consolidated_path = output_dir / "consolidated_transactions.csv"
    write_csv(accepted_rows, consolidated_path, consolidated_fields)
    
    # 7. Write Rejected CSV
    # Dynamically determine fields: metadata + original columns + reasons
    rejected_fields = ["source_file", "row_number", "transaction_id", "reasons"]
    
    # Add any other original columns found (like date, branch, etc.) except metadata
    for row in rejected_rows:
        for key in row.keys():
            if not key.startswith("_") and key not in rejected_fields:
                rejected_fields.append(key)
                
    # Prepare rejected rows for output (rename metadata keys)
    for row in rejected_rows:
        row["source_file"] = row.pop("_source_file")
        row["row_number"] = row.pop("_row_num")
        
    rejected_path = output_dir / "rejected_transactions.csv"
    write_csv(rejected_rows, rejected_path, rejected_fields)

    # 8. Summary
    summary_text = build_summary(stats)
    summary_path = output_dir / "summary.txt"
    
    # Ensure output dir exists before writing summary
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(summary_text)
        
    return stats


if __name__ == "__main__":
    # Setup command line argument parsing
    parser = argparse.ArgumentParser(description="Process daily transaction CSV files.")
    parser.add_argument("--input", type=Path, default=Path("data"), help="Input folder containing CSV files")
    parser.add_argument("--output", type=Path, default=Path("output"), help="Output folder for results")
    
    args = parser.parse_args()
    
    print(f"Starting pipeline...")
    print(f"Input directory:  {args.input.resolve()}")
    print(f"Output directory: {args.output.resolve()}\n")
    
    # Run the pipeline
    final_stats = run_pipeline(args.input, args.output)
    
    # Print summary to console
    print("\n" + build_summary(final_stats))
    print("\nPipeline complete.")