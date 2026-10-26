# Simple DE Projects

This repository is a collection of projects that showcase practical data engineering skills. It is meant to capture the work I have done so far and provide a structure for future projects as I continue to build on the same foundations.

## Purpose

The aim of this repository is to show how data engineering work turns raw, messy, or fragmented information into structured outputs that can be used for analysis, reporting, and downstream business decisions.

## Current project

### [Day 1: Duka Connect Analytics](day1-api-extraction)

This is the first project in the portfolio. It focuses on a real-world scenario in which a Nairobi-based distributor needs customer records from a partner API in a flat and analysis-ready format.

The project includes the following work:

- Pulling data from a REST API
- Validating the HTTP response and JSON payload
- Reviewing the structure of the source data
- Extracting the fields needed for analysis
- Flattening nested JSON into a simple tabular format
- Exporting a cleaned CSV for joining with internal business data

## Repository layout

```text
Simple-DE-Projects/
├── README.md
├── day1-api-extraction/
│   ├── README.md
│   ├── extract_customers.py
│   ├── requirements.txt
│   └── output/
│       └── customers_clean.csv
```

## Day 1 workflow

The first project follows a simple but important data engineering flow:

1. Fetch data from the API.
2. Check that the request succeeded.
3. Validate the response body.
4. Inspect the structure of the records.
5. Pull the required customer information.
6. Flatten nested fields such as address and company details.
7. Convert the output to a pandas DataFrame.
8. Save the final result as a CSV.

## Skills demonstrated

This project shows a useful set of core data engineering capabilities:

- API extraction
- Data validation
- Handling nested JSON
- Recursive flattening
- Data cleaning and normalization
- Pandas-based transformation
- CSV output for analytics use cases

## Quick start

From the project root, run:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r day1-api-extraction/requirements.txt
python day1-api-extraction/extract_customers.py
```

The output file is generated here:

```text
day1-api-extraction/output/customers_clean.csv
```

## Roadmap

This repository is designed to grow into a broader portfolio of data engineering work. Future projects will likely cover topics such as:

- ETL pipelines
- Data quality checks
- SQL and warehouse workflows
- Data modeling
- Reporting and visualization
- Automation and workflow orchestration

## Notes

This first project is a practical starting point. It is meant to show a clear and useful workflow from raw API data to a cleaned, usable dataset that can support analysis and downstream reporting.

