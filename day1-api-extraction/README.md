# Day 1 - API Data Extraction and Transformation

Day 1 of this project includes the fetching of user data from the JSONPlaceholder REST API, flattens the nested JSON structure dynamically, and exports it as a clean CSV ready for analysis.

## Approach & Assumptions

1. **Error Handling (`fetch_users`)**: 
   - Used `try/except` blocks to catch `ConnectionError`, `Timeout`, and invalid JSON.
   - Checked `response.status_code == 200` before attempting to parse the body.
   - On failure, the script prints a clear error message and uses `sys.exit(1)` to terminate gracefully without a raw traceback.
2. **Structure Description (`describe_structure`)**: 
   - Iterated through the first record's items. Checked for `dict` instances to identify nested keys (e.g., `address`, `geo`, `company`) and printed their types.
3. **Missing Fields (`extract_core_fields`)**: 
   - Assumption: If a nested field (like `city` or `company_name`) is missing, it defaults to `"Unknown"` using chained `.get()` methods instead of failing.
4. **Flattening (`flatten_user`)**: 
   - Used a recursive function to handle any depth of nesting. Keys are joined with underscores (`_`). This ensures it works for new, unseen nested data.
5. **DataFrame & Export (`to_dataframe` & `export_csv`)**: 
   - Cast `address_geo_lat` and `address_geo_lng` to floats.
   - Renamed `id` to `customer_id`.
   - Ensured the output directory is created if it doesn't exist. Used `to_csv(index=False)` to avoid the pandas index column in the output.
