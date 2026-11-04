"""
Transformer: checks raw records against the rules and cleans the valid ones.
"""

from typing import Any


def _parse_number(raw: Any) -> int:
    """Parse a raw value into an int, raising ValueError when it is not one."""
    # Convert to text first (CSV gives strings, JSON gives numbers), then drop
    # thousands separators so "1,150" becomes "1150".
    text = str(raw).strip().replace(",", "")
    # float() handles both "65" and "65.5"; int() then drops any decimals.
    return int(float(text))


class DataTransformer:
    """Validates and normalizes records, keeping rejected ones aside."""

    def __init__(self, required_fields: list[str]) -> None:
        """Store the field names every record must carry."""
        self.required_fields = required_fields
        # Primary-key fields for duplicate detection are tried in order.
        self.primary_key_fields = ("order_id", "id")
        # Rejections from the current transform() run are reset per run below
        # so one instance reused across pipelines stays accurate.
        self.rejected_records: list[dict[str, Any]] = []

    # Validation
    def validate(self, record: dict[str, Any]) -> list[str]:
        """Return every reason this record is invalid. An empty list means it is valid."""
        reasons: list[str] = []
        # Each helper appends readable reason strings; joining them
        # later with "; " produces one line per rejected record.
        reasons.extend(self._validate_required_fields(record))
        reasons.extend(self._validate_email(record))
        reasons.extend(self._validate_order_numbers(record))
        return reasons

    def _duplicate_reasons(
        self, record: dict[str, Any], seen_keys: set[tuple[str, str]]
    ) -> list[str]:
        """Reject duplicates with a reason instead of dropping them."""
        # Walk the candidate primary-key fields in order and use the first
        # one actually present on the record.
        for field_name in self.primary_key_fields:
            key_value = record.get(field_name)
            if key_value is None or str(key_value).strip() == "":
                continue
            # Key on (field, value) so an order and a user with the same value don't clash
            key = (field_name, str(key_value).strip())
            if key in seen_keys:
                return [f"duplicate {field_name} '{key_value}'"]
            seen_keys.add(key)
            return []
        # No primary key on the record: leave deduping to the required
        # fields rule already applied.
        return []

    def _validate_required_fields(
        self, record: dict[str, Any]
    ) -> list[str]:
        """Every required field must exist and not be blank/whitespace."""
        reasons: list[str] = []
        for field_name in self.required_fields:
            value = record.get(field_name)
            # .strip() catches fields that exist but hold only spaces,
            # which DictReader happily delivers as a non-empty value.
            if value is None or str(value).strip() == "":
                reasons.append(f"missing {field_name}")
        return reasons

    def _validate_email(self, record: dict[str, Any]) -> list[str]:
        """Ensures every email address contains an '@'."""
        for field_name in ("customer_email", "email"):
            email = record.get(field_name)
            if email:
                if "@" not in str(email):
                    return ["Invalid email format."]
        return []

    def _validate_order_numbers(self, record: dict[str, Any]) -> list[str]:
        """Check quantity and unit price are positive numbers."""
        reasons: list[str] = []

        # quantity 
        if "quantity" in record:
            try:
                quantity = _parse_number(record.get("quantity", ""))
                if quantity <= 0:
                    reasons.append("Quantity must be positive.")
            except ValueError:
                reasons.append("Invalid quantity, it must be a number.")

        # unit price 
        if "unit_price_kes" in record:
            try:
                price = _parse_number(record.get("unit_price_kes", ""))
                if price <= 0:
                    reasons.append("Price must be positive.")
            except ValueError:
                reasons.append("Invalid price, it must be a number.")

        return reasons

    # Cleaning
    def clean(self, record: dict[str, Any]) -> dict[str, Any]:
        """Return a normalized copy of a valid record."""
        cleaned = self._clean_strings(record)

        if "order_date" in cleaned:
            return self._clean_order_record(cleaned)

        # Non-order datasets (Users API): keep their own fields, but rename
        # the primary key `id` -> `order_id` so downstream consumers can
        # join orders and users on one column name.
        if "id" in cleaned:
            cleaned["order_id"] = cleaned.pop("id")
        return cleaned

    def _clean_strings(
        self, record: dict[str, Any]
    ) -> dict[str, Any]:
        """Copy the record, stripping whitespace from every text value."""
        # CSV values are always strings, but JSON can hold ints, so only
        # strings are stripped and everything else is copied unchanged.
        cleaned: dict[str, Any] = {
            key: value.strip() if isinstance(value, str) else value
            for key, value in record.items()
        }

        # City names arrive in mixed case ("kisumu", "NAIROBI"); Title Case
        # normalizes them into one consistent form.  Done only when a city
        # exists on the record.
        if "city" in cleaned:
            cleaned["city"] = cleaned["city"].title()
        return cleaned

    def _clean_order_record(
        self, cleaned: dict[str, Any]
    ) -> dict[str, Any]:
        """Finish an orders record: convert numbers and pin column shape."""
        cleaned["quantity"] = _parse_number(cleaned["quantity"])
        cleaned["unit_price_kes"] = _parse_number(cleaned["unit_price_kes"])

        # Derived metric: how much this order line is worth in KES.
        cleaned["line_total_kes"] = (
            cleaned["quantity"] * cleaned["unit_price_kes"]
        )

        # Return the columns in a fixed, documented order so the output CSV
        # is diff-friendly between runs.
        return {
            "order_id": cleaned["order_id"],
            "order_date": cleaned["order_date"],
            "customer_email": cleaned["customer_email"],
            "city": cleaned["city"],
            "product_sku": cleaned.get("product_sku", ""),
            "quantity": cleaned["quantity"],
            "unit_price_kes": cleaned["unit_price_kes"],
            "line_total_kes": cleaned["line_total_kes"],
            "status": cleaned.get("status", "UNKNOWN"),
        }

    # Orchestration
    def transform(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Process all records. Return the valid ones and stash the rest."""
        valid_records: list[dict[str, Any]] = []
        # Reset so a reused instance does not mix in rejections from a
        # previous pipeline run.
        self.rejected_records = []
        # Primary keys of records accepted so far, for duplicate detection.
        seen_keys: set[tuple[str, str]] = set()

        for record in records:
            problems = self.validate(record)
            if not problems:
                # Duplicate check runs only on records that passed every other rule
                problems = self._duplicate_reasons(record, seen_keys)
            if problems:
                # Keep the original record but attach every reason in one
                # "; " separated line for the rejected-records CSV.
                rejected = dict(record)
                rejected["reasons"] = "; ".join(problems)
                self.rejected_records.append(rejected)
            else:
                # Valid records are stored in their cleaned form.
                valid_records.append(self.clean(record))

        return valid_records
