#!/usr/bin/env python3
import csv
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

try:
    import pdfplumber  # type: ignore
except Exception:
    pdfplumber = None

try:
    from PyPDF2 import PdfReader  # type: ignore
except Exception:
    PdfReader = None


DATE_KEYWORDS = ("date", "bill date", "invoice date", "statement date", "period end")
TOTAL_KWH_KEYWORDS = ("total kwh", "kwh consumed", "consumption", "total consumption")
COST_KEYWORDS = ("total amount", "amount due", "total cost", "bill amount", "cost")
APPLIANCE_KEYS = ("appliance", "device", "item", "category")
KWH_KEYS = ("kwh", "consumption_kwh", "usage_kwh", "energy")
APPLIANCE_COST_KEYS = ("cost", "amount", "charge")


def normalize_date(value: str) -> str | None:
    text = value.strip()
    if not text:
        return None
    patterns = (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d-%m-%Y",
        "%m-%d-%Y",
        "%b %d, %Y",
        "%B %d, %Y",
    )
    for pattern in patterns:
        try:
            return datetime.strptime(text, pattern).date().isoformat()
        except ValueError:
            continue
    match = re.search(r"(\d{4}-\d{2}-\d{2}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", text)
    if not match:
        return None
    candidate = match.group(1).replace("-", "/")
    for pattern in ("%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(candidate, pattern).date().isoformat()
        except ValueError:
            continue
    return None


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    clean = re.sub(r"[^0-9.\-]", "", text.replace(",", ""))
    if clean in ("", "-", ".", "-."):
        return None
    try:
        return float(clean)
    except ValueError:
        return None


def build_base_result(file_path: Path) -> dict[str, Any]:
    return {
        "source_file": file_path.name,
        "file_type": file_path.suffix.lower().lstrip("."),
        "extracted_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "data": {
            "date": None,
            "total_kwh_consumed": None,
            "cost": None,
            "currency": None,
            "appliance_usage": [],
            "issues": [],
        },
    }


def append_issue(result: dict[str, Any], issue: str) -> None:
    result["data"]["issues"].append(issue)


def find_first_value(row: dict[str, Any], candidates: tuple[str, ...]) -> Any:
    lowered = {str(key).strip().lower(): value for key, value in row.items() if key is not None}
    for candidate in candidates:
        for key, value in lowered.items():
            if candidate in key:
                return value
    return None


def find_first_value_excluding(row: dict[str, Any], candidates: tuple[str, ...], excludes: tuple[str, ...]) -> Any:
    lowered = {str(key).strip().lower(): value for key, value in row.items() if key is not None}
    for candidate in candidates:
        for key, value in lowered.items():
            if candidate in key and all(exclude not in key for exclude in excludes):
                return value
    return None


def parse_csv(file_path: Path) -> dict[str, Any]:
    result = build_base_result(file_path)
    data = result["data"]
    total_from_rows = 0.0
    cost_from_rows = 0.0
    appliance_rows = 0
    header_seen = False

    with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(2048)
        handle.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample) if sample else csv.excel
        except csv.Error:
            dialect = csv.excel

        reader = csv.DictReader(handle, dialect=dialect)
        if reader.fieldnames:
            header_seen = True

        for index, row in enumerate(reader, start=2):
            if not row:
                continue

            date_candidate = find_first_value(row, DATE_KEYWORDS)
            if data["date"] is None and date_candidate:
                parsed_date = normalize_date(str(date_candidate))
                if parsed_date:
                    data["date"] = parsed_date

            kwh_candidate = find_first_value(row, TOTAL_KWH_KEYWORDS)
            cost_candidate = find_first_value(row, COST_KEYWORDS)
            appliance_name = find_first_value(row, APPLIANCE_KEYS)
            appliance_kwh = find_first_value_excluding(row, KWH_KEYS, ("total", "consumed", "consumption"))
            appliance_cost = find_first_value_excluding(row, APPLIANCE_COST_KEYS, ("total", "amount", "due", "bill"))

            parsed_total_kwh = parse_number(kwh_candidate)
            parsed_total_cost = parse_number(cost_candidate)

            if data["total_kwh_consumed"] is None and parsed_total_kwh is not None:
                data["total_kwh_consumed"] = parsed_total_kwh
            if data["cost"] is None and parsed_total_cost is not None:
                data["cost"] = parsed_total_cost

            if appliance_name:
                parsed_appliance_kwh = parse_number(appliance_kwh)
                parsed_appliance_cost = parse_number(appliance_cost)
                data["appliance_usage"].append(
                    {
                        "appliance": str(appliance_name).strip(),
                        "kwh": parsed_appliance_kwh,
                        "cost": parsed_appliance_cost,
                    }
                )
                if parsed_appliance_kwh is not None:
                    total_from_rows += parsed_appliance_kwh
                if parsed_appliance_cost is not None:
                    cost_from_rows += parsed_appliance_cost
                appliance_rows += 1

            if appliance_name and appliance_kwh and parse_number(appliance_kwh) is None:
                append_issue(result, f"Row {index}: malformed appliance kWh value '{appliance_kwh}'.")
            if appliance_name and appliance_cost and parse_number(appliance_cost) is None:
                append_issue(result, f"Row {index}: malformed appliance cost value '{appliance_cost}'.")

    if not header_seen:
        append_issue(result, "CSV has no recognizable header row.")
    if data["total_kwh_consumed"] is None and appliance_rows:
        data["total_kwh_consumed"] = round(total_from_rows, 3)
        append_issue(result, "Total kWh inferred from appliance rows.")
    if data["cost"] is None and appliance_rows and cost_from_rows > 0:
        data["cost"] = round(cost_from_rows, 2)
        append_issue(result, "Cost inferred from appliance rows.")
    if data["date"] is None:
        append_issue(result, "Billing date was not found.")
    if data["total_kwh_consumed"] is None:
        append_issue(result, "Total kWh consumed was not found.")
    if data["cost"] is None:
        append_issue(result, "Total cost was not found.")

    return result


def extract_pdf_text(file_path: Path) -> str:
    if pdfplumber is not None:
        pages: list[str] = []
        with pdfplumber.open(str(file_path)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                pages.append(text)
        return "\n".join(pages)

    if PdfReader is not None:
        reader = PdfReader(str(file_path))
        return "\n".join([(page.extract_text() or "") for page in reader.pages])

    raise RuntimeError("No PDF parser available. Install pdfplumber or PyPDF2.")


def parse_pdf(file_path: Path) -> dict[str, Any]:
    result = build_base_result(file_path)
    data = result["data"]
    text = extract_pdf_text(file_path)

    currency_match = re.search(r"([$€£])", text)
    if currency_match:
        symbol = currency_match.group(1)
        data["currency"] = {"$": "USD", "€": "EUR", "£": "GBP"}.get(symbol)

    date_patterns = [
        r"(?:bill(?:ing)?|invoice|statement)\s*date[:\s]+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2})",
        r"(?:period\s*end|service\s*to)[:\s]+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2})",
    ]
    for pattern in date_patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            parsed_date = normalize_date(match.group(1))
            if parsed_date:
                data["date"] = parsed_date
                break

    total_kwh_match = re.search(
        r"(?:total\s+kwh|kwh\s+consumed|total\s+consumption)[^\d\-]*([0-9][0-9,]*(?:\.[0-9]+)?)",
        text,
        flags=re.IGNORECASE,
    )
    if total_kwh_match:
        data["total_kwh_consumed"] = parse_number(total_kwh_match.group(1))

    total_cost_match = re.search(
        r"(?:total\s+amount|amount\s+due|total\s+cost|bill\s+amount)[^0-9\-]*([$€£]?\s*[0-9][0-9,]*(?:\.[0-9]+)?)",
        text,
        flags=re.IGNORECASE,
    )
    if total_cost_match:
        data["cost"] = parse_number(total_cost_match.group(1))

    for line in text.splitlines():
        appliance_match = re.search(
            r"^\s*([A-Za-z][A-Za-z0-9 \-_/]{1,40})\s+([0-9][0-9,]*(?:\.[0-9]+)?)\s*kwh(?:\s+([$€£]?\s*[0-9][0-9,]*(?:\.[0-9]+)?))?\s*$",
            line,
            flags=re.IGNORECASE,
        )
        if appliance_match:
            data["appliance_usage"].append(
                {
                    "appliance": appliance_match.group(1).strip(),
                    "kwh": parse_number(appliance_match.group(2)),
                    "cost": parse_number(appliance_match.group(3)),
                }
            )

    if data["total_kwh_consumed"] is None and data["appliance_usage"]:
        appliance_total = sum((entry.get("kwh") or 0) for entry in data["appliance_usage"])
        data["total_kwh_consumed"] = round(appliance_total, 3)
        append_issue(result, "Total kWh inferred from appliance lines in PDF.")
    if data["cost"] is None and data["appliance_usage"]:
        appliance_cost_total = sum((entry.get("cost") or 0) for entry in data["appliance_usage"])
        if appliance_cost_total > 0:
            data["cost"] = round(appliance_cost_total, 2)
            append_issue(result, "Cost inferred from appliance lines in PDF.")
    if data["date"] is None:
        append_issue(result, "Billing date was not found in PDF.")
    if data["total_kwh_consumed"] is None:
        append_issue(result, "Total kWh consumed was not found in PDF.")
    if data["cost"] is None:
        append_issue(result, "Total cost was not found in PDF.")

    return result


def main() -> int:
    if len(sys.argv) != 2:
        print(json.dumps({"error": "Usage: extract_bill_data.py <file_path>"}))
        return 1

    file_path = Path(sys.argv[1])
    if not file_path.exists() or not file_path.is_file():
        print(json.dumps({"error": f"File not found: {file_path}"}))
        return 1

    extension = file_path.suffix.lower()
    try:
        if extension == ".csv":
            result = parse_csv(file_path)
        elif extension == ".pdf":
            result = parse_pdf(file_path)
        else:
            print(json.dumps({"error": f"Unsupported file extension: {extension}"}))
            return 1
    except UnicodeDecodeError:
        print(json.dumps({"error": "Unable to decode file contents. Ensure file encoding is UTF-8 compatible."}))
        return 1
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        return 1

    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
