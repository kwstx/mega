#!/usr/bin/env python3
import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class Paths:
    input_path: Path
    output_path: Path


def now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_records(input_path: Path) -> list[dict[str, Any]]:
    if not input_path.exists():
        return []
    with input_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    records = payload.get("records", [])
    return records if isinstance(records, list) else []


def parse_hourly_row(record: dict[str, Any], usage: dict[str, Any]) -> tuple[pd.Timestamp, float] | None:
    raw_kwh = usage.get("kwh", usage.get("consumption_kwh", usage.get("usage_kwh")))
    if raw_kwh is None:
        return None
    try:
        kwh = float(raw_kwh)
    except (TypeError, ValueError):
        return None

    ts_value = usage.get("timestamp") or usage.get("datetime")
    if ts_value:
        ts = pd.to_datetime(ts_value, errors="coerce", utc=True)
        if pd.notna(ts):
            return ts, kwh

    date_value = record.get("data", {}).get("bill_date") or record.get("ingested_at")
    base_ts = pd.to_datetime(date_value, errors="coerce", utc=True)
    if pd.isna(base_ts):
        return None
    hour_value = usage.get("hour")
    hour = pd.to_numeric(hour_value, errors="coerce")
    if pd.isna(hour):
        return None
    return base_ts.normalize() + pd.to_timedelta(int(hour), unit="h"), kwh


def build_frames(records: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    usage_rows: list[dict[str, Any]] = []
    hourly_rows: list[dict[str, Any]] = []

    for record in records:
        data = record.get("data", {}) if isinstance(record.get("data"), dict) else {}
        date_value = data.get("bill_date") or record.get("ingested_at")
        ts = pd.to_datetime(date_value, errors="coerce", utc=True)
        if pd.notna(ts):
            total_kwh = pd.to_numeric(data.get("total_kwh_consumed"), errors="coerce")
            usage_rows.append(
                {
                    "timestamp": ts,
                    "kwh": float(total_kwh) if pd.notna(total_kwh) else None,
                }
            )

        hourly_usage = data.get("hourly_usage")
        if isinstance(hourly_usage, list):
            for item in hourly_usage:
                if not isinstance(item, dict):
                    continue
                parsed = parse_hourly_row(record, item)
                if parsed is None:
                    continue
                point_ts, point_kwh = parsed
                hourly_rows.append({"timestamp": point_ts, "kwh": point_kwh})

    usage_df = pd.DataFrame(usage_rows)
    hourly_df = pd.DataFrame(hourly_rows)
    return usage_df, hourly_df


def to_records(df: pd.DataFrame, date_format: str | None = None) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        period_value = row["period"]
        if date_format:
            period = period_value.strftime(date_format)
        else:
            period = str(period_value)
        output.append(
            {
                "period": period,
                "total_kwh": round(float(row["total_kwh"]), 3),
            }
        )
    return output


def aggregate_usage(usage_df: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    if usage_df.empty:
        return [], [], []

    frame = usage_df.dropna(subset=["timestamp", "kwh"]).copy()
    if frame.empty:
        return [], [], []
    frame = frame.set_index("timestamp").sort_index()

    daily = frame.resample("D")["kwh"].sum().reset_index(name="total_kwh")
    daily["period"] = daily["timestamp"].dt.date
    daily = daily[["period", "total_kwh"]]

    weekly = frame.resample("W-MON")["kwh"].sum().reset_index(name="total_kwh")
    weekly["period"] = weekly["timestamp"].dt.strftime("%Y-W%V")
    weekly = weekly[["period", "total_kwh"]]

    monthly = frame.resample("MS")["kwh"].sum().reset_index(name="total_kwh")
    monthly["period"] = monthly["timestamp"].dt.strftime("%Y-%m")
    monthly = monthly[["period", "total_kwh"]]

    return to_records(daily, "%Y-%m-%d"), to_records(weekly), to_records(monthly)


def classify_period(hour: int) -> str:
    if 0 <= hour <= 5:
        return "night"
    if 6 <= hour <= 11:
        return "morning"
    if 12 <= hour <= 17:
        return "afternoon"
    return "evening"


def aggregate_hourly(hourly_df: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    if hourly_df.empty:
        return [], [], []

    frame = hourly_df.dropna(subset=["timestamp", "kwh"]).copy()
    if frame.empty:
        return [], [], []
    frame["hour"] = frame["timestamp"].dt.hour
    hour_avg = frame.groupby("hour", as_index=False)["kwh"].mean().rename(columns={"kwh": "avg_kwh"})
    hour_avg["avg_kwh"] = hour_avg["avg_kwh"].round(3)

    average_per_hour = [
        {"hour": int(row["hour"]), "avg_kwh": float(row["avg_kwh"])}
        for _, row in hour_avg.sort_values("hour").iterrows()
    ]

    peak = hour_avg.sort_values(["avg_kwh", "hour"], ascending=[False, True]).head(3)
    peak_hours = [
        {"hour": int(row["hour"]), "avg_kwh": float(row["avg_kwh"])}
        for _, row in peak.iterrows()
    ]

    hour_avg["period"] = hour_avg["hour"].apply(classify_period)
    period_avg = hour_avg.groupby("period", as_index=False)["avg_kwh"].mean()
    period_avg["avg_kwh"] = period_avg["avg_kwh"].round(3)
    low_periods = period_avg.sort_values(["avg_kwh", "period"], ascending=[True, True]).head(2)
    low_usage_periods = [
        {"period": str(row["period"]), "avg_kwh": float(row["avg_kwh"])}
        for _, row in low_periods.iterrows()
    ]

    return average_per_hour, peak_hours, low_usage_periods


def write_output(paths: Paths, payload: dict[str, Any]) -> None:
    paths.output_path.parent.mkdir(parents=True, exist_ok=True)
    with paths.output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Aggregate normalized electricity data with pandas.")
    parser.add_argument("--input", default="data/bill_store.json", help="Input JSON file with normalized records.")
    parser.add_argument("--output", default="data/bill_aggregates.json", help="Output JSON file for aggregates.")
    args = parser.parse_args()

    paths = Paths(input_path=Path(args.input), output_path=Path(args.output))
    records = load_records(paths.input_path)
    usage_df, hourly_df = build_frames(records)
    daily, weekly, monthly = aggregate_usage(usage_df)
    hourly_avg, peak_hours, low_periods = aggregate_hourly(hourly_df)

    payload = {
        "metadata": {
            "generated_at": now_iso(),
            "source_file": str(paths.input_path),
            "total_records": len(records),
            "contains_hourly_usage": not hourly_df.empty,
        },
        "aggregates": {
            "daily_usage_kwh": daily,
            "weekly_usage_kwh": weekly,
            "monthly_usage_kwh": monthly,
            "average_consumption_per_hour_kwh": hourly_avg,
            "peak_hours": peak_hours,
            "low_usage_periods": low_periods,
        },
    }
    write_output(paths, payload)
    print(json.dumps({"ok": True, "output": str(paths.output_path)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
