#!/usr/bin/env python3
"""
Build a simple global fuel-cell-trade monitor JSON from UN Comtrade annual data.

Default HS codes:
- 850680 : Primary cells and primary batteries, other
- 850690 : Parts of primary cells and primary batteries

Notes:
- This is a pragmatic market-proxy index, not a perfect "fuel cell only" series.
- It prefers an official UN Comtrade API key from UN_COMTRADE_API_KEY, but will
  still attempt a request without one.
- Output JSON is designed for frontend charting/debugging.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

API_BASE = "https://comtradeapi.worldbank.org/data/v1/get/C/A/HS"
DEFAULT_CODES = ["850680", "850690"]
DEFAULT_START = 2019
TIMEOUT = 45


class AgentError(RuntimeError):
    pass


@dataclass
class YearPoint:
    year: int
    value: float
    index: float
    yoy: Optional[float]


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def get_first(d: Dict[str, Any], keys: Iterable[str], default: Any = None) -> Any:
    for key in keys:
        if key in d and d[key] not in (None, ""):
            return d[key]
    return default


def extract_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]

    if not isinstance(payload, dict):
        return []

    for key in ("data", "Data", "dataset", "results", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]

    # Some APIs return a dict with nested lists.
    for value in payload.values():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return [row for row in value if isinstance(row, dict)]

    return []


def fetch_comtrade_rows(
    hs_code: str,
    start_year: int,
    end_year: int,
    api_key: Optional[str] = None,
    max_count: int = 500,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    params = {
        "reporter": "all",
        "partner": "0",
        "flow": "M,X",
        "cmdCode": hs_code,
        "max": str(max_count),
        "fmt": "json",
        "period": ",".join(str(y) for y in range(start_year, end_year + 1)),
    }

    headers = {"Accept": "application/json"}
    if api_key:
        headers["Ocp-Apim-Subscription-Key"] = api_key

    response = requests.get(API_BASE, params=params, headers=headers, timeout=TIMEOUT)
    response.raise_for_status()

    try:
        payload = response.json()
    except Exception as exc:  # pragma: no cover
        raise AgentError(f"UN Comtrade returned non-JSON for HS {hs_code}: {exc}") from exc

    rows = extract_rows(payload)
    meta = {
        "requested_url": response.url,
        "status_code": response.status_code,
        "row_count": len(rows),
        "top_level_keys": list(payload.keys())[:20] if isinstance(payload, dict) else [],
    }
    return rows, meta


def aggregate_years(rows: List[Dict[str, Any]], hs_code: str) -> Tuple[Dict[int, float], Dict[str, Any]]:
    by_year: Dict[int, float] = defaultdict(float)
    sample_rows: List[Dict[str, Any]] = []
    used_rows = 0
    skipped_rows = 0

    for row in rows:
        period = get_first(row, ["period", "Period", "refYear", "yr"])
        trade_value = safe_float(
            get_first(row, ["primaryValue", "PrimaryValue", "tradeValue", "TradeValue", "fobvalue", "cifvalue"])
        )

        if period is None or trade_value is None:
            skipped_rows += 1
            continue

        try:
            year = int(str(period)[:4])
        except ValueError:
            skipped_rows += 1
            continue

        # Only use imports/exports if present; skip re-exports/re-imports when explicit.
        flow_desc = str(get_first(row, ["flowDesc", "FlowDesc", "flow", "rgDesc", "cmdDesc"], "")).lower()
        if any(x in flow_desc for x in ["re-export", "re export", "re-import", "re import"]):
            skipped_rows += 1
            continue

        by_year[year] += trade_value
        used_rows += 1
        if len(sample_rows) < 5:
            sample_rows.append(
                {
                    "period": period,
                    "value": trade_value,
                    "flow": get_first(row, ["flowDesc", "FlowDesc", "flow", "rgDesc"]),
                    "reporter": get_first(row, ["reporterDesc", "ReporterDesc", "reporterISO", "rtTitle"]),
                    "partner": get_first(row, ["partnerDesc", "PartnerDesc", "partnerISO", "ptTitle"]),
                    "hs_code": hs_code,
                }
            )

    debug = {
        "used_rows": used_rows,
        "skipped_rows": skipped_rows,
        "sample_rows": sample_rows,
    }
    return dict(sorted(by_year.items())), debug


def make_index(series_by_year: Dict[int, float]) -> List[YearPoint]:
    if not series_by_year:
        return []

    years = sorted(series_by_year)
    base_year = years[0]
    base_value = series_by_year[base_year]
    if not base_value or math.isclose(base_value, 0.0):
        raise AgentError("Base year value is zero; cannot build index.")

    points: List[YearPoint] = []
    prev_value: Optional[float] = None
    for year in years:
        value = series_by_year[year]
        index = (value / base_value) * 100.0
        yoy = None if prev_value in (None, 0) else ((value / prev_value) - 1.0) * 100.0
        points.append(YearPoint(year=year, value=value, index=index, yoy=yoy))
        prev_value = value
    return points


def build_output(points: List[YearPoint], debug: Dict[str, Any], hs_codes: List[str], source_details: List[Dict[str, Any]]) -> Dict[str, Any]:
    latest_year = points[-1].year if points else None
    latest_index = round(points[-1].index, 1) if points else None
    latest_yoy = round(points[-1].yoy, 1) if points and points[-1].yoy is not None else None

    return {
        "meta": {
            "title": "Global Fuel Cell Trade Index",
            "source": "UN Comtrade annual goods trade API",
            "proxy_note": "HS 850680 and 850690 used as a practical fuel-cell-related trade proxy.",
            "hs_codes": hs_codes,
            "latest_year": latest_year,
            "last_update_utc": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        },
        "summary": {
            "latest_index": latest_index,
            "latest_yoy_pct": latest_yoy,
            "series_points": len(points),
        },
        "series": [
            {
                "year": p.year,
                "trade_value": round(p.value, 2),
                "index": round(p.index, 1),
                "yoy_pct": None if p.yoy is None else round(p.yoy, 1),
            }
            for p in points
        ],
        "debug": {
            **debug,
            "source_details": source_details,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build global fuel cell trade index JSON from UN Comtrade.")
    parser.add_argument("--output", default="data/customs-monitor.json", help="Output JSON path")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START, help="First year to include")
    parser.add_argument("--end-year", type=int, default=time.gmtime().tm_year - 1, help="Last year to include")
    parser.add_argument("--hs-code", action="append", dest="hs_codes", help="HS code to include; repeatable")
    parser.add_argument("--api-key-env", default="UN_COMTRADE_API_KEY", help="Environment variable for UN Comtrade API key")
    return parser.parse_args()


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def main() -> int:
    args = parse_args()
    hs_codes = args.hs_codes or DEFAULT_CODES
    api_key = os.getenv(args.api_key_env)

    if args.end_year < args.start_year:
        raise AgentError("end-year must be >= start-year")

    combined: Dict[int, float] = defaultdict(float)
    per_code_debug: Dict[str, Any] = {}
    source_details: List[Dict[str, Any]] = []

    for hs_code in hs_codes:
        rows, source_meta = fetch_comtrade_rows(
            hs_code=hs_code,
            start_year=args.start_year,
            end_year=args.end_year,
            api_key=api_key,
        )
        source_details.append({"hs_code": hs_code, **source_meta})
        year_map, code_debug = aggregate_years(rows, hs_code)
        per_code_debug[hs_code] = code_debug
        for year, value in year_map.items():
            combined[year] += value

    points = make_index(dict(sorted(combined.items())))
    output = build_output(points, {"per_hs_code": per_code_debug}, hs_codes, source_details)

    ensure_parent_dir(args.output)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Wrote {args.output} with {len(points)} series points.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        print(f"HTTP error from UN Comtrade API: {status}", file=sys.stderr)
        if exc.response is not None:
            print(exc.response.text[:1000], file=sys.stderr)
        raise SystemExit(2)
    except AgentError as exc:
        print(f"Agent error: {exc}", file=sys.stderr)
        raise SystemExit(3)
