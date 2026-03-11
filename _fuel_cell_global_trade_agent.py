#!/usr/bin/env python3
"""
Build a simple global fuel-cell-trade monitor JSON from UN Comtrade-style annual data.

Important:
- This script is defensive by design.
- It ALWAYS writes output JSON, even when the upstream API fails.
- If the data source is unavailable, the output JSON includes a clear error/debug block
  so the frontend can show a message instead of an empty chart.

Default HS codes (proxy only, not fuel-cell-only classifications):
- 850680 : Primary cells and primary batteries, other
- 850690 : Parts of primary cells and primary batteries
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

# This can be overridden in GitHub Actions with COMTRADE_API_BASE if needed.
DEFAULT_API_BASE = "https://comtradeapi.worldbank.org/data/v1/get/C/A/HS"
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

    for value in payload.values():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return [row for row in value if isinstance(row, dict)]

    return []


def fetch_comtrade_rows(
    hs_code: str,
    start_year: int,
    end_year: int,
    api_base: str,
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

    try:
        response = requests.get(api_base, params=params, headers=headers, timeout=TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        rows = extract_rows(payload)
        meta = {
            "requested_url": response.url,
            "status": "ok",
            "status_code": response.status_code,
            "row_count": len(rows),
            "top_level_keys": list(payload.keys())[:20] if isinstance(payload, dict) else [],
        }
        return rows, meta
    except requests.RequestException as exc:
        response = getattr(exc, "response", None)
        return [], {
            "requested_url": getattr(response, "url", None),
            "status": "error",
            "status_code": getattr(response, "status_code", None),
            "row_count": 0,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "api_base": api_base,
            "params": params,
        }
    except ValueError as exc:
        return [], {
            "requested_url": None,
            "status": "error",
            "status_code": None,
            "row_count": 0,
            "error_type": "JSONDecodeError",
            "error": str(exc),
            "api_base": api_base,
            "params": params,
        }


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


def build_output(
    points: List[YearPoint],
    debug: Dict[str, Any],
    hs_codes: List[str],
    source_details: List[Dict[str, Any]],
    api_base: str,
) -> Dict[str, Any]:
    latest_year = points[-1].year if points else None
    latest_index = round(points[-1].index, 1) if points else None
    latest_yoy = round(points[-1].yoy, 1) if points and points[-1].yoy is not None else None
    any_error = any(detail.get("status") == "error" for detail in source_details)
    ok_rows = sum(int(detail.get("row_count", 0) or 0) for detail in source_details)

    return {
        "meta": {
            "title": "Global Fuel Cell Trade Index",
            "source": "UN Comtrade annual goods trade API proxy",
            "proxy_note": "HS 850680 and 850690 used as a practical fuel-cell-related trade proxy.",
            "hs_codes": hs_codes,
            "latest_year": latest_year,
            "last_update_utc": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "api_base": api_base,
            "status": "error" if any_error and ok_rows == 0 else ("partial" if any_error else "ok"),
        },
        "summary": {
            "latest_index": latest_index,
            "latest_yoy_pct": latest_yoy,
            "series_points": len(points),
            "message": None if points else "Data unavailable or upstream API returned no usable rows.",
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
    parser = argparse.ArgumentParser(description="Build global fuel cell trade index JSON from UN Comtrade-style data.")
    parser.add_argument("--output", default="data/customs-monitor.json", help="Output JSON path")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START, help="First year to include")
    parser.add_argument("--end-year", type=int, default=time.gmtime().tm_year - 1, help="Last year to include")
    parser.add_argument("--hs-code", action="append", dest="hs_codes", help="HS code to include; repeatable")
    parser.add_argument("--api-key-env", default="UN_COMTRADE_API_KEY", help="Environment variable for API key")
    parser.add_argument("--api-base-env", default="COMTRADE_API_BASE", help="Environment variable for API base override")
    return parser.parse_args()


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def main() -> int:
    args = parse_args()
    hs_codes = args.hs_codes or DEFAULT_CODES
    api_key = os.getenv(args.api_key_env)
    api_base = os.getenv(args.api_base_env, DEFAULT_API_BASE)

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
            api_base=api_base,
            api_key=api_key,
        )
        source_details.append({"hs_code": hs_code, **source_meta})
        year_map, code_debug = aggregate_years(rows, hs_code)
        per_code_debug[hs_code] = code_debug
        for year, value in year_map.items():
            combined[year] += value

    try:
        points = make_index(dict(sorted(combined.items())))
    except AgentError as exc:
        points = []
        per_code_debug["index_error"] = str(exc)

    output = build_output(points, {"per_hs_code": per_code_debug}, hs_codes, source_details, api_base)

    ensure_parent_dir(args.output)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    status = output.get("meta", {}).get("status")
    print(f"Wrote {args.output} with {len(points)} series points. Status: {status}")

    # Intentionally return success so GitHub Actions can commit the fallback JSON.
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AgentError as exc:
        print(f"Agent error: {exc}", file=sys.stderr)
        raise SystemExit(3)
