#!/usr/bin/env python3
"""
Fuel cell trade / customs monitor helper for the Cell Impact live tracker.

Purpose
-------
This script fetches a Eurostat JSON-stat dataset, parses it robustly, and
writes a tracker-friendly JSON payload for the customs monitor chart.

Why this exists
---------------
The common failure mode with Eurostat JSON-stat responses is that `value`
can be either a dict keyed by positional index or a list, while the time
series dimensions must be reconstructed from `id`, `size`, and `dimension`.
If that mapping is skipped or partially wrong, the frontend receives an
empty series and renders a blank chart.

What this script does
---------------------
- Fetches JSON-stat data from Eurostat
- Reconstructs observation tuples from dataset dimensions
- Detects a time/year dimension automatically
- Aggregates observations by year
- Builds a normalized index where the first non-empty year = 100
- Computes YoY
- Emits diagnostics instead of silently returning an empty chart

Usage examples
--------------
python fuel_cell_trade_agent.py \
  --dataset tet00013 \
  --output data/customs-monitor.json

python fuel_cell_trade_agent.py \
  --dataset tet00013 \
  --filter geo=EU27_2020 \
  --filter unit=I20 \
  --output data/customs-monitor.json

Notes
-----
This script does not claim that `tet00013` is the correct fuel-cell-specific
source. It only fixes the parser/series construction side. If your chosen
filters do not match actual dataset content, the script will tell you that in
its diagnostics instead of generating a misleading blank chart.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass
from itertools import product
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote

import requests

EUROSTAT_API_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
DEFAULT_TIMEOUT = 40


class FuelTradeAgentError(Exception):
    pass


@dataclass
class Observation:
    dims: Dict[str, str]
    value: Optional[float]


@dataclass
class ParsedDataset:
    label: str
    dimensions: List[str]
    observations: List[Observation]



def build_url(dataset: str, filters: Dict[str, str]) -> str:
    url = f"{EUROSTAT_API_BASE}/{quote(dataset)}?format=JSON"
    if filters:
        for key, value in filters.items():
            url += f"&{quote(key)}={quote(value)}"
    return url



def fetch_payload(dataset: str, filters: Dict[str, str]) -> Dict[str, Any]:
    url = build_url(dataset, filters)
    response = requests.get(url, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise FuelTradeAgentError("Eurostat payload is not a JSON object.")
    return payload



def _dimension_categories(dimension_obj: Dict[str, Any], dim_name: str) -> List[str]:
    category = dimension_obj.get("category", {}) or {}
    index = category.get("index")
    label = category.get("label", {}) or {}

    if isinstance(index, list):
        # Rare form, but preserve order as provided.
        keys = [str(x) for x in index]
    elif isinstance(index, dict):
        # Standard JSON-stat form: {"2021": 0, "2022": 1}
        keys = [k for k, _ in sorted(index.items(), key=lambda kv: kv[1])]
    else:
        # Fallback: labels only.
        if isinstance(label, dict) and label:
            keys = list(label.keys())
        else:
            raise FuelTradeAgentError(f"Could not resolve categories for dimension '{dim_name}'.")

    return [str(label.get(k, k)) for k in keys]



def parse_jsonstat(payload: Dict[str, Any]) -> ParsedDataset:
    dim_ids = payload.get("id") or []
    size = payload.get("size") or []
    dimension = payload.get("dimension") or {}
    values = payload.get("value")
    label = str(payload.get("label") or payload.get("source") or "Eurostat dataset")

    if not dim_ids or not size or not isinstance(dimension, dict):
        raise FuelTradeAgentError("Payload is missing JSON-stat dimensions ('id', 'size', or 'dimension').")

    if len(dim_ids) != len(size):
        raise FuelTradeAgentError("JSON-stat payload has mismatched dimension metadata.")

    categories_per_dim: List[List[str]] = []
    for dim_name in dim_ids:
        dim_obj = dimension.get(dim_name)
        if not isinstance(dim_obj, dict):
            raise FuelTradeAgentError(f"Missing dimension object for '{dim_name}'.")
        categories = _dimension_categories(dim_obj, dim_name)
        categories_per_dim.append(categories)

    expected_obs = math.prod(size)

    if isinstance(values, list):
        value_lookup = {i: values[i] for i in range(min(len(values), expected_obs))}
    elif isinstance(values, dict):
        value_lookup = {}
        for key, value in values.items():
            try:
                value_lookup[int(key)] = value
            except (TypeError, ValueError):
                continue
    else:
        raise FuelTradeAgentError("JSON-stat payload does not contain a supported 'value' field.")

    observations: List[Observation] = []
    for flat_index, combo in enumerate(product(*categories_per_dim)):
        raw_value = value_lookup.get(flat_index)
        value: Optional[float]
        if raw_value is None or raw_value == "":
            value = None
        else:
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                value = None
        observations.append(Observation(dims=dict(zip(dim_ids, combo)), value=value))

    return ParsedDataset(label=label, dimensions=[str(x) for x in dim_ids], observations=observations)



def detect_time_dimension(dimensions: Sequence[str]) -> Optional[str]:
    preferred_names = {"time", "TIME_PERIOD", "period", "year", "TIME"}
    for dim in dimensions:
        if dim in preferred_names:
            return dim
    for dim in dimensions:
        lowered = dim.lower()
        if "time" in lowered or "year" in lowered or "period" in lowered:
            return dim
    return None



def to_year_label(raw: str) -> Optional[str]:
    raw = str(raw).strip()
    if len(raw) >= 4 and raw[:4].isdigit():
        return raw[:4]
    return None



def aggregate_year_series(parsed: ParsedDataset) -> Tuple[List[Dict[str, float]], Dict[str, Any]]:
    time_dim = detect_time_dimension(parsed.dimensions)
    if not time_dim:
        return [], {
            "status": "error",
            "reason": "No time/year dimension found in dataset.",
            "dimensions": parsed.dimensions,
        }

    year_totals: Dict[str, float] = {}
    non_null_count = 0

    for obs in parsed.observations:
        year_raw = obs.dims.get(time_dim)
        year = to_year_label(year_raw) if year_raw is not None else None
        if year is None:
            continue
        if obs.value is None:
            continue
        year_totals[year] = year_totals.get(year, 0.0) + obs.value
        non_null_count += 1

    if not year_totals:
        return [], {
            "status": "empty",
            "reason": "Dataset returned no numeric observations after parsing and year aggregation.",
            "dimensions": parsed.dimensions,
            "time_dimension": time_dim,
            "observation_count": len(parsed.observations),
            "non_null_observation_count": non_null_count,
        }

    ordered_years = sorted(year_totals.keys())
    baseline_year = ordered_years[0]
    baseline_value = year_totals[baseline_year]

    if baseline_value == 0:
        return [], {
            "status": "empty",
            "reason": "Baseline year aggregated to zero, so index normalization would be invalid.",
            "baseline_year": baseline_year,
        }

    series: List[Dict[str, float]] = []
    for year in ordered_years:
        raw = year_totals[year]
        index_value = (raw / baseline_value) * 100.0
        series.append({
            "year": int(year),
            "value": round(index_value, 2),
            "raw_value": round(raw, 6),
        })

    return series, {
        "status": "ok",
        "time_dimension": time_dim,
        "baseline_year": int(baseline_year),
        "baseline_raw_value": round(baseline_value, 6),
        "years_found": [int(y) for y in ordered_years],
    }



def compute_yoy(series: Sequence[Dict[str, float]]) -> Optional[float]:
    if len(series) < 2:
        return None
    prev = float(series[-2]["value"])
    curr = float(series[-1]["value"])
    if prev == 0:
        return None
    return round(((curr - prev) / prev) * 100.0, 2)



def build_output(
    dataset: str,
    filters: Dict[str, str],
    parsed: ParsedDataset,
    series: Sequence[Dict[str, float]],
    diagnostics: Dict[str, Any],
) -> Dict[str, Any]:
    latest_year = series[-1]["year"] if series else None
    latest_value = series[-1]["value"] if series else None
    yoy = compute_yoy(series)

    status = diagnostics.get("status", "unknown")
    note = (
        "Series parsed successfully from Eurostat JSON-stat payload."
        if status == "ok"
        else diagnostics.get("reason", "Unknown parser state.")
    )

    return {
        "customs_monitor": {
            "title": "Global Fuel Cell Trade Index",
            "dataset_code": dataset,
            "dataset_label": parsed.label,
            "latest_year": latest_year,
            "latest_value": latest_value,
            "yoy": yoy,
            "series": list(series),
            "note": note,
            "status": status,
            "diagnostics": {
                **diagnostics,
                "filters": filters,
                "dimension_names": parsed.dimensions,
                "total_observations": len(parsed.observations),
            },
        }
    }



def parse_filter_args(filter_args: Optional[Sequence[str]]) -> Dict[str, str]:
    filters: Dict[str, str] = {}
    for item in filter_args or []:
        if "=" not in item:
            raise FuelTradeAgentError(f"Invalid --filter '{item}'. Expected key=value.")
        key, value = item.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise FuelTradeAgentError(f"Invalid --filter '{item}'. Filter key is empty.")
        filters[key] = value
    return filters



def write_json(path: str, data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")



def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Build customs monitor JSON from Eurostat JSON-stat data.")
    parser.add_argument("--dataset", required=True, help="Eurostat dataset code, for example tet00013")
    parser.add_argument("--filter", action="append", help="Dataset filter in key=value form. Repeat as needed.")
    parser.add_argument("--output", required=True, help="Output JSON file path")
    args = parser.parse_args(argv)

    try:
        filters = parse_filter_args(args.filter)
        payload = fetch_payload(args.dataset, filters)
        parsed = parse_jsonstat(payload)
        series, diagnostics = aggregate_year_series(parsed)
        output = build_output(args.dataset, filters, parsed, series, diagnostics)
        write_json(args.output, output)

        status = output["customs_monitor"]["status"]
        print(f"Wrote {args.output} (status={status}, points={len(series)})")
        return 0
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else "unknown"
        print(f"HTTP error while fetching Eurostat dataset: {status_code}", file=sys.stderr)
        return 2
    except requests.RequestException as exc:
        print(f"Network error while fetching Eurostat dataset: {exc}", file=sys.stderr)
        return 2
    except FuelTradeAgentError as exc:
        print(f"Parser error: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:  # pragma: no cover
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
