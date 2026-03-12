"""Supply Chain Radar v2

A more structured, event-capable radar module for Cell Impact tracker.

Features
- Keeps v1 compatibility with existing tracker fields
- Supports optional manufacturer_signals and event_signals blocks
- Produces supply_chain_radar with layers, momentum, nowcast, confidence
- Conservative scoring; not a revenue forecast
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def clamp(value: Any, lo: float = 0.0, hi: float = 100.0) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return lo
    return max(lo, min(hi, v))


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def score_to_status(score: float) -> str:
    if score >= 62:
        return "up"
    if score <= 42:
        return "down"
    return "stable"


def score_to_strength(score: float) -> str:
    if score >= 72:
        return "strong"
    if score >= 52:
        return "medium"
    return "weak"


def _pct_change(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous in (None, 0):
        return None
    return ((current / previous) - 1.0) * 100.0


def _series_latest_and_prev(series: Any) -> tuple[Optional[float], Optional[float]]:
    if not isinstance(series, list) or not series:
        return None, None
    latest = safe_float(series[-1].get("value")) if isinstance(series[-1], dict) else None
    prev = safe_float(series[-2].get("value")) if len(series) >= 2 and isinstance(series[-2], dict) else None
    return latest, prev


def _count_event_score(events: List[Dict[str, Any]], layer: str) -> float:
    total = 0.0
    for event in events:
        if str(event.get("layer", "")).strip().lower() != layer:
            continue
        weight = safe_float(event.get("weight"), 1.0) or 1.0
        impact = str(event.get("impact", "neutral")).strip().lower()
        if impact == "positive":
            total += weight
        elif impact == "negative":
            total -= weight
    return total


def _manufacturer_composite(manufacturer_signals: Dict[str, Any]) -> float:
    """Optional richer layer based on manufacturer signals.

    Expected shape example:
    {
      "companies": [
        {"name":"Bloom Energy", "backlog_yoy_pct":12, "orders_status":"up", "deployments_status":"stable"}
      ]
    }
    """
    companies = manufacturer_signals.get("companies", [])
    if not isinstance(companies, list) or not companies:
        return 0.0

    total = 0.0
    count = 0
    for company in companies:
        if not isinstance(company, dict):
            continue
        score = 50.0
        backlog_yoy = safe_float(company.get("backlog_yoy_pct"))
        if backlog_yoy is not None:
            score += backlog_yoy * 0.9
        orders_status = str(company.get("orders_status", "")).lower()
        deployments_status = str(company.get("deployments_status", "")).lower()
        if orders_status == "up":
            score += 6
        elif orders_status == "down":
            score -= 6
        if deployments_status == "up":
            score += 4
        elif deployments_status == "down":
            score -= 4
        total += clamp(score)
        count += 1
    return 0.0 if count == 0 else (total / count) - 50.0


def calculate_supply_chain_radar_v2(tracker: Dict[str, Any]) -> Dict[str, Any]:
    market_signals = tracker.get("market_signals", {})
    monthly_trade_pulse = tracker.get("monthly_trade_pulse", {})
    trade_signals = tracker.get("trade_signals", {})
    customs_monitor = tracker.get("customs_monitor", {})
    company = tracker.get("company", {})
    probability = tracker.get("probability", {})
    ir_headlines = tracker.get("ir_headlines", []) or []

    # Optional richer inputs for v2
    manufacturer_signals = tracker.get("manufacturer_signals", {}) or {}
    event_signals = tracker.get("event_signals", {}) or {}
    events = event_signals.get("events", []) if isinstance(event_signals, dict) else []
    if not isinstance(events, list):
        events = []

    macro_yoy = safe_float(trade_signals.get("macro_signal", {}).get("yoy_pct"))
    runway_months = safe_float(company.get("runway_months"))
    funding_prob = safe_float(probability.get("funding_through_2027_pct"))
    dilution_value = str(market_signals.get("dilution_pressure", {}).get("value", "")).strip().lower()

    balanced_series = monthly_trade_pulse.get("balanced_series", []) or []
    exports_series = monthly_trade_pulse.get("exports_series", []) or []
    imports_series = monthly_trade_pulse.get("imports_series", []) or []

    latest_balanced, prev_balanced = _series_latest_and_prev(balanced_series)
    latest_exports, prev_exports = _series_latest_and_prev(exports_series)
    latest_imports, prev_imports = _series_latest_and_prev(imports_series)

    balanced_mom_pct = _pct_change(latest_balanced, prev_balanced)
    exports_mom_pct = _pct_change(latest_exports, prev_exports)
    imports_mom_pct = _pct_change(latest_imports, prev_imports)

    # Event subscores (optional)
    event_market = _count_event_score(events, "market_demand")
    event_system = _count_event_score(events, "system_manufacturer")
    event_component = _count_event_score(events, "component_supplier")
    event_ci = _count_event_score(events, "cell_impact")

    market_score = 50.0
    if balanced_mom_pct is not None:
        market_score += balanced_mom_pct * 2.4
    if macro_yoy is not None:
        market_score += macro_yoy * 1.2
    if ir_headlines:
        market_score += 4
    market_score += event_market * 2.0
    market_score = clamp(market_score)

    system_score = 48.0
    if balanced_mom_pct is not None:
        system_score += balanced_mom_pct * 1.8
    if exports_mom_pct is not None:
        system_score += exports_mom_pct * 1.2
    if len(ir_headlines) >= 3:
        system_score += 5
    if macro_yoy is not None:
        system_score += macro_yoy * 0.8
    system_score += _manufacturer_composite(manufacturer_signals)
    system_score += event_system * 2.2
    system_score = clamp(system_score)

    component_score = 50.0
    if balanced_mom_pct is not None:
        component_score += balanced_mom_pct * 2.8
    if exports_mom_pct is not None:
        component_score += exports_mom_pct * 1.8
    if imports_mom_pct is not None:
        component_score += imports_mom_pct * 1.2
    component_score += event_component * 2.0
    component_score = clamp(component_score)

    ci_score = 50.0
    if runway_months is not None:
        ci_score += (runway_months - 8.0) * 3.0
    if funding_prob is not None:
        ci_score += (funding_prob - 50.0) * 0.35
    if "elevated" in dilution_value:
        ci_score -= 8
    if ir_headlines:
        ci_score += 4
    ci_score += event_ci * 2.0
    ci_score = clamp(ci_score)

    nowcast = round(
        (market_score * 0.28)
        + (system_score * 0.27)
        + (component_score * 0.25)
        + (ci_score * 0.20)
    )

    if balanced_mom_pct is None:
        momentum = "Unavailable"
    elif balanced_mom_pct >= 2.0:
        momentum = "Accelerating"
    elif balanced_mom_pct <= -2.0:
        momentum = "Weakening"
    else:
        momentum = "Stable"

    confidence_points = 0
    if balanced_mom_pct is not None:
        confidence_points += 1
    if exports_mom_pct is not None:
        confidence_points += 1
    if macro_yoy is not None:
        confidence_points += 1
    if runway_months is not None:
        confidence_points += 1
    if funding_prob is not None:
        confidence_points += 1
    if isinstance(manufacturer_signals.get("companies"), list) and manufacturer_signals.get("companies"):
        confidence_points += 1
    if events:
        confidence_points += 1

    if confidence_points >= 6:
        confidence = "High"
    elif confidence_points >= 4:
        confidence = "Medium"
    else:
        confidence = "Low"

    layers = [
        {
            "title": "Market Demand",
            "status": score_to_status(market_score),
            "strength": score_to_strength(market_score),
            "score": round(market_score),
            "description": "Derived from monthly balanced trade pulse, macro backdrop and market-level event flow.",
        },
        {
            "title": "System Manufacturers",
            "status": score_to_status(system_score),
            "strength": score_to_strength(system_score),
            "score": round(system_score),
            "description": "Built from market pulse, exports momentum and optional manufacturer signal/event inputs.",
        },
        {
            "title": "Component Suppliers",
            "status": score_to_status(component_score),
            "strength": score_to_strength(component_score),
            "score": round(component_score),
            "description": "Built from balanced bundle momentum plus export/import flow direction and supplier events.",
        },
        {
            "title": "Cell Impact Signals",
            "status": score_to_status(ci_score),
            "strength": score_to_strength(ci_score),
            "score": round(ci_score),
            "description": "Uses runway, funding probability, dilution pressure and company-level event context.",
        },
    ]

    return {
        "version": "v2",
        "momentum": momentum,
        "nowcast": nowcast,
        "confidence": confidence,
        "layers": layers,
        "methodology": "Auto-calculated from current tracker fields, with optional manufacturer_signals and event_signals. Conservative proxy model; not a revenue forecast.",
    }


def calculate_supply_chain_radar(tracker: Dict[str, Any]) -> Dict[str, Any]:
    """Compatibility alias."""
    return calculate_supply_chain_radar_v2(tracker)


if __name__ == "__main__":
    # Tiny self-test example
    example = {
        "market_signals": {"dilution_pressure": {"value": "Elevated"}},
        "monthly_trade_pulse": {
            "balanced_series": [{"value": 100.0}, {"value": 103.0}],
            "exports_series": [{"value": 100.0}, {"value": 102.0}],
            "imports_series": [{"value": 100.0}, {"value": 101.0}],
        },
        "trade_signals": {"macro_signal": {"yoy_pct": -2.8}},
        "company": {"runway_months": 8.3},
        "probability": {"funding_through_2027_pct": 41},
        "ir_headlines": [{"title": "Headline 1"}, {"title": "Headline 2"}, {"title": "Headline 3"}],
        "manufacturer_signals": {
            "companies": [
                {"name": "Bloom Energy", "backlog_yoy_pct": 12, "orders_status": "up", "deployments_status": "stable"}
            ]
        },
        "event_signals": {
            "events": [
                {"layer": "market_demand", "impact": "positive", "weight": 2},
                {"layer": "system_manufacturer", "impact": "positive", "weight": 2},
            ]
        },
    }
    import json
    print(json.dumps(calculate_supply_chain_radar_v2(example), indent=2))
