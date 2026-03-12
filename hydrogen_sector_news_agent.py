"""
Optimized Hydrogen Sector News Agent (lightweight)
- Designed for fast GitHub Actions runs
- Conservative parsing
- Limited requests / limited article count
- Produces compact event_signals block
- Can optionally enrich tracker["event_signals"]

Usage inside your main agent:
    from hydrogen_sector_news_agent import build_event_signals

    event_signals = build_event_signals(
        news_items=[
            {"title": "...", "source": "...", "date": "...", "snippet": "..."},
            ...
        ]
    )
    tracker["event_signals"] = event_signals

Important:
- This module does NOT fetch the web by itself.
- Keep fetching logic separate so you can fully control runtime.
- Feed it only already-fetched, high-quality news/IR items.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


MAX_EVENTS = 12

POSITIVE_HINTS = (
    "backlog",
    "order",
    "orders",
    "deployment",
    "contract",
    "expansion",
    "capacity",
    "ramp",
    "commissioning",
    "production",
    "manufacturing",
    "datacenter",
    "data center",
    "power deal",
    "bookings",
)

NEGATIVE_HINTS = (
    "delay",
    "delayed",
    "cancel",
    "canceled",
    "cancelled",
    "weak demand",
    "slowdown",
    "cut",
    "shutdown",
    "impairment",
    "financing pressure",
)

MANUFACTURER_KEYWORDS = {
    "Bloom Energy": ("bloom energy", "bloom"),
    "Plug Power": ("plug power", "plug"),
    "Ballard Power": ("ballard", "ballard power"),
    "FuelCell Energy": ("fuelcell energy", "fuelcell"),
    "Nel": ("nel asa", "nel hydrogen", "nel"),
    "Cummins": ("cummins",),
}

LAYER_RULES = {
    "market_demand": (
        "datacenter",
        "data center",
        "microgrid",
        "hydrogen investment",
        "resilient power",
        "onsite power",
        "distributed power",
    ),
    "system_manufacturer": (
        "bloom",
        "plug",
        "ballard",
        "fuelcell energy",
        "nel",
        "cummins",
        "backlog",
        "bookings",
        "deployment",
    ),
    "component_supplier": (
        "component",
        "stack",
        "bipolar plate",
        "flow plate",
        "interconnect",
        "trade flow",
        "customs",
        "comext",
    ),
    "cell_impact": (
        "cell impact",
        "continuous production",
        "tooling order",
        "prototype",
        "serial production",
        "capacity readiness",
    ),
}


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().split())


def _combined_text(item: Dict[str, Any]) -> str:
    parts = [
        _clean_text(item.get("title")),
        _clean_text(item.get("snippet")),
        _clean_text(item.get("summary")),
        _clean_text(item.get("source")),
    ]
    return " | ".join([p for p in parts if p]).lower()


def _pick_layer(text: str) -> str:
    for layer, keywords in LAYER_RULES.items():
        for kw in keywords:
            if kw in text:
                return layer
    return "market_demand"


def _pick_entity(text: str) -> str:
    if "cell impact" in text:
        return "Cell Impact"
    for name, kws in MANUFACTURER_KEYWORDS.items():
        if any(kw in text for kw in kws):
            return name
    return "Hydrogen sector"


def _impact_and_status(text: str) -> Dict[str, str]:
    pos_hits = sum(1 for kw in POSITIVE_HINTS if kw in text)
    neg_hits = sum(1 for kw in NEGATIVE_HINTS if kw in text)

    if pos_hits > neg_hits:
        return {"impact": "positive", "status": "up"}
    if neg_hits > pos_hits:
        return {"impact": "negative", "status": "down"}
    return {"impact": "neutral", "status": "stable"}


def _weight(text: str, layer: str) -> int:
    base = 2
    if layer == "cell_impact":
        base += 1
    if "backlog" in text or "contract" in text or "continuous production" in text:
        base += 1
    if "datacenter" in text or "data center" in text:
        base += 1
    return max(1, min(5, base))


def _confidence(text: str, source: str) -> str:
    source_l = source.lower()
    score = 0
    if any(k in source_l for k in ("investor", "ir", "press", "reuters", "bloomberg")):
        score += 2
    if len(text) > 80:
        score += 1
    if any(k in text for k in ("backlog", "contract", "production", "deployment")):
        score += 1

    if score >= 4:
        return "high"
    if score >= 2:
        return "medium"
    return "low"


def _dedupe_key(item: Dict[str, Any]) -> str:
    return _clean_text(item.get("title")).lower()


def build_event_signals(news_items: Optional[List[Dict[str, Any]]] = None, lookback_days: int = 90) -> Dict[str, Any]:
    """
    Convert already-fetched news / IR items into a compact event_signals block.
    Keep the input list small (e.g. 5-20 items) for fast GitHub runs.
    """
    news_items = news_items or []

    seen = set()
    events: List[Dict[str, Any]] = []

    for item in news_items:
        title = _clean_text(item.get("title"))
        if not title:
            continue

        dedupe = _dedupe_key(item)
        if dedupe in seen:
            continue
        seen.add(dedupe)

        text = _combined_text(item)
        layer = _pick_layer(text)
        entity = _pick_entity(text)
        impact_status = _impact_and_status(text)
        source = _clean_text(item.get("source"))
        confidence = _confidence(text, source)

        events.append(
            {
                "date": _clean_text(item.get("date")) or "",
                "layer": layer,
                "entity": entity,
                "title": title,
                "impact": impact_status["impact"],
                "status": impact_status["status"],
                "weight": _weight(text, layer),
                "confidence": confidence,
                "note": _clean_text(item.get("snippet"))[:220],
                "source": source,
            }
        )

        if len(events) >= MAX_EVENTS:
            break

    return {
        "lookback_days": lookback_days,
        "events": events,
        "momentum_rules": {
            "accelerating_threshold": 1.2,
            "weakening_threshold": 0.8,
            "note": "Momentum can be approximated as current-period event count divided by previous comparable-period event count."
        },
        "meta": {
            "source_type": "agent_enriched",
            "runtime_profile": "lightweight",
            "update_note": "Keep feeds small and sources high quality for fast GitHub Actions runs."
        }
    }
