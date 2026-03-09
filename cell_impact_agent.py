#!/usr/bin/env python3
import html
import json
import math
import re
import ssl
import urllib.parse
import urllib.request
from datetime import datetime, timezone

DATA_PATH = "data/tracker.json"
IR_URL = "https://investor.cellimpact.com/en/investor-relations"
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/CI.ST?range=5d&interval=1d"

EUROSTAT_API_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
EUROSTAT_DATASET = "tet00013"
EUROSTAT_QUERY = {}

# Monthly bundle configuration
COMEXT_MONTHLY_DATASET_CANDIDATES = ["DS-059332", "DS-059322"]
PARTNERS = ["US", "CN", "JP", "KR"]
PARTNER_ALIASES = {
    "US": ["US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"],
    "CN": ["CN", "CHINA", "PEOPLE'S REPUBLIC OF CHINA"],
    "JP": ["JP", "JAPAN"],
    "KR": ["KR", "KOREA", "SOUTH KOREA", "REPUBLIC OF KOREA"],
}
PRODUCT_GROUPS = {
    "hydrogen_anchor": ["8501", "8504"],
    "metals_forming_proxy": ["7219", "7326"],
}
MONTHLY_WINDOW = 18
MONTHLY_SMOOTHING = 3
EXPORT_WEIGHT = 0.6
IMPORT_WEIGHT = 0.4


def fetch_text(url, timeout=30):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return r.read().decode("utf-8", errors="ignore")


def fetch_json_url(url, timeout=30):
    return json.loads(fetch_text(url, timeout=timeout))


def build_eurostat_url(dataset, params=None):
    if not dataset:
        return None
    base = f"{EUROSTAT_API_BASE}/{dataset}"
    params = dict(params or {})
    params["format"] = "JSON"
    return f"{base}?{urllib.parse.urlencode(params, doseq=True)}"


def load_data():
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_data(data):
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def parse_ir_headlines(html_text):
    matches = re.findall(r'href="([^"]+)".{0,300}?>([^<]{12,160})<', html_text, flags=re.I | re.S)
    out, seen = [], set()
    for href, title in matches:
        t = html.unescape(re.sub(r"\s+", " ", title)).strip()
        if len(t) < 15:
            continue
        lower = t.lower()
        if not any(key in lower for key in ["cell impact", "rights issue", "bta", "report", "share", "annual", "quarter", "trading"]):
            continue
        if t in seen:
            continue
        seen.add(t)
        if href.startswith("/"):
            href = "https://investor.cellimpact.com" + href
        out.append({"title": t, "url": href, "date": ""})
        if len(out) == 6:
            break
    return out


def fetch_price():
    try:
        raw = fetch_text(YAHOO_URL)
        m = re.search(r'"regularMarketPrice":\s*([0-9.]+)', raw)
        c = re.search(r'"chartPreviousClose":\s*([0-9.]+)', raw)
        if m:
            price = float(m.group(1))
            prev = float(c.group(1)) if c else None
            meta = f"CI.ST | {((price - prev) / prev) * 100:+.2f}% vs prev close" if prev else "CI.ST"
            return {"value": f"{price:.3f} SEK", "meta": meta, "status": "Live"}
    except Exception:
        pass
    return None


def jsonstat_category_map(dim_obj):
    cat = dim_obj.get("category", {})
    idx = cat.get("index", {})
    labels = cat.get("label", {})
    if isinstance(idx, list):
        return {str(code): {"pos": pos, "label": labels.get(str(code), str(code))} for pos, code in enumerate(idx)}
    mapped = {}
    for code, pos in idx.items():
        mapped[str(code)] = {"pos": int(pos), "label": labels.get(str(code), str(code))}
    return mapped


def decode_coords(flat_index, sizes):
    coords = []
    for size in reversed(sizes):
        coords.append(flat_index % size)
        flat_index //= size
    return list(reversed(coords))


def build_pos_to_code(dim_obj):
    cmap = jsonstat_category_map(dim_obj)
    out = {}
    for code, meta in cmap.items():
        out[meta["pos"]] = (code, meta["label"])
    return out


def jsonstat_to_rows(payload):
    dims = payload.get("id", [])
    sizes = payload.get("size", [])
    dimension = payload.get("dimension", {})
    values = payload.get("value", {})
    pos_maps = {dim: build_pos_to_code(dimension[dim]) for dim in dims if dim in dimension}
    rows = []
    iterable = enumerate(values) if isinstance(values, list) else ((int(k), v) for k, v in values.items())
    for flat_idx, value in iterable:
        coords = decode_coords(flat_idx, sizes)
        row = {"value": value}
        for i, dim in enumerate(dims):
            pos = coords[i]
            code, label = pos_maps.get(dim, {}).get(pos, (str(pos), str(pos)))
            row[f"{dim}_code"] = code
            row[f"{dim}_label"] = label
        rows.append(row)
    return rows


def normalize_year(s):
    m = re.search(r"(20\d{2}|19\d{2})", str(s))
    return int(m.group(1)) if m else None


def normalize_period(s):
    s = str(s or "").strip()
    patterns = [
        r"(?P<y>20\d{2}|19\d{2})[-/]?(?P<m>0[1-9]|1[0-2])",
        r"(?P<y>20\d{2}|19\d{2})M(?P<m>0?[1-9]|1[0-2])",
        r"(?P<y>20\d{2}|19\d{2})[-_/ ](?P<m>0?[1-9]|1[0-2])",
    ]
    for pat in patterns:
        m = re.search(pat, s)
        if m:
            return f"{int(m.group('y')):04d}-{int(m.group('m')):02d}"
    return None


def choose_time_field(row):
    for k in row.keys():
        if k.endswith("_code") or k.endswith("_label"):
            base = k[:-5]
            if base.lower() in ("time", "time_period", "period", "year"):
                return base
    for k, v in row.items():
        if k.endswith("_label") and (normalize_period(v) or normalize_year(v)):
            return k[:-6]
    return None


def row_text(row):
    return " ".join(str(v).lower() for v in row.values() if isinstance(v, (str, int, float)))


def score_row(row):
    text = row_text(row)
    score = 0
    for term in ["fuel", "hydrogen", "energy", "electrical", "machinery", "parts", "equipment"]:
        if term in text:
            score += 1
    return score


def eurostat_series_from_payload(payload):
    try:
        rows = jsonstat_to_rows(payload)
        if not rows:
            return None
        time_field = choose_time_field(rows[0])
        if not time_field:
            return None
        grouped = {}
        for row in rows:
            year = normalize_year(row.get(f"{time_field}_code")) or normalize_year(row.get(f"{time_field}_label"))
            if not year:
                continue
            sc = score_row(row)
            val = row.get("value")
            if val in (None, ":"):
                continue
            try:
                val = float(val)
            except Exception:
                continue
            grouped.setdefault(year, []).append((sc, val, row))
        if not grouped:
            return None
        series = []
        for year in sorted(grouped.keys()):
            best = sorted(grouped[year], key=lambda x: (x[0], x[1]), reverse=True)[0]
            series.append({"year": int(year), "value": round(float(best[1]), 2)})
        if len(series) < 2:
            return None
        return {"eu_imports_proxy": series}
    except Exception:
        return None


def fetch_live_customs_series():
    url = build_eurostat_url(EUROSTAT_DATASET, EUROSTAT_QUERY)
    try:
        payload = fetch_json_url(url)
        return eurostat_series_from_payload(payload)
    except Exception:
        return None


def period_sort_key(period):
    try:
        y, m = period.split("-")
        return (int(y), int(m))
    except Exception:
        return (0, 0)


def moving_average_points(points, window=3):
    if not points:
        return []
    out = []
    for i in range(len(points)):
        start = max(0, i - window + 1)
        subset = points[start:i + 1]
        vals = [float(p["value"]) for p in subset if p.get("value") is not None]
        if vals:
            out.append({"period": points[i]["period"], "value": round(sum(vals) / len(vals), 1)})
    return out


def yoy_from_monthly(series):
    if not series:
        return None
    by_period = {p["period"]: p["value"] for p in series}
    latest = series[-1]["period"]
    y, m = latest.split("-")
    prev = f"{int(y)-1:04d}-{m}"
    if prev in by_period and by_period[prev]:
        return round(((series[-1]["value"] - by_period[prev]) / by_period[prev]) * 100, 1)
    return None


def iter_code_label_pairs(row):
    for key, value in row.items():
        if key.endswith("_code"):
            base = key[:-5]
            yield str(value), str(row.get(f"{base}_label", ""))
        elif key.endswith("_label"):
            base = key[:-6]
            if f"{base}_code" not in row:
                yield "", str(value)


def row_matches_partner(row):
    text_code = row_text(row).upper()
    for partner, aliases in PARTNER_ALIASES.items():
        for alias in aliases:
            if alias.upper() in text_code:
                return partner
    return None


def detect_flow(row, dataset_name=""):
    combined = (row_text(row) + " " + str(dataset_name)).lower()
    export_terms = ["export", "exports", "dispatch", "outgoing", "exp"]
    import_terms = ["import", "imports", "arrival", "incoming", "imp"]
    if any(term in combined for term in export_terms) and not any(term in combined for term in import_terms):
        return "exports"
    if any(term in combined for term in import_terms) and not any(term in combined for term in export_terms):
        return "imports"
    # DS-059332 is extra-EU imports dataset by design
    if str(dataset_name).upper() == "DS-059332":
        return "imports"
    return None


def row_matches_product_group(row):
    matched_groups = set()
    for code, label in iter_code_label_pairs(row):
        code = (code or "").replace(" ", "").upper()
        label = (label or "").lower()
        for group, prefixes in PRODUCT_GROUPS.items():
            for prefix in prefixes:
                if code.startswith(prefix):
                    matched_groups.add(group)
                    break
            # secondary fallback: obvious label hints
            if group == "hydrogen_anchor" and any(term in label for term in ["motor", "generator", "transform", "converter"]):
                matched_groups.add(group)
            if group == "metals_forming_proxy" and any(term in label for term in ["stainless", "iron", "steel", "flat-rolled"]):
                matched_groups.add(group)
    return matched_groups


def choose_period_and_value(row):
    period = None
    for key, value in row.items():
        if key.endswith("_code") or key.endswith("_label"):
            p = normalize_period(value)
            if p:
                period = p
                break
    if not period:
        return None, None
    val = row.get("value")
    if val in (None, ":"):
        return None, None
    try:
        return period, float(val)
    except Exception:
        return None, None


def fetch_monthly_rows_from_dataset(dataset):
    # Best-effort query. We intentionally keep this generic because exact dimension names
    # can vary across Eurostat/COMEXT payloads.
    params = {
        "freq": "M",
        "unit": "MIO_EUR",
    }
    url = build_eurostat_url(dataset, params)
    try:
        payload = fetch_json_url(url, timeout=45)
        return jsonstat_to_rows(payload)
    except Exception:
        try:
            payload = fetch_json_url(build_eurostat_url(dataset, {}), timeout=45)
            return jsonstat_to_rows(payload)
        except Exception:
            return []


def aggregate_monthly_bundle():
    exports = {}
    imports = {}
    rows_seen = 0

    for dataset in COMEXT_MONTHLY_DATASET_CANDIDATES:
        rows = fetch_monthly_rows_from_dataset(dataset)
        if not rows:
            continue
        for row in rows:
            period, value = choose_period_and_value(row)
            if not period:
                continue
            partner = row_matches_partner(row)
            if not partner:
                continue
            flow = detect_flow(row, dataset_name=dataset)
            if flow not in ("imports", "exports"):
                continue
            groups = row_matches_product_group(row)
            if not groups:
                continue
            # simple equal-weight contribution across matched groups
            group_weight = 1.0 / max(1, len(groups))
            weighted_value = value * group_weight
            if flow == "exports":
                exports[period] = exports.get(period, 0.0) + weighted_value
            else:
                imports[period] = imports.get(period, 0.0) + weighted_value
            rows_seen += 1

    if rows_seen == 0:
        return None

    periods = sorted(set(exports.keys()) | set(imports.keys()), key=period_sort_key)
    periods = periods[-MONTHLY_WINDOW:]
    imports_series_raw = [{"period": p, "value": round(imports.get(p, 0.0), 1)} for p in periods]
    exports_series_raw = [{"period": p, "value": round(exports.get(p, 0.0), 1)} for p in periods]
    balanced_raw = [
        {"period": p, "value": round(EXPORT_WEIGHT * exports.get(p, 0.0) + IMPORT_WEIGHT * imports.get(p, 0.0), 1)}
        for p in periods
    ]
    imports_series = moving_average_points(imports_series_raw, MONTHLY_SMOOTHING)
    exports_series = moving_average_points(exports_series_raw, MONTHLY_SMOOTHING)
    balanced_series = moving_average_points(balanced_raw, MONTHLY_SMOOTHING)
    latest_period = balanced_series[-1]["period"] if balanced_series else None
    latest_value = balanced_series[-1]["value"] if balanced_series else None
    yoy_pct = yoy_from_monthly(balanced_series)

    return {
        "status": "Live",
        "scope": "EU + selected partners | imports + exports | 60/40 weighting",
        "partners": PARTNERS,
        "flows": {"exports": EXPORT_WEIGHT, "imports": IMPORT_WEIGHT},
        "window_months": MONTHLY_WINDOW,
        "smoothing": f"{MONTHLY_SMOOTHING}M MA",
        "latest_period": latest_period,
        "latest_value": latest_value,
        "latest_raw_balanced": balanced_raw[-1]["value"] if balanced_raw else None,
        "yoy_pct": yoy_pct,
        "methodology": "Monthly balanced bundle aggregated from COMEXT monthly rows using HS4 anchors (8501, 8504, 7219, 7326), EU + selected partners, exports weighted above imports, then smoothed with a 3-month moving average.",
        "note": "Best-effort monthly proxy. It is a trade pulse, not a direct measure of Cell Impact revenue or orders.",
        "balanced_series": balanced_series,
        "balanced_raw_series": balanced_raw,
        "exports_series": exports_series,
        "imports_series": imports_series,
        "source_rows_used": rows_seen,
        "source_datasets": COMEXT_MONTHLY_DATASET_CANDIDATES,
    }


def ensure_bundle_metadata(data):
    cm = data.setdefault("customs_monitor", {})
    cm["coverage_note"] = "Annual macro/proxy remains live. Monthly balanced bundle is layered on top as a faster but noisier trade pulse."
    cm["dataset_target"] = EUROSTAT_DATASET
    cm["next_step_note"] = "Next step: refine the metals/forming proxy or tighten product-level filters if live monthly bundle quality improves."
    cm["bundle_definition"] = {
        "status": "Configured",
        "bundle_name": "Balanced EU + partner hydrogen hardware bundle",
        "dataset_target": "DS-059322 + DS-059332",
        "partners": PARTNERS,
        "flows": "imports + exports",
        "weights": {"exports": EXPORT_WEIGHT, "imports": IMPORT_WEIGHT},
        "why": "Primary monthly trade pulse for a faster read than the annual macro proxy, combining a hydrogen hardware anchor with a cautious metals/forming proxy.",
        "limitation": "Proxy signal only. It does not directly measure Cell Impact revenue or orders.",
        "codes": [
            {"group": "hydrogen_anchor", "code": "8501", "level": "HS4", "label": "Electric motors and generators"},
            {"group": "hydrogen_anchor", "code": "8504", "level": "HS4", "label": "Electrical transformers and converters"},
            {"group": "metals_forming_proxy", "code": "7219", "level": "HS4", "label": "Stainless steel flat-rolled products"},
            {"group": "metals_forming_proxy", "code": "7326", "level": "HS4", "label": "Other articles of iron or steel"},
        ],
    }


def recompute_trade_index(data):
    cm = data.setdefault("customs_monitor", {})
    live_series = fetch_live_customs_series()
    if live_series:
        cm["series"] = live_series
        cm["methodology"] = "Live Eurostat dataset tet00013 mapped via JSON-stat payload parser."
        cm["source_type"] = "single-dataset proxy"
    else:
        cm["methodology"] = "Fallback proxy series active. JSON-stat payload mapper is present, but no verified live match was extracted."
        cm["source_type"] = "fallback proxy"

    ensure_bundle_metadata(data)

    series = cm.get("series", {})
    latest_vals = [arr[-1]["value"] for arr in series.values() if arr]
    if latest_vals:
        avg = sum(latest_vals) / len(latest_vals)
        prev_vals = [arr[-2]["value"] for arr in series.values() if len(arr) >= 2]
        prev = sum(prev_vals) / len(prev_vals) if prev_vals else avg
        yoy = ((avg - prev) / prev * 100) if prev else 0
        cm["latest_index"] = round(avg, 1)
        cm["latest_year"] = max(arr[-1]["year"] for arr in series.values() if arr)
        cm["yoy_pct"] = round(yoy, 1)
        data.setdefault("market_signals", {})["hydrogen_trade_pulse"] = {
            "value": f"Trade index {round(avg,1)}",
            "meta": "Annual macro/proxy backdrop from tracker series.",
            "status": "Live" if live_series else "Proxy",
        }


def build_monthly_trade_pulse(data):
    existing = data.get("monthly_trade_pulse", {})
    live = aggregate_monthly_bundle()
    if live:
        data["monthly_trade_pulse"] = live
        data.setdefault("trade_signals", {})
        data["trade_signals"]["balanced_signal"] = {
            "status": "Live",
            "scope": "EU + selected partners",
            "flows": "imports + exports",
            "weights": {"exports": EXPORT_WEIGHT, "imports": IMPORT_WEIGHT},
            "value": live.get("latest_value"),
            "latest_period": live.get("latest_period"),
            "yoy_pct": live.get("yoy_pct"),
            "note": "Primary monthly trade pulse after 3M smoothing.",
        }
        last_exports = live["exports_series"][-1]["value"] if live.get("exports_series") else None
        last_imports = live["imports_series"][-1]["value"] if live.get("imports_series") else None
        data["trade_signals"]["exports_signal"] = {
            "status": "Live",
            "scope": "EU + selected partners",
            "flows": "exports",
            "value": last_exports,
            "note": "Exports-only 3M smoothed signal.",
        }
        data["trade_signals"]["imports_signal"] = {
            "status": "Live",
            "scope": "EU + selected partners",
            "flows": "imports",
            "value": last_imports,
            "note": "Imports-only 3M smoothed signal.",
        }
    else:
        data["monthly_trade_pulse"] = {
            "status": existing.get("status", "Pending live extraction"),
            "scope": "EU + selected partners | imports + exports | 60/40 weighting",
            "partners": PARTNERS,
            "flows": {"exports": EXPORT_WEIGHT, "imports": IMPORT_WEIGHT},
            "window_months": MONTHLY_WINDOW,
            "smoothing": f"{MONTHLY_SMOOTHING}M MA",
            "latest_period": existing.get("latest_period"),
            "latest_value": existing.get("latest_value"),
            "latest_raw_balanced": existing.get("latest_raw_balanced"),
            "yoy_pct": existing.get("yoy_pct"),
            "methodology": existing.get("methodology", "Monthly COMEXT extraction did not complete on this run. Existing monthly pulse, if any, is preserved."),
            "note": existing.get("note", "Monthly trade pulse remains a best-effort proxy until the COMEXT monthly extraction returns a stable live series."),
            "balanced_series": existing.get("balanced_series", []),
            "balanced_raw_series": existing.get("balanced_raw_series", []),
            "exports_series": existing.get("exports_series", []),
            "imports_series": existing.get("imports_series", []),
            "source_datasets": COMEXT_MONTHLY_DATASET_CANDIDATES,
        }
        data.setdefault("trade_signals", {})
        data["trade_signals"]["balanced_signal"] = {
            "status": "Pending live extraction" if not existing.get("balanced_series") else "Fallback",
            "scope": "EU + selected partners",
            "flows": "imports + exports",
            "weights": {"exports": EXPORT_WEIGHT, "imports": IMPORT_WEIGHT},
            "value": existing.get("latest_value"),
            "latest_period": existing.get("latest_period"),
            "yoy_pct": existing.get("yoy_pct"),
            "note": "Monthly balanced bundle waits for a successful live extraction. Existing monthly data is preserved when available.",
        }
        data["trade_signals"]["exports_signal"] = {
            "status": "Pending live extraction" if not existing.get("exports_series") else "Fallback",
            "scope": "EU + selected partners",
            "flows": "exports",
            "value": existing.get("exports_series", [])[-1]["value"] if existing.get("exports_series") else None,
            "note": "Exports-only 3M smoothed signal.",
        }
        data["trade_signals"]["imports_signal"] = {
            "status": "Pending live extraction" if not existing.get("imports_series") else "Fallback",
            "scope": "EU + selected partners",
            "flows": "imports",
            "value": existing.get("imports_series", [])[-1]["value"] if existing.get("imports_series") else None,
            "note": "Imports-only 3M smoothed signal.",
        }

    data["trade_signals"]["macro_signal"] = {
        "status": "Live" if data.get("customs_monitor", {}).get("source_type") == "single-dataset proxy" else "Proxy",
        "dataset_target": EUROSTAT_DATASET,
        "value": data.get("customs_monitor", {}).get("latest_index"),
        "yoy_pct": data.get("customs_monitor", {}).get("yoy_pct"),
        "note": "Annual macro/proxy backdrop.",
    }


def build_what_matters_now(data):
    runway = float(data.get("company", {}).get("runway_months", 0) or 0)
    pressure = data.get("market_signals", {}).get("dilution_pressure", {}).get("value", "Elevated")
    trade = data.get("customs_monitor", {}).get("yoy_pct", 0)
    monthly = data.get("monthly_trade_pulse", {})
    monthly_note = ""
    if monthly.get("latest_period") and monthly.get("latest_value") is not None:
        monthly_note = f" Monthly pulse ({monthly['latest_period']}) sits at {monthly['latest_value']:.1f} after smoothing."
    data["what_matters_now"] = [
        {"label": "Cash runway", "value": "Short-to-moderate" if runway < 12 else "More comfortable", "note": f"Current tracker baseline implies roughly {runway:.1f} months of runway."},
        {"label": "Funding pressure", "value": pressure, "note": "Funding pressure remains meaningful until commercial progress clearly reduces financing risk."},
        {"label": "Commercial signal", "value": "Early but improving", "note": "Recent investor-page headlines suggest movement, but stronger recurring production proof is still needed." + monthly_note},
        {"label": "Biggest current risk", "value": "Dilution before scale", "note": f"Even with annual trade backdrop at {trade:+.1f}% YoY, the key company-specific risk is financing before scale-up."},
    ]


def main():
    data = load_data()
    changes = []
    data.setdefault("meta", {})
    data["meta"]["last_update"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    data["meta"]["data_source"] = "GitHub Pages / same-repo JSON"
    changes.append("Tracker refreshed")

    try:
        html_text = fetch_text(IR_URL)
        headlines = parse_ir_headlines(html_text)
        if headlines:
            data["ir_headlines"] = headlines[:3]
            data.setdefault("market_signals", {})["latest_ir_signal"] = {
                "title": headlines[0]["title"],
                "meta": "Latest headline from Cell Impact investor page",
                "status": "Watch",
            }
            changes.append("IR headlines updated")
    except Exception:
        pass

    price = fetch_price()
    if price:
        data.setdefault("market_signals", {})["share_price"] = price
        changes.append("Share price updated")

    recompute_trade_index(data)
    changes.append("Trade index recalculated")

    build_monthly_trade_pulse(data)
    changes.append("Monthly balanced bundle refreshed")

    build_what_matters_now(data)
    changes.append("What matters now updated")

    data["changes"] = changes
    save_data(data)
    print("Tracker JSON updated.")


if __name__ == "__main__":
    main()
