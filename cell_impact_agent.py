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

# Annual macro/proxy (kept live)
EUROSTAT_API_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
EUROSTAT_DATASET = "tet00013"
EUROSTAT_QUERY = {}

# Monthly scaffold target (best-effort only)
COMEXT_API_BASE = "https://ec.europa.eu/eurostat/api/comext/dissemination/statistics/1.0/data"
COMEXT_DATASETS = ["DS-059322", "DS-059332"]
PARTNERS = ["US", "CN", "JP", "KR"]
BUNDLE_CODES = [
    {"group": "hydrogen_anchor", "code": "8501", "level": "HS4", "label": "Electric motors and generators"},
    {"group": "hydrogen_anchor", "code": "8504", "level": "HS4", "label": "Electrical transformers and converters"},
    {"group": "metals_forming_proxy", "code": "7219", "level": "HS4", "label": "Stainless steel flat-rolled products"},
    {"group": "metals_forming_proxy", "code": "7326", "level": "HS4", "label": "Other articles of iron or steel"},
]
MONTHLY_WINDOW = 18
MONTHLY_SMOOTHING = 3


def fetch_text(url, timeout=25):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return r.read().decode("utf-8", errors="ignore")


def fetch_json_url(url, timeout=25):
    return json.loads(fetch_text(url, timeout=timeout))


def build_stats_url(dataset, params=None, base=EUROSTAT_API_BASE):
    if not params:
        return f"{base}/{dataset}?format=JSON"
    query = urllib.parse.urlencode(params, doseq=True)
    return f"{base}/{dataset}?{query}&format=JSON"


def load_data():
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_data(data):
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def parse_ir_headlines(page_html):
    matches = re.findall(r'href="([^"]+)".{0,300}?>([^<]{12,180})<', page_html, flags=re.I | re.S)
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

    if isinstance(values, list):
        iterable = enumerate(values)
    else:
        iterable = ((int(k), v) for k, v in values.items())

    for flat_idx, value in iterable:
        coords = decode_coords(flat_index=flat_idx, sizes=sizes)
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
    m = re.search(r"((?:19|20)\d{2})[-]?([01]\d)", str(s))
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m2 = re.search(r"(20\d{2}|19\d{2})M([01]?\d)", str(s))
    if m2:
        return f"{m2.group(1)}-{int(m2.group(2)):02d}"
    return None


def choose_time_field(row):
    for k in row.keys():
        if k.endswith("_code") or k.endswith("_label"):
            base = k[:-5]
            if base.lower() in ("time", "time_period", "period", "year", "month"):
                return base
    for k, v in row.items():
        if k.endswith("_label") and (normalize_year(v) or normalize_period(v)):
            return k[:-6]
    return None


def score_row(row):
    text = " ".join(str(v).lower() for v in row.values() if isinstance(v, (str, int, float)))
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
    url = build_stats_url(EUROSTAT_DATASET, EUROSTAT_QUERY)
    try:
        payload = fetch_json_url(url)
        return eurostat_series_from_payload(payload)
    except Exception:
        return None


def recompute_trade_index(data):
    cm = data.setdefault("customs_monitor", {})
    live_series = fetch_live_customs_series()
    if live_series:
        cm["series"] = live_series
        cm["methodology"] = "Live Eurostat dataset tet00013 mapped via JSON-stat payload parser."
        cm["source_type"] = "single-dataset proxy"
        cm["coverage_note"] = "This is a customs proxy signal, not a complete global fuel-cell trade dataset."
    else:
        cm["methodology"] = "Fallback proxy series active. JSON-stat payload mapper is present, but no verified live match was extracted."
        cm["source_type"] = "fallback proxy"
        cm["coverage_note"] = "Fallback proxy series remain active until a verified live extraction is available."
    cm["dataset_target"] = EUROSTAT_DATASET
    cm["comext_dataset_target"] = " + ".join(COMEXT_DATASETS)
    cm["next_step_note"] = "Next step: replace or supplement tet00013 with a verified COMEXT monthly bundle."
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
            "value": f"Trade index {round(avg, 1)}",
            "meta": "Customs index based on tracker series.",
            "status": "Live" if live_series else "Proxy"
        }


def moving_average_points(points, window=3):
    out = []
    for i in range(len(points)):
        subset = points[max(0, i - window + 1): i + 1]
        vals = [float(p["value"]) for p in subset if p.get("value") is not None]
        if vals:
            out.append({"period": points[i]["period"], "value": round(sum(vals) / len(vals), 1)})
    return out


def ensure_monthly_structures(data):
    cm = data.setdefault("customs_monitor", {})
    cm["bundle_definition"] = {
        "status": "Configured",
        "bundle_name": "Balanced EU + partner hydrogen hardware bundle",
        "dataset_target": " + ".join(COMEXT_DATASETS),
        "partners": PARTNERS,
        "flows": "imports + exports",
        "weights": {"exports": 0.6, "imports": 0.4},
        "why": "Faster customs pulse than the annual macro proxy, combining a hydrogen hardware anchor with a cautious metals/forming proxy.",
        "limitation": "Proxy signal only. It does not directly measure Cell Impact revenue or orders.",
        "codes": BUNDLE_CODES
    }
    data["trade_signals"] = {
        "balanced_signal": {
            "status": "Pending live run",
            "scope": "EU + selected partners",
            "flows": "imports + exports",
            "weights": {"exports": 0.6, "imports": 0.4},
            "note": "Primary monthly trade pulse. Will update after the agent completes a successful COMEXT monthly extraction."
        },
        "exports_signal": {
            "status": "Pending live run",
            "scope": "EU + selected partners",
            "flows": "exports",
            "note": "Closer commercial-direction signal for an EU-based supplier."
        },
        "imports_signal": {
            "status": "Pending live run",
            "scope": "EU + selected partners",
            "flows": "imports",
            "note": "Demand-environment signal."
        },
        "macro_signal": {
            "status": "Live",
            "dataset_target": EUROSTAT_DATASET,
            "value": cm.get("latest_index"),
            "yoy_pct": cm.get("yoy_pct"),
            "note": "Annual macro/proxy backdrop."
        }
    }
    data["monthly_trade_pulse"] = {
        "status": "Pending live run",
        "scope": "EU + selected partners | imports + exports | 60/40 weighting",
        "partners": PARTNERS,
        "flows": {"exports": 0.6, "imports": 0.4},
        "window_months": MONTHLY_WINDOW,
        "smoothing": f"{MONTHLY_SMOOTHING}M MA",
        "latest_period": None,
        "latest_value": None,
        "latest_raw_balanced": None,
        "yoy_pct": None,
        "methodology": "Monthly balanced bundle. Shows the last 18 months after 3-month moving-average smoothing when live COMEXT extraction succeeds.",
        "note": "Starts empty until the updated agent writes live monthly series.",
        "balanced_series": [],
        "balanced_raw_series": [],
        "exports_series": [],
        "imports_series": [],
        "debug": {}
    }


def update_monthly_error_state(data, message, attempted_urls=None):
    pulse = data.setdefault("monthly_trade_pulse", {})
    pulse["status"] = "Fetch failed"
    pulse["note"] = message
    pulse["debug"] = {
        "message": message,
        "attempted_urls": attempted_urls or [],
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    }
    ts = data.setdefault("trade_signals", {})
    if "balanced_signal" in ts:
        ts["balanced_signal"]["status"] = "Fetch failed"
        ts["balanced_signal"]["note"] = message
    if "exports_signal" in ts:
        ts["exports_signal"]["status"] = "Fetch failed"
        ts["exports_signal"]["note"] = message
    if "imports_signal" in ts:
        ts["imports_signal"]["status"] = "Fetch failed"
        ts["imports_signal"]["note"] = message


def build_monthly_from_payload(payload):
    """
    Best-effort generic extractor:
    - flattens JSON-stat rows
    - identifies a monthly time dimension
    - builds three series if flow labels can be recognized
    This is intentionally cautious: if it cannot find a credible mapping,
    it returns None and the tracker stays on annual macro only.
    """
    rows = jsonstat_to_rows(payload)
    if not rows:
        return None
    time_field = choose_time_field(rows[0])
    if not time_field:
        return None

    period_key_code = f"{time_field}_code"
    period_key_label = f"{time_field}_label"

    def detect_flow(row):
        text = " ".join(str(v).lower() for k, v in row.items() if k.endswith("_label") or k.endswith("_code"))
        if "export" in text:
            return "exports"
        if "import" in text:
            return "imports"
        return None

    grouped = {"exports": {}, "imports": {}}
    attempted_periods = 0
    for row in rows:
        period = normalize_period(row.get(period_key_code)) or normalize_period(row.get(period_key_label))
        if not period:
            continue
        attempted_periods += 1
        flow = detect_flow(row)
        if not flow:
            continue
        val = row.get("value")
        if val in (None, ":"):
            continue
        try:
            val = float(val)
        except Exception:
            continue
        # For each period/flow take the largest positive value row
        current = grouped[flow].get(period)
        if current is None or val > current:
            grouped[flow][period] = val

    if attempted_periods == 0:
        return None

    periods = sorted(set(grouped["exports"].keys()) | set(grouped["imports"].keys()))
    if len(periods) < 6:
        return None

    exports = [{"period": p, "value": round(grouped["exports"].get(p), 1)} for p in periods if p in grouped["exports"]]
    imports = [{"period": p, "value": round(grouped["imports"].get(p), 1)} for p in periods if p in grouped["imports"]]

    # Build aligned balanced raw series on union of periods
    balanced_raw = []
    for p in periods:
        ex = grouped["exports"].get(p)
        im = grouped["imports"].get(p)
        if ex is None and im is None:
            continue
        ex = ex if ex is not None else 0.0
        im = im if im is not None else 0.0
        balanced_raw.append({"period": p, "value": round(0.6 * ex + 0.4 * im, 1)})

    if len(balanced_raw) < 6:
        return None

    balanced = moving_average_points(balanced_raw, window=MONTHLY_SMOOTHING)

    return {
        "balanced_series": balanced[-MONTHLY_WINDOW:],
        "balanced_raw_series": balanced_raw[-MONTHLY_WINDOW:],
        "exports_series": exports[-MONTHLY_WINDOW:],
        "imports_series": imports[-MONTHLY_WINDOW:]
    }


def attempt_monthly_comext():
    """
    Best-effort monthly extractor.
    Uses official Comext endpoint but several cautious candidate queries.
    Because DS-prefixed datasets require filtering and exact dimensions can vary,
    failure is reported back into tracker.json instead of silently leaving a pending scaffold.
    """
    attempted_urls = []
    # Candidate queries are intentionally minimal and broad.
    # They may still fail if dimension names differ; that failure is surfaced in JSON debug.
    candidate_queries = [
        {"freq": "M"},
        {"freq": "M", "geo": "EU27_2020"},
    ]
    last_error = None

    for dataset in COMEXT_DATASETS:
        for params in candidate_queries:
            url = build_stats_url(dataset, params=params, base=COMEXT_API_BASE)
            attempted_urls.append(url)
            try:
                payload = fetch_json_url(url, timeout=35)
                result = build_monthly_from_payload(payload)
                if result:
                    return result, attempted_urls, None
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"

    return None, attempted_urls, last_error or "No usable monthly COMEXT payload extracted."


def recompute_monthly_trade_pulse(data):
    ensure_monthly_structures(data)
    result, attempted_urls, err = attempt_monthly_comext()
    pulse = data["monthly_trade_pulse"]
    signals = data["trade_signals"]

    if not result:
        update_monthly_error_state(
            data,
            message=f"Monthly COMEXT extraction did not yield a usable series yet. {err or ''}".strip(),
            attempted_urls=attempted_urls,
        )
        return False

    pulse["status"] = "Live"
    pulse["note"] = "Monthly balanced bundle is live."
    pulse["balanced_series"] = result["balanced_series"]
    pulse["balanced_raw_series"] = result["balanced_raw_series"]
    pulse["exports_series"] = result["exports_series"]
    pulse["imports_series"] = result["imports_series"]
    pulse["latest_period"] = pulse["balanced_series"][-1]["period"] if pulse["balanced_series"] else None
    pulse["latest_value"] = pulse["balanced_series"][-1]["value"] if pulse["balanced_series"] else None
    pulse["latest_raw_balanced"] = pulse["balanced_raw_series"][-1]["value"] if pulse["balanced_raw_series"] else None

    # YoY: compare against same month one year earlier when present
    yoy = None
    if pulse["balanced_series"] and len(pulse["balanced_series"]) >= 13:
        by_period = {p["period"]: p["value"] for p in pulse["balanced_series"]}
        latest = pulse["latest_period"]
        if latest and re.match(r"^\d{4}-\d{2}$", latest):
            year, month = latest.split("-")
            prev = f"{int(year)-1:04d}-{month}"
            if prev in by_period and by_period[prev]:
                yoy = ((pulse["latest_value"] - by_period[prev]) / by_period[prev]) * 100
    pulse["yoy_pct"] = round(yoy, 1) if yoy is not None else None
    pulse["debug"] = {
        "message": "Live monthly series extracted.",
        "attempted_urls": attempted_urls,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    }

    signals["balanced_signal"]["status"] = "Live"
    signals["balanced_signal"]["value"] = pulse["latest_value"]
    signals["balanced_signal"]["latest_period"] = pulse["latest_period"]
    signals["balanced_signal"]["note"] = "Primary monthly trade pulse with 3M smoothing."

    signals["exports_signal"]["status"] = "Live" if pulse["exports_series"] else "Partial"
    signals["exports_signal"]["value"] = pulse["exports_series"][-1]["value"] if pulse["exports_series"] else None
    signals["exports_signal"]["latest_period"] = pulse["exports_series"][-1]["period"] if pulse["exports_series"] else None

    signals["imports_signal"]["status"] = "Live" if pulse["imports_series"] else "Partial"
    signals["imports_signal"]["value"] = pulse["imports_series"][-1]["value"] if pulse["imports_series"] else None
    signals["imports_signal"]["latest_period"] = pulse["imports_series"][-1]["period"] if pulse["imports_series"] else None
    return True


def build_what_matters_now(data):
    runway = float(data.get("company", {}).get("runway_months", 0) or 0)
    pressure = data.get("market_signals", {}).get("dilution_pressure", {}).get("value", "Elevated")
    trade = data.get("customs_monitor", {}).get("yoy_pct", 0)
    data["what_matters_now"] = [
        {
            "label": "Cash runway",
            "value": "Short-to-moderate" if runway < 12 else "More comfortable",
            "note": f"Current tracker baseline implies roughly {runway:.1f} months of runway."
        },
        {
            "label": "Funding pressure",
            "value": pressure,
            "note": "Funding pressure remains meaningful until commercial progress clearly reduces financing risk."
        },
        {
            "label": "Commercial signal",
            "value": "Early but improving",
            "note": "Recent investor-page headlines suggest movement, but stronger recurring production proof is still needed."
        },
        {
            "label": "Biggest current risk",
            "value": "Dilution before scale",
            "note": f"Even with trade backdrop at {trade:+.1f}% YoY, the key company-specific risk is financing before scale-up."
        }
    ]


def main():
    data = load_data()
    changes = []

    data.setdefault("meta", {})
    data["meta"]["last_update"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    data["meta"]["data_source"] = "GitHub Pages / same-repo JSON"

    changes.append("Tracker refreshed")

    try:
        page_html = fetch_text(IR_URL)
        headlines = parse_ir_headlines(page_html)
        if headlines:
            data["ir_headlines"] = headlines[:3]
            data.setdefault("market_signals", {})["latest_ir_signal"] = {
                "title": headlines[0]["title"],
                "meta": "Latest headline from Cell Impact investor page",
                "status": "Watch"
            }
            changes.append("IR headlines updated")
    except Exception as exc:
        changes.append(f"IR headlines fetch failed: {type(exc).__name__}")

    price = fetch_price()
    if price:
        data.setdefault("market_signals", {})["share_price"] = price
        changes.append("Share price updated")

    recompute_trade_index(data)
    changes.append("Trade index recalculated")

    monthly_live = recompute_monthly_trade_pulse(data)
    changes.append("Monthly balanced bundle updated" if monthly_live else "Monthly balanced bundle fetch failed")

    build_what_matters_now(data)
    changes.append("What matters now updated")

    data["changes"] = changes
    save_data(data)
    print("Tracker JSON updated.")


if __name__ == "__main__":
    main()
