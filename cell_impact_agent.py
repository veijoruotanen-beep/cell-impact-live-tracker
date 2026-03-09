#!/usr/bin/env python3
import html
import json
import math
import re
import ssl
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

DATA_PATH = "data/tracker.json"
IR_URL = "https://investor.cellimpact.com/en/investor-relations"
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/CI.ST?range=5d&interval=1d"

EUROSTAT_API_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
EUROSTAT_DATASET = "tet00013"
EUROSTAT_QUERY = {}

COMEXT_STATS_BASE = "https://ec.europa.eu/eurostat/api/comext/dissemination/statistics/1.0/data"
COMEXT_SDMX21_BASE = "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1"
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


def fetch_text(url, timeout=30):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return r.read().decode("utf-8", errors="ignore")


def fetch_json_url(url, timeout=30):
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
    out = {}
    for code, pos in idx.items():
        out[str(code)] = {"pos": int(pos), "label": labels.get(str(code), str(code))}
    return out


def decode_coords(flat_index, sizes):
    coords = []
    for size in reversed(sizes):
        coords.append(flat_index % size)
        flat_index //= size
    return list(reversed(coords))


def build_pos_to_code(dim_obj):
    cmap = jsonstat_category_map(dim_obj)
    return {meta["pos"]: (code, meta["label"]) for code, meta in cmap.items()}


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


def choose_time_field(row):
    for k in row.keys():
        if k.endswith("_code") or k.endswith("_label"):
            base = k[:-5] if k.endswith("_code") else k[:-6]
            if base.lower() in ("time", "time_period", "period", "year"):
                return base
    for k, v in row.items():
        if k.endswith("_label") and normalize_year(v):
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
            val = row.get("value")
            if val in (None, ":"):
                continue
            try:
                val = float(val)
            except Exception:
                continue
            grouped.setdefault(year, []).append((score_row(row), val, row))
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
    url = build_stats_url(EUROSTAT_DATASET, EUROSTAT_QUERY, EUROSTAT_API_BASE)
    try:
        payload = fetch_json_url(url)
        return eurostat_series_from_payload(payload)
    except Exception:
        return None


def ensure_trade_structures(data):
    data.setdefault("customs_monitor", {})
    cm = data["customs_monitor"]
    cm.setdefault("bundle_definition", {
        "status": "Configured",
        "bundle_name": "Balanced EU + partner hydrogen hardware bundle",
        "dataset_target": "DS-059322 + DS-059332",
        "partners": PARTNERS,
        "flows": "imports + exports",
        "weights": {"exports": 0.6, "imports": 0.4},
        "why": "Faster customs pulse than the annual macro proxy, combining a hydrogen hardware anchor with a cautious metals/forming proxy.",
        "limitation": "Proxy signal only. It does not directly measure Cell Impact revenue or orders.",
        "codes": BUNDLE_CODES,
    })
    cm["source_type"] = "single-dataset proxy"
    cm["coverage_note"] = "This is a customs proxy signal, not a complete global fuel-cell trade dataset."
    cm["dataset_target"] = EUROSTAT_DATASET
    cm["next_step_note"] = "Next step: replace or supplement tet00013 with a verified COMEXT monthly bundle."
    cm["comext_dataset_target"] = "DS-059322 + DS-059332"

    data.setdefault("trade_signals", {})
    ts = data["trade_signals"]
    ts.setdefault("balanced_signal", {
        "status": "Pending live run",
        "scope": "EU + selected partners",
        "flows": "imports + exports",
        "weights": {"exports": 0.6, "imports": 0.4},
        "note": "Primary monthly trade pulse. Will update after the agent completes a successful COMEXT monthly extraction."
    })
    ts.setdefault("exports_signal", {
        "status": "Pending live run",
        "scope": "EU + selected partners",
        "flows": "exports",
        "note": "Closer commercial-direction signal for an EU-based supplier."
    })
    ts.setdefault("imports_signal", {
        "status": "Pending live run",
        "scope": "EU + selected partners",
        "flows": "imports",
        "note": "Demand-environment signal."
    })
    ts.setdefault("macro_signal", {
        "status": "Live",
        "dataset_target": EUROSTAT_DATASET,
        "value": None,
        "yoy_pct": None,
        "note": "Annual macro/proxy backdrop."
    })

    data.setdefault("monthly_trade_pulse", {
        "status": "Pending live run",
        "scope": "EU + selected partners | imports + exports | 60/40 weighting",
        "partners": PARTNERS,
        "flows": {"exports": 0.6, "imports": 0.4},
        "window_months": MONTHLY_WINDOW,
        "smoothing": "3M MA",
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
        "debug": {},
    })


def recompute_trade_index(data):
    ensure_trade_structures(data)
    cm = data["customs_monitor"]
    live_series = fetch_live_customs_series()
    if live_series:
        cm["series"] = live_series
        cm["methodology"] = "Live Eurostat dataset tet00013 mapped via JSON-stat payload parser."
    else:
        cm["methodology"] = "Fallback proxy series active. JSON-stat payload mapper is present, but no verified live match was extracted."
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
        data["market_signals"]["hydrogen_trade_pulse"] = {
            "value": f"Trade index {round(avg,1)}",
            "meta": "Customs index based on tracker series.",
            "status": "Live" if live_series else "Proxy"
        }
        data["trade_signals"]["macro_signal"]["value"] = round(avg, 1)
        data["trade_signals"]["macro_signal"]["yoy_pct"] = round(yoy, 1)


def _safe_xml(url, timeout=35):
    txt = fetch_text(url, timeout=timeout)
    return ET.fromstring(txt), txt


def _strip_ns(tag):
    return tag.split("}", 1)[-1]


def probe_comext_metadata(dataset):
    debug = {
        "dataset": dataset,
        "dataflow_url": f"{COMEXT_SDMX21_BASE}/dataflow/ESTAT/{dataset}/latest?references=all",
        "datastructure_url": f"{COMEXT_SDMX21_BASE}/datastructure/ESTAT/{dataset}/latest?references=all",
        "contentconstraint_url": f"{COMEXT_SDMX21_BASE}/contentconstraint/ESTAT/{dataset}/latest?references=all",
    }
    try:
        root, _ = _safe_xml(debug["dataflow_url"])
        ids = []
        for el in root.iter():
            if _strip_ns(el.tag).lower() == "dataflow":
                if el.attrib.get("id"):
                    ids.append(el.attrib["id"])
        debug["dataflow_ids"] = ids
    except Exception as exc:
        debug["dataflow_error"] = f"{type(exc).__name__}: {exc}"

    try:
        root, _ = _safe_xml(debug["datastructure_url"])
        dims = []
        for el in root.iter():
            name = _strip_ns(el.tag).lower()
            if name in ("dimension", "timedimension"):
                dim_id = el.attrib.get("id")
                if dim_id:
                    dims.append(dim_id)
        debug["dimensions"] = sorted(set(dims))
    except Exception as exc:
        debug["datastructure_error"] = f"{type(exc).__name__}: {exc}"

    try:
        root, _ = _safe_xml(debug["contentconstraint_url"])
        member_keys = []
        for el in root.iter():
            if _strip_ns(el.tag).lower() == "keyvalue":
                mv = el.attrib.get("id") or el.attrib.get("value")
                if mv:
                    member_keys.append(mv)
        debug["constraint_keyvalues_found"] = sorted(set(member_keys))[:50]
    except Exception as exc:
        debug["contentconstraint_error"] = f"{type(exc).__name__}: {exc}"

    return debug


def month_key(period):
    s = str(period)
    m = re.search(r"(\d{4})[-]?M?(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m = re.search(r"(\d{4})[-]?(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return None


def moving_average(values, window=3):
    out = []
    for i in range(len(values)):
        chunk = values[max(0, i - window + 1):i + 1]
        nums = [v for v in chunk if v is not None]
        out.append(sum(nums) / len(nums) if nums else None)
    return out


def _row_period_field(row):
    for key in row.keys():
        if key.endswith("_code"):
            base = key[:-5]
            if base.lower() in ("time", "period", "time_period"):
                return base
    return None


def _classify_flow(raw):
    s = str(raw).upper()
    if any(x in s for x in ("EXP", "EXPORT")):
        return "exports"
    if any(x in s for x in ("IMP", "IMPORT")):
        return "imports"
    return None


def _sum_by_period(rows):
    if not rows:
        return {}
    time_field = _row_period_field(rows[0])
    if not time_field:
        return {}
    out = {}
    flow_field = None
    for k in rows[0].keys():
        if k.endswith("_code") and ("flow" in k.lower() or "indic" in k.lower()):
            flow_field = k
            break
    for row in rows:
        p = month_key(row.get(f"{time_field}_code") or row.get(f"{time_field}_label"))
        if not p:
            continue
        val = row.get("value")
        if val in (None, ":"):
            continue
        try:
            val = float(val)
        except Exception:
            continue
        flow = _classify_flow(row.get(flow_field)) if flow_field else None
        if flow not in ("imports", "exports"):
            text = " ".join(str(v) for v in row.values())
            flow = _classify_flow(text)
        if flow not in ("imports", "exports"):
            continue
        out.setdefault(flow, {})
        out[flow][p] = out[flow].get(p, 0.0) + val
    return out


def _periods_last_n(periods, n):
    periods = sorted(set(periods))
    return periods[-n:] if len(periods) > n else periods


def build_monthly_from_payload(payload):
    rows = jsonstat_to_rows(payload)
    sums = _sum_by_period(rows)
    if not sums.get("imports") or not sums.get("exports"):
        return None
    periods = _periods_last_n(set(sums["imports"].keys()) | set(sums["exports"].keys()), MONTHLY_WINDOW)
    if len(periods) < 6:
        return None

    exports_raw = [sums["exports"].get(p) for p in periods]
    imports_raw = [sums["imports"].get(p) for p in periods]
    balanced_raw = []
    for e, i in zip(exports_raw, imports_raw):
        if e is None and i is None:
            balanced_raw.append(None)
        else:
            balanced_raw.append((0.6 * (e or 0.0)) + (0.4 * (i or 0.0)))
    balanced_ma = moving_average(balanced_raw, MONTHLY_SMOOTHING)
    exports_ma = moving_average(exports_raw, MONTHLY_SMOOTHING)
    imports_ma = moving_average(imports_raw, MONTHLY_SMOOTHING)

    def pack(ps, vals):
        return [{"period": p, "value": round(v, 2)} for p, v in zip(ps, vals) if v is not None]

    return {
        "balanced_series": pack(periods, balanced_ma),
        "balanced_raw_series": pack(periods, balanced_raw),
        "exports_series": pack(periods, exports_ma),
        "imports_series": pack(periods, imports_ma),
    }


def build_candidate_param_sets():
    code_values = [c["code"] for c in BUNDLE_CODES]
    partner_values = PARTNERS
    flow_pairs = [
        (["EXP", "IMP"], "EXP+IMP"),
        (["EXP"], "EXP"),
        (["IMP"], "IMP"),
    ]
    candidates = []
    # deliberately tries multiple likely dimension names
    product_dim_names = ["product", "prod", "commodity", "hs", "cn", "sitc"]
    partner_dim_names = ["partner", "partner_country", "partner_geo", "partner_country_code"]
    flow_dim_names = ["flow", "indic_et", "trade", "stk_flow"]

    for flow_vals, label in flow_pairs:
        for prod_name in product_dim_names[:2]:
            params = {"freq": "M"}
            params[prod_name] = code_values
            params["geo"] = "EU27_2020"
            params["partner"] = partner_values
            params["flow"] = flow_vals
            candidates.append({"label": f"simple_{label}_{prod_name}", "params": params})

    # broader variants with alternative dim names
    for prod_name in product_dim_names:
        for partner_name in partner_dim_names:
            for flow_name in flow_dim_names:
                params = {"freq": "M", "geo": "EU27_2020"}
                params[prod_name] = code_values
                params[partner_name] = partner_values
                params[flow_name] = ["EXP", "IMP"]
                candidates.append({"label": f"{prod_name}_{partner_name}_{flow_name}", "params": params})
    return candidates[:18]


def attempt_monthly_comext():
    attempted = []
    metadata = {}
    for dataset in COMEXT_DATASETS:
        metadata[dataset] = probe_comext_metadata(dataset)

    last_error = None
    for dataset in COMEXT_DATASETS:
        for cand in build_candidate_param_sets():
            url = build_stats_url(dataset, params=cand["params"], base=COMEXT_STATS_BASE)
            attempted.append({"dataset": dataset, "label": cand["label"], "url": url})
            try:
                payload = fetch_json_url(url, timeout=40)
                result = build_monthly_from_payload(payload)
                if result:
                    return result, {"attempted": attempted, "metadata": metadata}, None
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"

    return None, {"attempted": attempted, "metadata": metadata}, last_error or "No usable monthly COMEXT payload extracted."


def update_monthly_error_state(data, message, debug_obj):
    ensure_trade_structures(data)
    pulse = data["monthly_trade_pulse"]
    signals = data["trade_signals"]
    pulse["status"] = "Fetch failed"
    pulse["latest_period"] = None
    pulse["latest_value"] = None
    pulse["latest_raw_balanced"] = None
    pulse["yoy_pct"] = None
    pulse["balanced_series"] = []
    pulse["balanced_raw_series"] = []
    pulse["exports_series"] = []
    pulse["imports_series"] = []
    pulse["note"] = message
    pulse["debug"] = {
        "message": message,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        **debug_obj,
    }
    for key in ("balanced_signal", "exports_signal", "imports_signal"):
        data["trade_signals"][key]["status"] = "Fetch failed"
        data["trade_signals"][key]["note"] = message


def recompute_monthly_trade_pulse(data):
    ensure_trade_structures(data)
    result, debug_obj, err = attempt_monthly_comext()
    pulse = data["monthly_trade_pulse"]
    signals = data["trade_signals"]

    if not result:
        update_monthly_error_state(
            data,
            message=f"Monthly COMEXT extraction did not yield a usable series yet. {err or ''}".strip(),
            debug_obj=debug_obj,
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
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        **debug_obj,
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
        {"label": "Cash runway", "value": "Short-to-moderate" if runway < 12 else "More comfortable", "note": f"Current tracker baseline implies roughly {runway:.1f} months of runway."},
        {"label": "Funding pressure", "value": pressure, "note": "Funding pressure remains meaningful until commercial progress clearly reduces financing risk."},
        {"label": "Commercial signal", "value": "Early but improving", "note": "Recent investor-page headlines suggest movement, but stronger recurring production proof is still needed."},
        {"label": "Biggest current risk", "value": "Dilution before scale", "note": f"Even with trade backdrop at {trade:+.1f}% YoY, the key company-specific risk is financing before scale-up."}
    ]


def main():
    data = load_data()
    data.setdefault("meta", {})
    data.setdefault("market_signals", {})
    changes = []
    data["meta"]["last_update"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    data["meta"]["data_source"] = "GitHub Pages / same-repo JSON"
    changes.append("Tracker refreshed")

    try:
        page_html = fetch_text(IR_URL)
        headlines = parse_ir_headlines(page_html)
        if headlines:
            data["ir_headlines"] = headlines[:3]
            data["market_signals"]["latest_ir_signal"] = {
                "title": headlines[0]["title"],
                "meta": "Latest headline from Cell Impact investor page",
                "status": "Watch"
            }
            changes.append("IR headlines updated")
    except Exception:
        pass

    price = fetch_price()
    if price:
        data["market_signals"]["share_price"] = price
        changes.append("Share price updated")

    recompute_trade_index(data)
    changes.append("Trade index recalculated")

    monthly_ok = recompute_monthly_trade_pulse(data)
    changes.append("Monthly balanced bundle refreshed" if monthly_ok else "Monthly balanced bundle fetch failed")

    build_what_matters_now(data)
    changes.append("What matters now updated")
    data["changes"] = changes
    save_data(data)
    print("Tracker JSON updated.")


if __name__ == "__main__":
    main()
