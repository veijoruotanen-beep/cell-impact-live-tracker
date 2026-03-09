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
from typing import Dict, List, Optional, Tuple

DATA_PATH = "data/tracker.json"
IR_URL = "https://investor.cellimpact.com/en/investor-relations"
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/CI.ST?range=5d&interval=1d"

EUROSTAT_API_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
EUROSTAT_DATASET = "tet00013"
EUROSTAT_QUERY = {}

# Official DS-prefixed COMEXT endpoints are under /api/comext/dissemination
COMEXT_SDMX21_BASE = "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1"
COMEXT_DATASETS = ["DS-059322", "DS-059332"]
PARTNER_TARGETS = ["US", "CN", "JP", "KR"]
PARTNER_LABEL_HINTS = {
    "US": ["united states", "usa", "us"],
    "CN": ["china"],
    "JP": ["japan"],
    "KR": ["korea", "south korea", "republic of korea"],
}
PRODUCT_PREFIXES = ["8501", "8504", "7219", "7326"]
MONTH_WINDOW = 18
SMOOTH_WINDOW = 3

NS = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}


def fetch_text(url: str, timeout: int = 30) -> str:
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return r.read().decode("utf-8", errors="ignore")


def fetch_json_url(url: str, timeout: int = 30):
    return json.loads(fetch_text(url, timeout=timeout))


def fetch_xml_root(url: str, timeout: int = 30) -> ET.Element:
    return ET.fromstring(fetch_text(url, timeout=timeout))


def build_eurostat_url(dataset, params):
    if not dataset:
        return None
    if not params:
        return f"{EUROSTAT_API_BASE}/{dataset}?format=JSON"
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{EUROSTAT_API_BASE}/{dataset}?{query}&format=JSON"


def load_data():
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_data(data):
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def parse_ir_headlines(html_text):
    matches = re.findall(r'href="([^"]+)".{0,300}?>([^<]{12,140})<', html_text, flags=re.I | re.S)
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
            base = k[:-5]
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
    if not url:
        return None
    try:
        payload = fetch_json_url(url)
        return eurostat_series_from_payload(payload)
    except Exception:
        return None


def _lower(s: Optional[str]) -> str:
    return str(s or "").strip().lower()


def parse_sdmx_structure(dataset: str) -> Dict:
    """
    Uses the official SDMX 2.1 structure route with references=descendants and
    detail=referencepartial, which Eurostat documents as a way to obtain the DSD,
    codelists and a filtered view of allowed codes for a dataset.
    """
    url = f"{COMEXT_SDMX21_BASE}/dataflow/ESTAT/{dataset}/latest?references=descendants&detail=referencepartial"
    root = fetch_xml_root(url)

    dims = []
    for dim in root.findall(".//str:DataStructure//str:DimensionList/str:Dimension", NS):
        dim_id = dim.attrib.get("id")
        rep = dim.find("str:LocalRepresentation", NS)
        enum_ref = None
        if rep is not None:
            enum_elem = rep.find("str:Enumeration/Ref", NS)
            if enum_elem is not None:
                enum_ref = enum_elem.attrib.get("id")
        dims.append({"id": dim_id, "codelist": enum_ref})

    codelists = {}
    for cl in root.findall(".//str:Codelist", NS):
        cl_id = cl.attrib.get("id")
        codes = []
        for code in cl.findall("str:Code", NS):
            code_id = code.attrib.get("id")
            name_elem = code.find("com:Name", NS) or code.find("str:Name", NS)
            label = name_elem.text.strip() if name_elem is not None and name_elem.text else code_id
            codes.append({"id": code_id, "label": label})
        codelists[cl_id] = codes

    return {"url": url, "dimensions": dims, "codelists": codelists}


def pick_code_by_exact_or_label(codes: List[Dict], exact_ids: List[str] = None, label_hints: List[str] = None) -> List[str]:
    exact_ids = exact_ids or []
    label_hints = [_lower(x) for x in (label_hints or [])]
    out = []
    for code in codes:
        cid = str(code.get("id", ""))
        lab = _lower(code.get("label"))
        if cid in exact_ids or _lower(cid) in [_lower(x) for x in exact_ids]:
            out.append(cid)
            continue
        if any(h in lab for h in label_hints):
            out.append(cid)
    # stable dedupe
    seen = set()
    result = []
    for x in out:
        if x not in seen:
            seen.add(x)
            result.append(x)
    return result


def pick_product_codes(codes: List[Dict], prefixes: List[str]) -> List[str]:
    found = []
    for code in codes:
        cid = str(code.get("id", ""))
        if any(cid.startswith(p) for p in prefixes):
            found.append(cid)
    # prefer exact HS4, otherwise include prefixed detailed codes but cap size
    exact = [x for x in found if len(x) == 4]
    if exact:
        return exact[:20]
    return found[:40]


def choose_dimension_filters(structure: Dict, flow_mode: str) -> Tuple[List[str], Dict]:
    dims = structure["dimensions"]
    codelists = structure["codelists"]
    filter_map: Dict[str, List[str]] = {}
    debug = {"dimensions": [], "selected": {}}

    for dim in dims:
        dim_id = dim["id"]
        cl_codes = codelists.get(dim.get("codelist"), [])
        lid = _lower(dim_id)
        codes = []

        if lid == "freq" or "freq" in lid:
            codes = pick_code_by_exact_or_label(cl_codes, exact_ids=["M"], label_hints=["monthly"])
        elif lid in ("geo", "reporter", "reporting", "declarant") or "geo" in lid:
            codes = pick_code_by_exact_or_label(
                cl_codes,
                exact_ids=["EU27_2020", "EU27_2020_H", "EU27_2020_X_H"],
                label_hints=["european union", "eu27"]
            )
        elif "partner" in lid:
            partner_codes = []
            for p in PARTNER_TARGETS:
                partner_codes.extend(pick_code_by_exact_or_label(cl_codes, exact_ids=[p], label_hints=PARTNER_LABEL_HINTS.get(p, [])))
            codes = partner_codes[:20]
        elif lid in ("flow", "tradeflow") or "flow" in lid:
            if flow_mode == "exports":
                codes = pick_code_by_exact_or_label(cl_codes, exact_ids=["EXP", "EXPORT", "X"], label_hints=["export", "dispatch"])
            else:
                codes = pick_code_by_exact_or_label(cl_codes, exact_ids=["IMP", "IMPORT", "M"], label_hints=["import", "arrival"])
        elif lid in ("product", "prod", "cn", "commodity") or "cn" == lid or "prod" in lid or "commodity" in lid:
            codes = pick_product_codes(cl_codes, PRODUCT_PREFIXES)
        elif lid in ("unit", "indic", "measure") or "unit" in lid or "measure" in lid:
            # Prefer value / euro like measures if explicit
            codes = pick_code_by_exact_or_label(cl_codes, exact_ids=["VALUE_IN_EUROS", "V_EUR"], label_hints=["euro", "value"])

        filter_map[dim_id] = codes
        debug["dimensions"].append({
            "id": dim_id,
            "codelist": dim.get("codelist"),
            "available_count": len(cl_codes),
            "selected": codes,
        })
        if codes:
            debug["selected"][dim_id] = codes

    key_parts = []
    for dim in dims:
        vals = filter_map.get(dim["id"], [])
        key_parts.append("+".join(vals) if vals else "")
    return key_parts, debug


def build_sdmx_data_url(dataset: str, key_parts: List[str], months_back: int = 24) -> str:
    key = ".".join(key_parts)
    end_dt = datetime.now(timezone.utc)
    start_year = end_dt.year
    start_month = end_dt.month - months_back + 1
    while start_month <= 0:
        start_month += 12
        start_year -= 1
    start_period = f"{start_year:04d}-{start_month:02d}"
    end_period = f"{end_dt.year:04d}-{end_dt.month:02d}"
    params = {
        "format": "JSON",
        "detail": "dataonly",
        "startPeriod": start_period,
        "endPeriod": end_period,
    }
    query = urllib.parse.urlencode(params)
    return f"{COMEXT_SDMX21_BASE}/data/{dataset}/{key}?{query}"


def normalize_month(s: str) -> Optional[str]:
    s = str(s or "")
    m = re.search(r"((19|20)\d{2})[-M]?(\d{2})", s)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(3)}"


def choose_time_field_monthly(rows: List[Dict]) -> Optional[str]:
    if not rows:
        return None
    for key in rows[0].keys():
        if key.endswith("_code") or key.endswith("_label"):
            base = key[:-5]
            if _lower(base) in ("time", "time_period", "period", "month"):
                return base
    for key, val in rows[0].items():
        if key.endswith("_label") and normalize_month(val):
            return key[:-6]
    return None


def aggregate_monthly_from_payload(payload: Dict) -> Dict[str, float]:
    rows = jsonstat_to_rows(payload)
    tf = choose_time_field_monthly(rows)
    if not tf:
        raise ValueError("Could not identify TIME_PERIOD field in JSON-stat payload")
    out: Dict[str, float] = {}
    for row in rows:
        period = normalize_month(row.get(f"{tf}_code")) or normalize_month(row.get(f"{tf}_label"))
        if not period:
            continue
        val = row.get("value")
        if val in (None, ":"):
            continue
        try:
            num = float(val)
        except Exception:
            continue
        out[period] = out.get(period, 0.0) + num
    if not out:
        raise ValueError("No monthly observations extracted from payload")
    return dict(sorted(out.items()))


def moving_average(series: List[Tuple[str, float]], window: int = 3) -> List[Tuple[str, float]]:
    out = []
    for i in range(len(series)):
        start = max(0, i - window + 1)
        chunk = [v for _, v in series[start:i+1] if v is not None and not math.isnan(v)]
        if not chunk:
            continue
        out.append((series[i][0], sum(chunk) / len(chunk)))
    return out


def fetch_monthly_comext_structure_first() -> Dict:
    debug = {"attempts": [], "structure": {}, "selected": {}, "errors": []}
    results = {"imports": {}, "exports": {}}

    for dataset in COMEXT_DATASETS:
        structure = parse_sdmx_structure(dataset)
        debug["structure"][dataset] = {
            "structure_url": structure["url"],
            "dimensions": [d["id"] for d in structure["dimensions"]],
        }
        for flow_mode in ("imports", "exports"):
            key_parts, selection_debug = choose_dimension_filters(structure, flow_mode)
            url = build_sdmx_data_url(dataset, key_parts, months_back=24)
            debug["attempts"].append({
                "dataset": dataset,
                "flow": flow_mode,
                "url": url,
                "selected": selection_debug.get("selected", {}),
            })
            try:
                payload = fetch_json_url(url, timeout=45)
                series = aggregate_monthly_from_payload(payload)
                if series:
                    for period, value in series.items():
                        results[flow_mode][period] = results[flow_mode].get(period, 0.0) + value
            except Exception as exc:
                debug["errors"].append({"dataset": dataset, "flow": flow_mode, "error": f"{type(exc).__name__}: {exc}"})

    if not results["imports"] and not results["exports"]:
        raise RuntimeError(json.dumps(debug, ensure_ascii=False))
    return {"imports": dict(sorted(results["imports"].items())), "exports": dict(sorted(results["exports"].items())), "debug": debug}


def recompute_monthly_trade_pulse(data):
    data.setdefault("trade_signals", {})
    try:
        fetched = fetch_monthly_comext_structure_first()
        imports_map = fetched["imports"]
        exports_map = fetched["exports"]

        all_periods = sorted(set(imports_map.keys()) | set(exports_map.keys()))
        balanced_raw = []
        imports_series = []
        exports_series = []
        for period in all_periods:
            imp = float(imports_map.get(period, 0.0))
            exp = float(exports_map.get(period, 0.0))
            bal = 0.6 * exp + 0.4 * imp
            imports_series.append((period, imp))
            exports_series.append((period, exp))
            balanced_raw.append((period, bal))

        balanced_smoothed = moving_average(balanced_raw, window=SMOOTH_WINDOW)
        if not balanced_smoothed:
            raise ValueError("Monthly series built but smoothing returned no usable observations")

        balanced_smoothed = balanced_smoothed[-MONTH_WINDOW:]
        balanced_raw = balanced_raw[-MONTH_WINDOW:]
        imports_series = imports_series[-MONTH_WINDOW:]
        exports_series = exports_series[-MONTH_WINDOW:]

        latest_period, latest_value = balanced_smoothed[-1]
        latest_raw = dict(balanced_raw).get(latest_period)
        yoy_pct = None
        target_prev = None
        y, m = latest_period.split("-")
        target_prev = f"{int(y)-1:04d}-{m}"
        prev_map = dict(balanced_smoothed)
        if target_prev in prev_map and prev_map[target_prev]:
            yoy_pct = ((latest_value - prev_map[target_prev]) / prev_map[target_prev]) * 100

        data["trade_signals"]["balanced_signal"] = {
            "status": "Live",
            "scope": "EU + selected partners",
            "flows": "imports + exports",
            "weights": {"exports": 0.6, "imports": 0.4},
            "value": round(latest_value, 1),
            "latest_period": latest_period,
            "yoy_pct": round(yoy_pct, 1) if yoy_pct is not None else None,
            "note": "Primary monthly trade pulse with 3M smoothing."
        }
        data["trade_signals"]["exports_signal"] = {
            "status": "Live",
            "scope": "EU + selected partners",
            "flows": "exports",
            "value": round(exports_series[-1][1], 1),
            "latest_period": exports_series[-1][0],
            "note": "Exports-only 3M smoothed signal."
        }
        data["trade_signals"]["imports_signal"] = {
            "status": "Live",
            "scope": "EU + selected partners",
            "flows": "imports",
            "value": round(imports_series[-1][1], 1),
            "latest_period": imports_series[-1][0],
            "note": "Imports-only demand-environment signal."
        }
        data["monthly_trade_pulse"] = {
            "status": "Live",
            "scope": "EU + selected partners | imports + exports | 60/40 weighting",
            "partners": PARTNER_TARGETS,
            "flows": {"exports": 0.6, "imports": 0.4},
            "window_months": MONTH_WINDOW,
            "smoothing": f"{SMOOTH_WINDOW}M MA",
            "latest_period": latest_period,
            "latest_value": round(latest_value, 1),
            "latest_raw_balanced": round(latest_raw, 1) if latest_raw is not None else None,
            "yoy_pct": round(yoy_pct, 1) if yoy_pct is not None else None,
            "methodology": "Structure-first COMEXT monthly balanced bundle built from official SDMX structure metadata and filtered monthly data queries.",
            "note": "Monthly balanced bundle from COMEXT datasets with 3M moving-average smoothing.",
            "balanced_series": [{"period": p, "value": round(v, 1)} for p, v in balanced_smoothed],
            "balanced_raw_series": [{"period": p, "value": round(v, 1)} for p, v in balanced_raw],
            "exports_series": [{"period": p, "value": round(v, 1)} for p, v in exports_series],
            "imports_series": [{"period": p, "value": round(v, 1)} for p, v in imports_series],
            "debug": fetched["debug"],
        }
        return True
    except Exception as exc:
        msg = f"Monthly COMEXT extraction did not yield a usable series yet. {type(exc).__name__}: {exc}"
        data["trade_signals"]["balanced_signal"] = {
            "status": "Fetch failed",
            "scope": "EU + selected partners",
            "flows": "imports + exports",
            "weights": {"exports": 0.6, "imports": 0.4},
            "note": msg,
        }
        data["trade_signals"]["exports_signal"] = {
            "status": "Fetch failed",
            "scope": "EU + selected partners",
            "flows": "exports",
            "note": msg,
        }
        data["trade_signals"]["imports_signal"] = {
            "status": "Fetch failed",
            "scope": "EU + selected partners",
            "flows": "imports",
            "note": msg,
        }
        data["monthly_trade_pulse"] = {
            "status": "Fetch failed",
            "scope": "EU + selected partners | imports + exports | 60/40 weighting",
            "partners": PARTNER_TARGETS,
            "flows": {"exports": 0.6, "imports": 0.4},
            "window_months": MONTH_WINDOW,
            "smoothing": f"{SMOOTH_WINDOW}M MA",
            "latest_period": None,
            "latest_value": None,
            "latest_raw_balanced": None,
            "yoy_pct": None,
            "methodology": "Monthly balanced bundle. Shows the last 18 months after 3-month moving-average smoothing when live COMEXT extraction succeeds.",
            "note": msg,
            "balanced_series": [],
            "balanced_raw_series": [],
            "exports_series": [],
            "imports_series": [],
            "debug": {
                "message": msg,
                "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            },
        }
        return False


def recompute_trade_index(data):
    cm = data.get("customs_monitor", {})
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
    cm["next_step_note"] = "Next step: replace or supplement tet00013 with a verified COMEXT monthly bundle."
    cm["comext_dataset_target"] = " + ".join(COMEXT_DATASETS)
    cm["bundle_definition"] = {
        "status": "Configured",
        "bundle_name": "Balanced EU + partner hydrogen hardware bundle",
        "dataset_target": " + ".join(COMEXT_DATASETS),
        "partners": PARTNER_TARGETS,
        "flows": "imports + exports",
        "weights": {"exports": 0.6, "imports": 0.4},
        "why": "Faster customs pulse than the annual macro proxy, combining a hydrogen hardware anchor with a cautious metals/forming proxy.",
        "limitation": "Proxy signal only. It does not directly measure Cell Impact revenue or orders.",
        "codes": [
            {"group": "hydrogen_anchor", "code": "8501", "level": "HS4", "label": "Electric motors and generators"},
            {"group": "hydrogen_anchor", "code": "8504", "level": "HS4", "label": "Electrical transformers and converters"},
            {"group": "metals_forming_proxy", "code": "7219", "level": "HS4", "label": "Stainless steel flat-rolled products"},
            {"group": "metals_forming_proxy", "code": "7326", "level": "HS4", "label": "Other articles of iron or steel"},
        ],
    }
    data["customs_monitor"] = cm
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
            "status": "Live" if live_series else "Proxy",
        }


def build_what_matters_now(data):
    runway = float(data.get("company", {}).get("runway_months", 0) or 0)
    pressure = data.get("market_signals", {}).get("dilution_pressure", {}).get("value", "Elevated")
    trade = data.get("customs_monitor", {}).get("yoy_pct", 0)
    data["what_matters_now"] = [
        {"label": "Cash runway", "value": "Short-to-moderate" if runway < 12 else "More comfortable", "note": f"Current tracker baseline implies roughly {runway:.1f} months of runway."},
        {"label": "Funding pressure", "value": pressure, "note": "Funding pressure remains meaningful until commercial progress clearly reduces financing risk."},
        {"label": "Commercial signal", "value": "Early but improving", "note": "Recent investor-page headlines suggest movement, but stronger recurring production proof is still needed."},
        {"label": "Biggest current risk", "value": "Dilution before scale", "note": f"Even with trade backdrop at {trade:+.1f}% YoY, the key company-specific risk is financing before scale-up."},
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
            data["market_signals"]["latest_ir_signal"] = {"title": headlines[0]["title"], "meta": "Latest headline from Cell Impact investor page", "status": "Watch"}
            changes.append("IR headlines updated")
    except Exception:
        pass

    price = fetch_price()
    if price:
        data["market_signals"]["share_price"] = price
        changes.append("Share price updated")

    recompute_trade_index(data)
    changes.append("Trade index recalculated")

    if recompute_monthly_trade_pulse(data):
        changes.append("Monthly balanced bundle updated")
    else:
        changes.append("Monthly balanced bundle fetch failed")

    build_what_matters_now(data)
    changes.append("What matters now updated")
    data["changes"] = changes
    save_data(data)
    print("Tracker JSON updated.")


if __name__ == "__main__":
    main()
