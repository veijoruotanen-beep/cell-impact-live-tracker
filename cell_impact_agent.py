#!/usr/bin/env python3
import csv
import html
import io
import json
import os
import re
import ssl
import tempfile
import urllib.parse
import urllib.request
from datetime import datetime, timezone

try:
    import py7zr
except Exception:
    py7zr = None

DATA_PATH = "data/tracker.json"
IR_URL = "https://investor.cellimpact.com/en/investor-relations"
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/CI.ST?range=5d&interval=1d"

EUROSTAT_API_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
EUROSTAT_DATASET = "tet00013"
EUROSTAT_QUERY = {}

FILES_API_BASE = "https://ec.europa.eu/eurostat/api/dissemination/files"
COMEXT_PRODUCTS_DIR = "comext/COMEXT_DATA/PRODUCTS"
PARTNERS = ["US", "CN", "JP", "KR"]
PRODUCT_CODES = ["8501", "8504", "7219", "7326"]
WINDOW_MONTHS = 18
MA_WINDOW = 3


def fetch_text(url, timeout=60):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return r.read().decode("utf-8", errors="ignore")


def fetch_bytes(url, timeout=120):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return r.read()


def fetch_json_url(url, timeout=60):
    return json.loads(fetch_text(url, timeout=timeout))


def build_eurostat_url(dataset, params):
    if not dataset:
        return None
    if not params:
        return f"{EUROSTAT_API_BASE}/{dataset}?format=JSON"
    query = urllib.parse.urlencode(params, doseq=True)
    return f"{EUROSTAT_API_BASE}/{dataset}?{query}&format=JSON"


def load_data():
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_data(data):
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def parse_ir_headlines(raw_html):
    matches = re.findall(r'href="([^"]+)".{0,300}?>([^<]{12,140})<', raw_html, flags=re.I | re.S)
    out, seen = [], set()
    for href, title in matches:
        t = html.unescape(re.sub(r"\s+", " ", title)).strip()
        if len(t) < 15:
            continue
        lower = t.lower()
        if not any(key in lower for key in ["cell impact", "rights issue", "bta", "report", "share", "annual", "quarter", "trading", "investor"]):
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
    mapped = {}
    if isinstance(idx, list):
        for pos, code in enumerate(idx):
            mapped[str(code)] = {"pos": pos, "label": labels.get(str(code), str(code))}
    else:
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
    cm["comext_dataset_target"] = "DS-059322 + DS-059332"

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
    data["customs_monitor"] = cm


def ensure_bundle_definition(data):
    cm = data.setdefault("customs_monitor", {})
    cm["bundle_definition"] = {
        "status": "Configured",
        "bundle_name": "Balanced EU + partner hydrogen hardware bundle",
        "dataset_target": "DS-059322 + DS-059332",
        "partners": PARTNERS,
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


def default_monthly_state():
    return {
        "status": "Pending live run",
        "scope": "EU + selected partners | imports + exports | 60/40 weighting",
        "partners": PARTNERS,
        "flows": {"exports": 0.6, "imports": 0.4},
        "window_months": WINDOW_MONTHS,
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
        "debug": {}
    }


def files_api_listing():
    params = {
        "format": "csv",
        "dir": COMEXT_PRODUCTS_DIR,
        "hierarchy": "false",
        "sizeFormat": "NONE",
        "dateFormat": "ISO",
    }
    url = f"{FILES_API_BASE}?{urllib.parse.urlencode(params)}"
    raw = fetch_text(url, timeout=120)
    reader = csv.DictReader(io.StringIO(raw))
    rows = list(reader)
    fieldnames = reader.fieldnames or []
    return url, rows, fieldnames, raw[:2000]


def row_values(row):
    vals = []
    for v in row.values():
        if v is None:
            continue
        vals.append(str(v).strip().strip('"'))
    return vals


def find_archive_name_in_row(row):
    # Prefer obvious filename-like columns, but fall back to scanning all values.
    preferred_keys = []
    for k in row.keys():
        lk = str(k).lower()
        if any(tok in lk for tok in ["name", "file", "filename", "path"]):
            preferred_keys.append(k)
    check_keys = preferred_keys + [k for k in row.keys() if k not in preferred_keys]
    pattern = re.compile(r"(full[^,;\s]*?\d{6}\.7z)", re.I)
    for k in check_keys:
        v = row.get(k)
        if v is None:
            continue
        txt = str(v).strip().strip('"')
        m = pattern.search(txt)
        if m:
            return m.group(1)
    for txt in row_values(row):
        m = pattern.search(txt)
        if m:
            return m.group(1)
    return None


def pick_latest_archive(rows):
    candidates = []
    ym_pattern = re.compile(r"(\d{6})(?=\.7z$)", re.I)
    for row in rows:
        name = find_archive_name_in_row(row)
        if not name:
            continue
        m = ym_pattern.search(name)
        if m:
            candidates.append((m.group(1), name))
    if not candidates:
        raise RuntimeError("No matching full*_YYYYMM.7z files found in COMEXT_DATA/PRODUCTS listing")
    latest = sorted(candidates, key=lambda x: x[0])[-1]
    unique_names = []
    seen = set()
    for _, name in sorted(candidates):
        if name not in seen:
            unique_names.append(name)
            seen.add(name)
    return latest[1], unique_names[-20:]


def build_file_download_url(filename):
    params = {"file": f"{COMEXT_PRODUCTS_DIR}/{filename}"}
    return f"{FILES_API_BASE}?{urllib.parse.urlencode(params)}"


def extract_first_csv_from_7z(archive_bytes):
    if py7zr is None:
        raise RuntimeError("py7zr is not installed in the workflow environment")
    with tempfile.TemporaryDirectory() as td:
        archive_path = os.path.join(td, "comext.7z")
        with open(archive_path, "wb") as f:
            f.write(archive_bytes)
        with py7zr.SevenZipFile(archive_path, mode="r") as z:
            names = z.getnames()
            z.extractall(path=td)
        csv_files = []
        for name in names:
            full = os.path.join(td, name)
            if os.path.isfile(full) and name.lower().endswith((".csv", ".tsv", ".txt")):
                csv_files.append(full)
        if not csv_files:
            for root, _, files in os.walk(td):
                for file in files:
                    if file.lower().endswith((".csv", ".tsv", ".txt")):
                        csv_files.append(os.path.join(root, file))
        if not csv_files:
            raise RuntimeError("No CSV/TSV/TXT file found inside COMEXT archive")
        chosen = sorted(csv_files)[0]
        with open(chosen, "rb") as f:
            sample = f.read(4096)
        return chosen, sample.decode("utf-8", errors="ignore")


def choose_csv_dialect(text_sample):
    if "\t" in text_sample and text_sample.count("\t") > text_sample.count(";"):
        return "excel-tab"
    if ";" in text_sample and text_sample.count(";") > text_sample.count(","):
        class Semi(csv.excel):
            delimiter = ";"
        return Semi
    return "excel"


def parse_period_to_key(value):
    s = str(value).strip()
    m = re.match(r"^(\d{4})[-/]?M?(\d{2})$", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m = re.search(r"(20\d{2})[-/]?(0[1-9]|1[0-2])", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return None


def detect_columns(fieldnames):
    result = {}
    for key in fieldnames:
        lk = key.lower()
        if "period" in lk or lk in ("time", "month"):
            result.setdefault("period", key)
        if lk in ("geo", "reporter", "rep", "reporting", "declarant"):
            result.setdefault("geo", key)
        if "partner" in lk:
            result.setdefault("partner", key)
        if lk in ("flow", "trade_flow", "flow_dir", "stk_flow", "trade", "indic_et"):
            result.setdefault("flow", key)
        if lk in ("product", "prod", "cn", "commodity") or lk.startswith("product") or lk.startswith("prod"):
            if "unit" not in lk and "value" not in lk:
                result.setdefault("product", key)
        if lk in ("value", "obs_value", "obsvalue", "val", "trade_value"):
            result.setdefault("value", key)
    return result


def flow_bucket(v):
    s = str(v or "").strip().upper()
    if any(x in s for x in ["EXP", "EXPORT"]):
        return "exports"
    if any(x in s for x in ["IMP", "IMPORT"]):
        return "imports"
    return None


def parse_number(v):
    s = str(v).strip().replace(" ", "")
    if not s or s == ":":
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def series_to_sorted(series_dict):
    return [{"period": k, "value": round(v, 2)} for k, v in sorted(series_dict.items())]


def moving_average(series, window=3):
    out = []
    vals = [p["value"] for p in series]
    for i in range(len(series)):
        if i + 1 < window:
            continue
        chunk = vals[i - window + 1:i + 1]
        out.append({"period": series[i]["period"], "value": round(sum(chunk) / len(chunk), 2)})
    return out


def yoy_monthly(series):
    if len(series) < 13:
        return None
    latest = series[-1]["value"]
    prev = series[-13]["value"]
    if prev in (None, 0):
        return None
    return round(((latest - prev) / prev) * 100, 1)


def last_n(series, n):
    return series[-n:] if len(series) > n else series


def monthly_from_bulk():
    debug = {}
    listing_url, rows, listing_fields, listing_raw_preview = files_api_listing()
    names = [(r.get("name") or r.get("file") or "") for r in rows]
    debug["files_api"] = {
        "listing_url": listing_url,
        "listing_count": len(rows),
                "listing_fields": listing_fields,
                "listing_raw_preview": listing_raw_preview,
        "sample_names": names[:20],
    }

    filename, recent_matches = pick_latest_archive(rows)
    debug["files_api"]["matched_names_tail"] = recent_matches
    debug["selected_archive"] = filename
    download_url = build_file_download_url(filename)
    debug["download_url"] = download_url

    archive_bytes = fetch_bytes(download_url, timeout=180)
    csv_path, sample_text = extract_first_csv_from_7z(archive_bytes)
    debug["extracted_file"] = os.path.basename(csv_path)

    dialect = choose_csv_dialect(sample_text)
    with open(csv_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f, dialect=dialect)
        fieldnames = reader.fieldnames or []
        debug["columns"] = fieldnames[:60]
        cols = detect_columns(fieldnames)
        debug["column_map"] = cols

        if not all(k in cols for k in ("period", "product", "value")):
            raise RuntimeError(f"Could not map essential columns from extracted file: {cols}")

        export_raw = {}
        import_raw = {}
        row_count = 0
        kept_count = 0

        for row in reader:
            row_count += 1
            period = parse_period_to_key(row.get(cols["period"], ""))
            if not period:
                continue

            product = str(row.get(cols["product"], "")).strip()
            if not any(product.startswith(code) for code in PRODUCT_CODES):
                continue

            partner_ok = True
            if "partner" in cols:
                partner_val = str(row.get(cols["partner"], "")).strip().upper()
                partner_ok = any(p in partner_val for p in PARTNERS)
            if not partner_ok:
                continue

            if "geo" in cols:
                geo_val = str(row.get(cols["geo"], "")).strip().upper()
                if "EU27_2020" not in geo_val and geo_val not in ("EU", "EU27_2020"):
                    continue

            bucket = None
            if "flow" in cols:
                bucket = flow_bucket(row.get(cols["flow"], ""))
            if bucket is None:
                continue

            value = parse_number(row.get(cols["value"], ""))
            if value is None:
                continue

            kept_count += 1
            if bucket == "exports":
                export_raw[period] = export_raw.get(period, 0.0) + value
            elif bucket == "imports":
                import_raw[period] = import_raw.get(period, 0.0) + value

    debug["rows_seen"] = row_count
    debug["rows_kept"] = kept_count

    exports_raw_series = series_to_sorted(export_raw)
    imports_raw_series = series_to_sorted(import_raw)
    periods = sorted(set([p["period"] for p in exports_raw_series] + [p["period"] for p in imports_raw_series]))

    balanced_raw = []
    for period in periods:
        ev = export_raw.get(period, 0.0)
        iv = import_raw.get(period, 0.0)
        val = 0.6 * ev + 0.4 * iv
        balanced_raw.append({"period": period, "value": round(val, 2)})

    exports_ma = moving_average(exports_raw_series, MA_WINDOW)
    imports_ma = moving_average(imports_raw_series, MA_WINDOW)
    balanced_ma = moving_average(balanced_raw, MA_WINDOW)

    exports_ma = last_n(exports_ma, WINDOW_MONTHS)
    imports_ma = last_n(imports_ma, WINDOW_MONTHS)
    balanced_raw = last_n(balanced_raw, WINDOW_MONTHS + MA_WINDOW)
    balanced_ma = last_n(balanced_ma, WINDOW_MONTHS)

    if not balanced_ma:
        raise RuntimeError("Bulk COMEXT file parsed, but no usable monthly balanced series was produced")

    latest_period = balanced_ma[-1]["period"]
    latest_value = balanced_ma[-1]["value"]
    latest_raw = balanced_raw[-1]["value"] if balanced_raw else None
    yoy = yoy_monthly(balanced_ma)

    return {
        "latest_period": latest_period,
        "latest_value": latest_value,
        "latest_raw_balanced": latest_raw,
        "yoy_pct": yoy,
        "balanced_series": balanced_ma,
        "balanced_raw_series": balanced_raw,
        "exports_series": exports_ma,
        "imports_series": imports_ma,
        "debug": debug,
    }


def update_monthly_trade_pulse(data, changes):
    ensure_bundle_definition(data)
    monthly = default_monthly_state()
    try:
        result = monthly_from_bulk()
        monthly.update(result)
        monthly["status"] = "Live"
        monthly["note"] = "Monthly balanced bundle extracted from COMEXT bulk files."
        data["trade_signals"] = {
            "balanced_signal": {
                "status": "Live",
                "scope": "EU + selected partners",
                "flows": "imports + exports",
                "weights": {"exports": 0.6, "imports": 0.4},
                "value": result["latest_value"],
                "latest_period": result["latest_period"],
                "yoy_pct": result["yoy_pct"],
                "note": "Primary monthly trade pulse from COMEXT bulk files."
            },
            "exports_signal": {
                "status": "Live",
                "scope": "EU + selected partners",
                "flows": "exports",
                "value": result["exports_series"][-1]["value"] if result["exports_series"] else None,
                "note": "Closer commercial-direction signal for an EU-based supplier."
            },
            "imports_signal": {
                "status": "Live",
                "scope": "EU + selected partners",
                "flows": "imports",
                "value": result["imports_series"][-1]["value"] if result["imports_series"] else None,
                "note": "Demand-environment signal."
            },
            "macro_signal": {
                "status": "Live",
                "dataset_target": "tet00013",
                "value": data.get("customs_monitor", {}).get("latest_index"),
                "yoy_pct": data.get("customs_monitor", {}).get("yoy_pct"),
                "note": "Annual macro/proxy backdrop."
            }
        }
        changes.append("Monthly balanced bundle updated")
    except Exception as e:
        msg = f"Monthly COMEXT bulk extraction did not yield a usable series yet. {type(e).__name__}: {e}"
        monthly["status"] = "Fetch failed"
        monthly["note"] = msg
        monthly["debug"] = monthly.get("debug", {})
        monthly["debug"]["message"] = msg
        monthly["debug"]["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        data["trade_signals"] = {
            "balanced_signal": {
                "status": "Fetch failed",
                "scope": "EU + selected partners",
                "flows": "imports + exports",
                "weights": {"exports": 0.6, "imports": 0.4},
                "note": msg
            },
            "exports_signal": {
                "status": "Fetch failed",
                "scope": "EU + selected partners",
                "flows": "exports",
                "note": msg
            },
            "imports_signal": {
                "status": "Fetch failed",
                "scope": "EU + selected partners",
                "flows": "imports",
                "note": msg
            },
            "macro_signal": {
                "status": "Live",
                "dataset_target": "tet00013",
                "value": data.get("customs_monitor", {}).get("latest_index"),
                "yoy_pct": data.get("customs_monitor", {}).get("yoy_pct"),
                "note": "Annual macro/proxy backdrop."
            }
        }
        changes.append("Monthly balanced bundle fetch failed")
    data["monthly_trade_pulse"] = monthly


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
    changes = []
    data.setdefault("meta", {})
    data["meta"]["last_update"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    data["meta"]["data_source"] = "GitHub Pages / same-repo JSON"

    changes.append("Tracker refreshed")

    try:
        raw_html = fetch_text(IR_URL)
        headlines = parse_ir_headlines(raw_html)
        if headlines:
            data["ir_headlines"] = headlines[:3]
            data.setdefault("market_signals", {})
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
        data.setdefault("market_signals", {})
        data["market_signals"]["share_price"] = price
        changes.append("Share price updated")

    recompute_trade_index(data)
    changes.append("Trade index recalculated")

    update_monthly_trade_pulse(data, changes)
    build_what_matters_now(data)
    changes.append("What matters now updated")
    data["changes"] = changes
    save_data(data)
    print("Tracker JSON updated.")


if __name__ == "__main__":
    main()
