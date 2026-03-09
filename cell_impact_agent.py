#!/usr/bin/env python3
import csv, html, io, json, math, os, re, ssl, tempfile, urllib.parse, urllib.request
from datetime import datetime, timezone

DATA_PATH = "data/tracker.json"
IR_URL = "https://investor.cellimpact.com/en/investor-relations"
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/CI.ST?range=5d&interval=1d"
EUROSTAT_API_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
EUROSTAT_DATASET = "tet00013"
EUROSTAT_QUERY = {}
FILES_API = "https://ec.europa.eu/eurostat/api/dissemination/files"
COMEXT_PRODUCTS_DIR = "comext/COMEXT_DATA/PRODUCTS"
PARTNERS = ["US", "CN", "JP", "KR"]
PRODUCT_CODES = ["8501", "8504", "7219", "7326"]
FLOW_WEIGHTS = {"exports": 0.6, "imports": 0.4}
WINDOW_MONTHS = 18


def fetch_text(url, timeout=30):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return r.read().decode("utf-8", errors="ignore")


def fetch_bytes(url, timeout=60):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return r.read()


def fetch_json_url(url, timeout=30):
    return json.loads(fetch_text(url, timeout=timeout))


def build_eurostat_url(dataset, params):
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


def parse_ir_headlines(html_text):
    matches = re.findall(r'href="([^"]+)".{0,300}?>([^<]{12,160})<', html_text, flags=re.I | re.S)
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
        if k.endswith("_code"):
            base = k[:-5]
            if base.lower() in ("time", "time_period", "period", "year"):
                return base
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
            grouped.setdefault(year, []).append((score_row(row), val))
        if not grouped:
            return None
        series = []
        for year in sorted(grouped):
            best = sorted(grouped[year], key=lambda x: (x[0], x[1]), reverse=True)[0]
            series.append({"year": int(year), "value": round(float(best[1]), 2)})
        return {"eu_imports_proxy": series} if len(series) >= 2 else None
    except Exception:
        return None


def fetch_live_customs_series():
    url = build_eurostat_url(EUROSTAT_DATASET, EUROSTAT_QUERY)
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
        data.setdefault("market_signals", {})["hydrogen_trade_pulse"] = {
            "value": f"Trade index {round(avg,1)}",
            "meta": "Customs index based on tracker series.",
            "status": "Live" if live_series else "Proxy"
        }


def norm(s):
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


def parse_period(s):
    s = str(s or "")
    m = re.search(r"(20\d{2})[-/]?(0[1-9]|1[0-2])", s)
    return f"{m.group(1)}-{m.group(2)}" if m else None


def month_sort_key(p):
    if not p:
        return (0, 0)
    y, m = p.split("-")
    return (int(y), int(m))


def moving_average(series, k=3):
    out = []
    vals = [x["value"] for x in series]
    for i, item in enumerate(series):
        window = vals[max(0, i-k+1):i+1]
        if not window:
            continue
        out.append({"period": item["period"], "value": round(sum(window)/len(window), 2)})
    return out


def safe_float(v):
    if v in (None, "", ":"):
        return None
    s = str(v).replace(" ", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def pick_col(headers, candidates):
    m = {norm(h): h for h in headers}
    for cand in candidates:
        for nk, orig in m.items():
            if cand in nk:
                return orig
    return None


def list_comext_product_files(debug):
    url = f"{FILES_API}?format=csv&dir={urllib.parse.quote(COMEXT_PRODUCTS_DIR, safe='')}&hierarchy=false&sizeFormat=NONE&dateFormat=ISO"
    debug.setdefault("files_api", {})["listing_url"] = url
    text = fetch_text(url, timeout=60)
    rows = list(csv.DictReader(io.StringIO(text)))
    names = [r.get("NAME", "") for r in rows]
    debug["files_api"]["listing_count"] = len(rows)
    debug["files_api"]["sample_names"] = names[:20]
    candidates = sorted([n for n in names if re.match(r"^full\d{6}\.7z$", n)], reverse=True)
    debug["files_api"]["full_candidates"] = candidates[:12]
    return candidates


def extract_first_text_from_7z(blob, debug):
    try:
        import py7zr
    except Exception as e:
        debug.setdefault("files_api", {})["extract_error"] = f"py7zr missing: {e}"
        return None, None
    with tempfile.TemporaryDirectory() as td:
        arc = os.path.join(td, "comext.7z")
        with open(arc, "wb") as f:
            f.write(blob)
        with py7zr.SevenZipFile(arc, mode="r") as z:
            names = z.getnames()
            debug.setdefault("files_api", {})["archive_members"] = names[:20]
            z.extractall(path=td)
        files = []
        for root, _, fnames in os.walk(td):
            for fn in fnames:
                if fn == "comext.7z":
                    continue
                files.append(os.path.join(root, fn))
        files.sort()
        debug.setdefault("files_api", {})["extracted_files"] = [os.path.relpath(f, td) for f in files[:20]]
        for path in files:
            if path.lower().endswith((".csv", ".txt", ".tsv")):
                with open(path, "rb") as f:
                    return os.path.basename(path), f.read()
        if files:
            with open(files[0], "rb") as f:
                return os.path.basename(files[0]), f.read()
    return None, None


def build_monthly_from_bulk(debug):
    candidates = list_comext_product_files(debug)
    if not candidates:
        raise RuntimeError("No fullYYYYMM.7z files found in COMEXT_DATA/PRODUCTS listing")
    selected = candidates[0]
    debug.setdefault("files_api", {})["selected_file"] = selected
    file_url = f"{FILES_API}?file={urllib.parse.quote(COMEXT_PRODUCTS_DIR + '/' + selected, safe='/')}"
    debug["files_api"]["selected_url"] = file_url
    blob = fetch_bytes(file_url, timeout=120)
    fname, payload = extract_first_text_from_7z(blob, debug)
    if payload is None:
        raise RuntimeError(debug.get("files_api", {}).get("extract_error", "Unable to extract COMEXT 7z payload"))
    debug["files_api"]["selected_inner_file"] = fname
    sample = payload[:200000].decode("utf-8", errors="ignore")
    first_line = sample.splitlines()[0] if sample.splitlines() else ""
    dialect = csv.Sniffer().sniff(sample[:5000], delimiters=",;\t|")
    headers = next(csv.reader(io.StringIO(first_line), dialect))
    debug["files_api"]["headers"] = headers[:50]

    geo_col = pick_col(headers, ["reporter", "declarant", "geo"]) 
    partner_col = pick_col(headers, ["partner", "partnercountry", "partn", "flowpartner"]) 
    flow_col = pick_col(headers, ["flow", "indicet", "trade"]) 
    product_col = pick_col(headers, ["product", "prod", "commodity", "cn", "hs"]) 
    period_col = pick_col(headers, ["period", "time", "month"]) 
    value_col = pick_col(headers, ["valueineuros", "tradevalue", "value", "valeur", "euro", "statvalue"]) 
    debug["files_api"]["mapped_columns"] = {
        "geo": geo_col, "partner": partner_col, "flow": flow_col,
        "product": product_col, "period": period_col, "value": value_col
    }
    if not all([geo_col, partner_col, flow_col, product_col, period_col, value_col]):
        raise RuntimeError("Could not map required columns from extracted COMEXT file")

    text_stream = io.StringIO(payload.decode("utf-8", errors="ignore"))
    reader = csv.DictReader(text_stream, dialect=dialect)
    imports = {}
    exports = {}
    counts = {"rows": 0, "kept": 0}
    for row in reader:
        counts["rows"] += 1
        geo = str(row.get(geo_col, "")).strip().upper()
        partner = str(row.get(partner_col, "")).strip().upper()
        flow = str(row.get(flow_col, "")).strip().upper()
        product = str(row.get(product_col, "")).strip()
        period = parse_period(row.get(period_col))
        val = safe_float(row.get(value_col))
        if not period or val is None:
            continue
        if geo not in ("EU27_2020", "EU27", "EU"):
            continue
        if partner not in PARTNERS:
            continue
        if not any(product.startswith(code) for code in PRODUCT_CODES):
            continue
        counts["kept"] += 1
        if "EXP" in flow or "EXPORT" in flow:
            exports[period] = exports.get(period, 0.0) + val
        elif "IMP" in flow or "IMPORT" in flow:
            imports[period] = imports.get(period, 0.0) + val
    debug["files_api"]["row_counts"] = counts
    periods = sorted(set(imports) | set(exports), key=month_sort_key)
    if not periods:
        raise RuntimeError("No rows matched EU/partner/product/flow filters in COMEXT bulk file")
    raw_balanced = []
    exp_series = []
    imp_series = []
    for p in periods:
        e = exports.get(p, 0.0)
        i = imports.get(p, 0.0)
        exp_series.append({"period": p, "value": round(e, 2)})
        imp_series.append({"period": p, "value": round(i, 2)})
        raw_balanced.append({"period": p, "value": round(FLOW_WEIGHTS['exports'] * e + FLOW_WEIGHTS['imports'] * i, 2)})
    balanced = moving_average(raw_balanced, 3)
    exp_ma = moving_average(exp_series, 3)
    imp_ma = moving_average(imp_series, 3)
    return {
        "balanced_raw_series": raw_balanced[-WINDOW_MONTHS:],
        "balanced_series": balanced[-WINDOW_MONTHS:],
        "exports_series": exp_ma[-WINDOW_MONTHS:],
        "imports_series": imp_ma[-WINDOW_MONTHS:]
    }


def update_monthly_trade_pulse(data, changes):
    pulse = data.setdefault("monthly_trade_pulse", {})
    pulse.update({
        "scope": "EU + selected partners | imports + exports | 60/40 weighting",
        "partners": PARTNERS,
        "flows": FLOW_WEIGHTS,
        "window_months": WINDOW_MONTHS,
        "smoothing": "3M MA",
        "methodology": "Monthly balanced bundle. Shows the last 18 months after 3-month moving-average smoothing when live COMEXT extraction succeeds.",
    })
    data.setdefault("trade_signals", {})
    debug = {"updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}
    try:
        monthly = build_monthly_from_bulk(debug)
        bal = monthly["balanced_series"]
        raw = monthly["balanced_raw_series"]
        exp = monthly["exports_series"]
        imp = monthly["imports_series"]
        if not bal:
            raise RuntimeError("Monthly bulk parse returned no balanced series")
        latest = bal[-1]
        yoy = None
        if len(bal) >= 13:
            prev = bal[-13]["value"]
            yoy = round(((latest["value"] - prev) / prev) * 100, 1) if prev else None
        pulse.update({
            "status": "Live",
            "latest_period": latest["period"],
            "latest_value": latest["value"],
            "latest_raw_balanced": raw[-1]["value"] if raw else None,
            "yoy_pct": yoy,
            "note": "Monthly COMEXT bulk extraction succeeded.",
            "balanced_series": bal,
            "balanced_raw_series": raw,
            "exports_series": exp,
            "imports_series": imp,
            "debug": debug,
        })
        data["trade_signals"]["balanced_signal"] = {
            "status": "Live", "scope": "EU + selected partners", "flows": "imports + exports",
            "weights": FLOW_WEIGHTS,
            "value": latest["value"], "latest_period": latest["period"], "yoy_pct": yoy,
            "note": "Primary monthly trade pulse from COMEXT bulk file extraction."
        }
        data["trade_signals"]["exports_signal"] = {
            "status": "Live", "scope": "EU + selected partners", "flows": "exports",
            "value": exp[-1]["value"] if exp else None,
            "latest_period": exp[-1]["period"] if exp else None,
            "note": "Exports-only 3M smoothed signal."
        }
        data["trade_signals"]["imports_signal"] = {
            "status": "Live", "scope": "EU + selected partners", "flows": "imports",
            "value": imp[-1]["value"] if imp else None,
            "latest_period": imp[-1]["period"] if imp else None,
            "note": "Imports-only 3M smoothed signal."
        }
        changes.append("Monthly balanced bundle updated")
    except Exception as e:
        msg = f"Monthly COMEXT bulk extraction did not yield a usable series yet. {type(e).__name__}: {e}"
        pulse.update({
            "status": "Fetch failed",
            "latest_period": None,
            "latest_value": None,
            "latest_raw_balanced": None,
            "yoy_pct": None,
            "note": msg,
            "balanced_series": [],
            "balanced_raw_series": [],
            "exports_series": [],
            "imports_series": [],
            "debug": {"message": msg, **debug},
        })
        data["trade_signals"]["balanced_signal"] = {"status": "Fetch failed", "scope": "EU + selected partners", "flows": "imports + exports", "weights": FLOW_WEIGHTS, "note": msg}
        data["trade_signals"]["exports_signal"] = {"status": "Fetch failed", "scope": "EU + selected partners", "flows": "exports", "note": msg}
        data["trade_signals"]["imports_signal"] = {"status": "Fetch failed", "scope": "EU + selected partners", "flows": "imports", "note": msg}
        changes.append("Monthly balanced bundle fetch failed")
    data["trade_signals"]["macro_signal"] = {
        "status": "Live",
        "dataset_target": "tet00013",
        "value": data.get("customs_monitor", {}).get("latest_index"),
        "yoy_pct": data.get("customs_monitor", {}).get("yoy_pct"),
        "note": "Annual macro/proxy backdrop."
    }


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
    data.setdefault("meta", {})["last_update"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    data["meta"]["data_source"] = "GitHub Pages / same-repo JSON"
    changes.append("Tracker refreshed")
    try:
        html_text = fetch_text(IR_URL)
        headlines = parse_ir_headlines(html_text)
        if headlines:
            data["ir_headlines"] = headlines[:3]
            data.setdefault("market_signals", {})["latest_ir_signal"] = {"title": headlines[0]["title"], "meta": "Latest headline from Cell Impact investor page", "status": "Watch"}
            changes.append("IR headlines updated")
    except Exception:
        pass
    price = fetch_price()
    if price:
        data.setdefault("market_signals", {})["share_price"] = price
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
