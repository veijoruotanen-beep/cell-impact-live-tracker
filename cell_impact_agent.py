#!/usr/bin/env python3
import csv
import gzip
import html
import io
import json
import re
import ssl
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import py7zr

DATA_PATH = "data/tracker.json"
IR_URL = "https://investor.cellimpact.com/en/investor-relations"
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/CI.ST?range=5d&interval=1d"

EUROSTAT_API_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
EUROSTAT_DATASET = "tet00013"
EUROSTAT_QUERY = {}

FILES_API_LISTING = "https://ec.europa.eu/eurostat/api/dissemination/files?format=csv&dir=comext%2FCOMEXT_DATA%2FPRODUCTS&hierarchy=false&sizeFormat=NONE&dateFormat=ISO"
FILES_API_DOWNLOAD = "https://ec.europa.eu/eurostat/api/dissemination/files?file={file}"

PARTNERS = {"US", "CN", "JP", "KR"}
HS4_CODES = {"8501", "8504", "7219", "7326"}
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
        data["market_signals"]["hydrogen_trade_pulse"] = {"value": f"Trade index {round(avg,1)}", "meta": "Customs index based on tracker series.", "status": "Live" if live_series else "Proxy"}

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

def list_files_api():
    raw = fetch_text(FILES_API_LISTING, timeout=60)
    return list(csv.DictReader(io.StringIO(raw)))

def extract_archive_name(row):
    for v in row.values():
        if not isinstance(v, str):
            continue
        m = re.search(r'full.*?((20\d{2})(0[1-9]|1[0-2]))\.7z$', v)
        if m:
            return v.strip(), m.group(1)
    return None, None

def choose_latest_archive(rows, debug):
    debug["listing_fields"] = list(rows[0].keys()) if rows else []
    debug["listing_raw_preview"] = rows[:5]
    candidates = []
    for row in rows:
        name, yyyymm = extract_archive_name(row)
        if name and yyyymm:
            candidates.append((yyyymm, name))
    debug["archive_candidates"] = [n for _, n in candidates[:80]]
    if not candidates:
        raise RuntimeError("No matching monthly full*_YYYYMM.7z files found in COMEXT_DATA/PRODUCTS listing")
    return sorted(candidates, key=lambda x: x[0])[-1][1], sorted(candidates, key=lambda x: x[0])[-1][0]

def download_archive_bytes(name):
    quoted = urllib.parse.quote(f"comext/COMEXT_DATA/PRODUCTS/{name}", safe="")
    return fetch_bytes(FILES_API_DOWNLOAD.format(file=quoted), timeout=120)

def extract_first_usable_payload(blob, workdir: Path, debug):
    workdir.mkdir(parents=True, exist_ok=True)
    first_pass_dir = workdir / "first_pass"
    first_pass_dir.mkdir(parents=True, exist_ok=True)
    with io.BytesIO(blob) as bio:
        with py7zr.SevenZipFile(bio, mode='r') as z:
            debug["archive_members"] = z.getnames()[:200]
            z.extractall(path=first_pass_dir)
    debug["extracted_all_files"] = [str(p.relative_to(workdir)) for p in sorted(first_pass_dir.rglob("*")) if p.is_file()][:400]
    queue = sorted(first_pass_dir.rglob("*"), key=lambda p: (not p.is_file(), len(str(p))))
    nested = []
    for p in queue:
        if not p.is_file():
            continue
        lname = p.name.lower()
        if lname.endswith((".csv", ".tsv", ".txt", ".dat", ".parquet")):
            debug["extracted_file"] = str(p.relative_to(workdir))
            debug["extracted_kind"] = p.suffix.lower()
            return p
        if lname.endswith((".csv.gz", ".tsv.gz", ".txt.gz", ".dat.gz")):
            out = workdir / p.stem
            with gzip.open(p, "rb") as src, open(out, "wb") as dst:
                dst.write(src.read())
            debug["extracted_file"] = str(out.relative_to(workdir))
            debug["extracted_kind"] = ".gz"
            return out
        if lname.endswith(".7z"):
            nested.append(p)
    if nested:
        second_dir = workdir / "second_pass"
        second_dir.mkdir(parents=True, exist_ok=True)
        with py7zr.SevenZipFile(nested[0], mode='r') as z:
            debug["nested_archive"] = str(nested[0].relative_to(workdir))
            debug["nested_members"] = z.getnames()[:200]
            z.extractall(path=second_dir)
        debug["second_pass_files"] = [str(p.relative_to(workdir)) for p in sorted(second_dir.rglob("*")) if p.is_file()][:400]
        for p in sorted(second_dir.rglob("*"), key=lambda x: len(str(x))):
            if not p.is_file():
                continue
            lname = p.name.lower()
            if lname.endswith((".csv", ".tsv", ".txt", ".dat", ".parquet")):
                debug["extracted_file"] = str(p.relative_to(workdir))
                debug["extracted_kind"] = p.suffix.lower()
                return p
            if lname.endswith((".csv.gz", ".tsv.gz", ".txt.gz", ".dat.gz")):
                out = workdir / p.stem
                with gzip.open(p, "rb") as src, open(out, "wb") as dst:
                    dst.write(src.read())
                debug["extracted_file"] = str(out.relative_to(workdir))
                debug["extracted_kind"] = ".gz"
                return out
    raise RuntimeError("No CSV/TSV/TXT/DAT/PARQUET/GZ or nested 7z payload found inside COMEXT archive")

def sniff_delimiter(sample: str):
    counts = {",": sample.count(","), ";": sample.count(";"), "\t": sample.count("\t"), "|": sample.count("|")}
    return max(counts, key=counts.get) if counts else ","

def read_tabular(path: Path, debug):
    if ".parquet" in [s.lower() for s in path.suffixes]:
        raise RuntimeError("PARQUET payload found; workflow would need pyarrow/pandas support")
    with open(path, "rb") as f:
        raw = f.read(65536)
    text = raw.decode("utf-8", errors="ignore")
    debug["delimiter_guess"] = sniff_delimiter(text)
    with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f, delimiter=debug["delimiter_guess"])
        rows = list(reader)
        debug["columns"] = reader.fieldnames or []
    return rows

def choose_column(columns, candidates):
    lowered = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lowered:
            return lowered[cand.lower()]
    for c in columns:
        cl = c.lower()
        if any(cand.lower() in cl for cand in candidates):
            return c
    return None

def map_columns(columns):
    return {
        "period": choose_column(columns, ["period", "time", "month", "time_period"]),
        "flow": choose_column(columns, ["flow", "trade_flow", "stk_flow", "indic_et", "trade"]),
        "geo": choose_column(columns, ["geo", "reporter", "reporting_country"]),
        "partner": choose_column(columns, ["partner", "partner_country", "partner_geo"]),
        "product": choose_column(columns, ["product_nc", "product", "prod", "commodity", "cn", "hs"]),
        "value": choose_column(columns, ["value_eur", "value", "obs_value", "trade_value", "valeur", "obsValue"]),
    }

def normalize_period(v):
    if v is None:
        return None
    s = str(v).strip()
    m = re.search(r"(20\d{2})[-/]?(0[1-9]|1[0-2])$", s)
    return f"{m.group(1)}-{m.group(2)}" if m else None

def normalize_partner(v):
    if v is None:
        return None
    s = str(v).upper().strip()
    aliases = {"USA":"US","UNITED STATES":"US","US":"US","CHN":"CN","CN":"CN","CHINA":"CN","JPN":"JP","JP":"JP","JAPAN":"JP","KOR":"KR","KR":"KR","SOUTH KOREA":"KR","REPUBLIC OF KOREA":"KR"}
    return aliases.get(s, s)

def normalize_flow(v):
    if v is None:
        return None
    s = str(v).upper().strip()
    if "EXP" in s or s in {"1","EXPORTS","EXPORT"}:
        return "EXP"
    if "IMP" in s or s in {"2","IMPORTS","IMPORT"}:
        return "IMP"
    return s

def numeric_value(v):
    if v is None:
        return None
    s = str(v).replace(" ", "").replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group(0)) if m else None

def moving_average(series, window=3):
    out = []
    vals = [p["value"] for p in series]
    periods = [p["period"] for p in series]
    for i in range(len(vals)):
        if i + 1 < window:
            continue
        chunk = vals[i-window+1:i+1]
        out.append({"period": periods[i], "value": round(sum(chunk)/len(chunk), 2)})
    return out

def last_n(series, n):
    return series[-n:] if len(series) > n else series

def aggregate_monthly(rows, cmap, debug):
    debug["column_map"] = cmap
    missing = [k for k in ["period","flow","partner","product","value"] if not cmap.get(k)]
    if missing:
        raise RuntimeError(f"Required columns missing: {', '.join(missing)}")
    rows_seen = 0
    rows_kept = 0
    fail_counts = {"period":0,"flow":0,"partner":0,"product":0,"value":0,"geo":0}
    kept_examples = []
    exp, imp = {}, {}
    geo_col = cmap.get("geo")
    for row in rows:
        rows_seen += 1
        period = normalize_period(row.get(cmap["period"]))
        if not period:
            fail_counts["period"] += 1
            continue
        flow = normalize_flow(row.get(cmap["flow"]))
        if flow not in {"EXP","IMP"}:
            fail_counts["flow"] += 1
            continue
        partner = normalize_partner(row.get(cmap["partner"]))
        if partner not in PARTNERS:
            fail_counts["partner"] += 1
            continue
        prod4 = re.sub(r"\D", "", str(row.get(cmap["product"], "")).strip())[:4]
        if prod4 not in HS4_CODES:
            fail_counts["product"] += 1
            continue
        val = numeric_value(row.get(cmap["value"]))
        if val is None:
            fail_counts["value"] += 1
            continue
        geo = str(row.get(geo_col, "")).upper().strip() if geo_col else ""
        if geo_col and geo and "EU27_2020" not in geo and "EU27" not in geo and geo != "EU":
            fail_counts["geo"] += 1
            continue
        rows_kept += 1
        if len(kept_examples) < 5:
            kept_examples.append({"period": period, "flow": flow, "partner": partner, "prod4": prod4, "geo": geo, "value": val})
        (exp if flow == "EXP" else imp)[period] = (exp if flow == "EXP" else imp).get(period, 0.0) + val
    debug["rows_seen"] = rows_seen
    debug["rows_kept"] = rows_kept
    debug["fail_counts"] = fail_counts
    debug["kept_examples"] = kept_examples
    periods = sorted(set(exp.keys()) | set(imp.keys()))
    exports_series = [{"period": p, "value": round(exp.get(p, 0.0), 2)} for p in periods]
    imports_series = [{"period": p, "value": round(imp.get(p, 0.0), 2)} for p in periods]
    balanced_raw = [{"period": p, "value": round(0.6*exp.get(p,0.0) + 0.4*imp.get(p,0.0), 2)} for p in periods]
    exports_series = last_n(exports_series, WINDOW_MONTHS)
    imports_series = last_n(imports_series, WINDOW_MONTHS)
    balanced_raw = last_n(balanced_raw, WINDOW_MONTHS)
    balanced_ma = moving_average(balanced_raw, 3)
    return exports_series, imports_series, balanced_raw, balanced_ma

def ensure_monthly_scaffold(data):
    data["trade_signals"] = {
        "balanced_signal":{"status":"Pending live run","scope":"EU + selected partners","flows":"imports + exports","weights":{"exports":0.6,"imports":0.4},"note":"Primary monthly trade pulse. Will update after the agent completes a successful COMEXT monthly extraction."},
        "exports_signal":{"status":"Pending live run","scope":"EU + selected partners","flows":"exports","note":"Closer commercial-direction signal for an EU-based supplier."},
        "imports_signal":{"status":"Pending live run","scope":"EU + selected partners","flows":"imports","note":"Demand-environment signal."},
        "macro_signal":{"status":"Live","dataset_target":"tet00013","value":data.get("customs_monitor",{}).get("latest_index"),"yoy_pct":data.get("customs_monitor",{}).get("yoy_pct"),"note":"Annual macro/proxy backdrop."}
    }
    data["monthly_trade_pulse"] = {"status":"Pending live run","scope":"EU + selected partners | imports + exports | 60/40 weighting","partners":sorted(PARTNERS),"flows":{"exports":0.6,"imports":0.4},"window_months":WINDOW_MONTHS,"smoothing":"3M MA","latest_period":None,"latest_value":None,"latest_raw_balanced":None,"yoy_pct":None,"methodology":"Monthly balanced bundle. Shows the last 18 months after 3-month moving-average smoothing when live COMEXT extraction succeeds.","note":"Starts empty until the updated agent writes live monthly series.","balanced_series":[],"balanced_raw_series":[],"exports_series":[],"imports_series":[],"debug":{}}

def recompute_monthly_bundle(data):
    ensure_monthly_scaffold(data)
    debug = {"updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}
    data["monthly_trade_pulse"]["debug"] = debug
    try:
        rows = list_files_api()
        debug["files_api"] = {"listing_url": FILES_API_LISTING, "listing_count": len(rows)}
        archive_name, yyyymm = choose_latest_archive(rows, debug["files_api"])
        debug["selected_archive"] = archive_name
        debug["selected_yyyymm"] = yyyymm
        blob = download_archive_bytes(archive_name)
        debug["download_url"] = FILES_API_DOWNLOAD.format(file=urllib.parse.quote(f"comext/COMEXT_DATA/PRODUCTS/{archive_name}", safe=""))
        workdir = Path("/tmp/comext_monthly")
        if workdir.exists():
            import shutil; shutil.rmtree(workdir)
        payload_path = extract_first_usable_payload(blob, workdir, debug)
        rows = read_tabular(payload_path, debug)
        cmap = map_columns(debug.get("columns", []))
        exports_series, imports_series, balanced_raw, balanced_ma = aggregate_monthly(rows, cmap, debug)
        if not balanced_ma:
            raise RuntimeError("Monthly COMEXT extraction produced no usable smoothed balanced series")
        latest_ma = balanced_ma[-1]
        latest_raw_map = {p["period"]: p["value"] for p in balanced_raw}
        latest_raw = latest_raw_map.get(latest_ma["period"])
        yoy = None
        y, m = latest_ma["period"].split("-")
        prev = latest_raw_map.get(f"{int(y)-1:04d}-{m}")
        if prev not in (None, 0):
            yoy = round(((latest_raw - prev) / prev) * 100, 1)
        data["monthly_trade_pulse"].update({"status":"Live","latest_period":latest_ma["period"],"latest_value":latest_ma["value"],"latest_raw_balanced":latest_raw,"yoy_pct":yoy,"note":"Monthly balanced bundle computed from COMEXT bulk file.","balanced_series":balanced_ma,"balanced_raw_series":balanced_raw,"exports_series":exports_series,"imports_series":imports_series})
        data["trade_signals"]["balanced_signal"].update({"status":"Live","note":"Primary monthly trade pulse from COMEXT bulk file."})
        data["trade_signals"]["exports_signal"].update({"status":"Live","note":"Exports slice from monthly COMEXT bundle."})
        data["trade_signals"]["imports_signal"].update({"status":"Live","note":"Imports slice from monthly COMEXT bundle."})
    except Exception as e:
        msg = f"Monthly COMEXT bulk extraction did not yield a usable series yet. {type(e).__name__}: {e}"
        for k in ("balanced_signal","exports_signal","imports_signal"):
            data["trade_signals"][k]["status"] = "Fetch failed"
            data["trade_signals"][k]["note"] = msg
        data["monthly_trade_pulse"]["status"] = "Fetch failed"
        data["monthly_trade_pulse"]["note"] = msg
        data["monthly_trade_pulse"]["debug"]["message"] = msg
        raise

def main():
    data = load_data()
    changes = []
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
    try:
        recompute_monthly_bundle(data)
        changes.append("Monthly balanced bundle updated")
    except Exception:
        changes.append("Monthly balanced bundle fetch failed")
    build_what_matters_now(data)
    changes.append("What matters now updated")
    data["changes"] = changes
    save_data(data)
    print("Tracker JSON updated.")

if __name__ == "__main__":
    main()
