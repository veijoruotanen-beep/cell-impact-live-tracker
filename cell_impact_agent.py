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
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import py7zr

DATA_PATH = "data/tracker.json"
IR_URL = "https://investor.cellimpact.com/en/investor-relations"
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/CI.ST?range=5d&interval=1d"
EUROSTAT_API_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
EUROSTAT_DATASET = "tet00013"
FILES_API_LISTING = "https://ec.europa.eu/eurostat/api/dissemination/files?format=csv&dir=comext%2FCOMEXT_DATA%2FPRODUCTS&hierarchy=false&sizeFormat=NONE&dateFormat=ISO"
FILES_API_DOWNLOAD = "https://ec.europa.eu/eurostat/api/dissemination/files?file={file}"

PARTNERS = {"US", "CN", "JP", "KR"}
HS4_CODES = {"8501", "8504", "7219", "7326"}
EU_REPORTERS = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "EL", "GR", "HU", "IE", "IT",
    "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE", "EU", "EU27", "EU27_2020"
}
WINDOW_MONTHS = 18
ARCHIVES_TO_LOAD = 6


def fetch_text(url, timeout=30):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return r.read().decode("utf-8", errors="ignore")


def fetch_bytes(url, timeout=120):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return r.read()


def fetch_json_url(url, timeout=30):
    return json.loads(fetch_text(url, timeout=timeout))


def build_eurostat_url(dataset):
    return f"{EUROSTAT_API_BASE}/{dataset}?format=JSON"


def _default_data():
    return {
        "meta": {},
        "changes": [],
        "company": {},
        "market_signals": {},
        "probability": {},
        "scenarios": [],
        "monte_carlo": {},
        "ir_headlines": [],
        "customs_monitor": {"series": {}},
        "trade_signals": {},
        "monthly_trade_pulse": {},
        "what_matters_now": [],
    }


def load_data():
    p = Path(DATA_PATH)
    if not p.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
        data = _default_data()
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return data
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def save_data(data):
    p = Path(DATA_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def parse_ir_headlines(html_text):
    matches = re.findall(r'href="([^"]+)".{0,300}?>([^<]{12,160})<', html_text, flags=re.I | re.S)
    out, seen = [], set()
    for href, title in matches:
        t = html.unescape(re.sub(r"\s+", " ", title)).strip()
        if len(t) < 15:
            continue
        if t in seen:
            continue
        if not any(k in t.lower() for k in ["cell impact", "rights issue", "report", "annual", "quarter", "share", "investor"]):
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
        if not m:
            return None
        price = float(m.group(1))
        prev = float(c.group(1)) if c else None
        meta = f"CI.ST | {((price - prev) / prev) * 100:+.2f}% vs prev close" if prev else "CI.ST"
        return {"value": f"{price:.3f} SEK", "meta": meta, "status": "Live"}
    except Exception:
        return None


def jsonstat_series(payload):
    ids = payload.get("id", [])
    sizes = payload.get("size", [])
    dim = payload.get("dimension", {})
    vals = payload.get("value", {})
    if not ids or not sizes or not dim or not vals:
        return None
    try:
        time_dim = next((d for d in ids if d.lower().startswith("time")), None)
        if not time_dim:
            return None
        t_index = dim[time_dim]["category"]["index"]
        years_by_pos = {int(pos): int(code[:4]) for code, pos in t_index.items() if re.match(r"\d{4}", code)}
        series = {}
        for k, v in vals.items():
            flat = int(k) if not isinstance(k, int) else k
            coords = []
            for s in reversed(sizes):
                coords.append(flat % s)
                flat //= s
            coords = list(reversed(coords))
            year = years_by_pos.get(coords[ids.index(time_dim)])
            if year is None:
                continue
            try:
                val = float(v)
            except Exception:
                continue
            series[year] = val
        out = [{"year": y, "value": round(series[y], 2)} for y in sorted(series)]
        return {"eu_imports_proxy": out} if len(out) >= 2 else None
    except Exception:
        return None


def build_what_matters_now(data):
    runway = float(data.get("company", {}).get("runway_months", 0) or 0)
    pressure = data.get("market_signals", {}).get("dilution_pressure", {}).get("value", "Elevated")
    trade = data.get("customs_monitor", {}).get("yoy_pct", 0)
    data["what_matters_now"] = [
        {
            "label": "Cash runway",
            "value": "Short-to-moderate" if runway < 12 else "More comfortable",
            "note": f"Current tracker baseline implies roughly {runway:.1f} months of runway.",
        },
        {
            "label": "Funding pressure",
            "value": pressure,
            "note": "Funding pressure remains meaningful until commercial progress clearly reduces financing risk.",
        },
        {
            "label": "Commercial signal",
            "value": "Early but improving",
            "note": "Recent investor-page headlines suggest movement, but stronger recurring production proof is still needed.",
        },
        {
            "label": "Biggest current risk",
            "value": "Dilution before scale",
            "note": f"Even with trade backdrop at {trade:+.1f}% YoY, the key company-specific risk is financing before scale-up.",
        },
    ]


def recompute_trade_index(data):
    url = build_eurostat_url(EUROSTAT_DATASET)
    try:
        payload = fetch_json_url(url)
        live = jsonstat_series(payload)
    except Exception:
        live = None

    cm = data.get("customs_monitor", {})
    if live:
        cm["series"] = live
        cm["methodology"] = "Live Eurostat dataset tet00013 mapped via JSON-stat payload parser."

    series = cm.get("series", {})
    latest_vals = [arr[-1]["value"] for arr in series.values() if arr]
    prev_vals = [arr[-2]["value"] for arr in series.values() if len(arr) >= 2]
    if latest_vals:
        avg = sum(latest_vals) / len(latest_vals)
        prev = sum(prev_vals) / len(prev_vals) if prev_vals else avg
        yoy = ((avg - prev) / prev * 100) if prev else 0
        cm["latest_index"] = round(avg, 1)
        cm["latest_year"] = max(arr[-1]["year"] for arr in series.values() if arr)
        cm["yoy_pct"] = round(yoy, 1)
        data.setdefault("market_signals", {})
        data["market_signals"]["hydrogen_trade_pulse"] = {
            "value": f"Trade index {round(avg,1)}",
            "meta": "Customs index based on tracker series.",
            "status": "Live",
        }
    cm["source_type"] = "single-dataset proxy"
    cm["coverage_note"] = "This is a customs proxy signal, not a complete global fuel-cell trade dataset."
    cm["dataset_target"] = EUROSTAT_DATASET
    cm["next_step_note"] = "Next step: replace or supplement tet00013 with a verified COMEXT monthly bundle."
    cm["comext_dataset_target"] = "DS-059322 + DS-059332"
    data["customs_monitor"] = cm


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
    aliases = {
        "USA": "US", "UNITED STATES": "US", "CHN": "CN", "CHINA": "CN",
        "JPN": "JP", "JAPAN": "JP", "KOR": "KR", "SOUTH KOREA": "KR",
        "REPUBLIC OF KOREA": "KR",
    }
    return aliases.get(s, s)


def normalize_flow(v):
    if v is None:
        return None
    s = str(v).upper().strip()
    if "EXP" in s or s in {"1", "EXPORT", "EXPORTS", "E"}:
        return "EXP"
    if "IMP" in s or s in {"2", "IMPORT", "IMPORTS", "I"}:
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
        chunk = vals[i - window + 1:i + 1]
        out.append({"period": periods[i], "value": round(sum(chunk) / len(chunk), 2)})
    return out


def last_n(series, n):
    return series[-n:] if len(series) > n else series


def list_files_api():
    raw = fetch_text(FILES_API_LISTING, timeout=60)
    return list(csv.DictReader(io.StringIO(raw)))


def extract_archive_name(row):
    for v in row.values():
        if not isinstance(v, str):
            continue
        name = v.strip()
        m = re.search(r"^full_v2_((20\d{2})(0[1-9]|1[0-2]))\.7z$", name)
        if m:
            return name, m.group(1)
    return None, None


def choose_recent_archives(rows, debug, n=ARCHIVES_TO_LOAD):
    debug["listing_fields"] = list(rows[0].keys()) if rows else []
    debug["listing_raw_preview"] = rows[:5]
    candidates = []
    for row in rows:
        name, yyyymm = extract_archive_name(row)
        if name and yyyymm:
            candidates.append((yyyymm, name))
    candidates.sort(key=lambda x: x[0])
    debug["archive_candidates"] = [name for _, name in candidates[:120]]
    if not candidates:
        raise RuntimeError("No matching monthly full_v2_YYYYMM.7z files found in COMEXT_DATA/PRODUCTS listing")
    chosen = candidates[-n:]
    debug["selected_archives"] = [name for _, name in chosen]
    debug["selected_periods"] = [yyyymm for yyyymm, _ in chosen]
    return chosen


def download_archive_bytes(name):
    quoted = urllib.parse.quote(f"comext/COMEXT_DATA/PRODUCTS/{name}", safe="")
    return fetch_bytes(FILES_API_DOWNLOAD.format(file=quoted), timeout=120)


def extract_first_payload(blob, workdir: Path, debug):
    workdir.mkdir(parents=True, exist_ok=True)
    p1 = workdir / "first_pass"
    p1.mkdir(parents=True, exist_ok=True)
    with io.BytesIO(blob) as bio:
        with py7zr.SevenZipFile(bio, mode="r") as z:
            debug["archive_members"] = z.getnames()[:200]
            z.extractall(path=p1)
    files = [p for p in sorted(p1.rglob("*")) if p.is_file()]
    debug["extracted_all_files"] = [str(p.relative_to(workdir)) for p in files[:200]]
    for p in files:
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
    raise RuntimeError("No CSV/TSV/TXT/DAT/PARQUET/GZ payload found inside COMEXT archive")


def sniff_delimiter(sample: str):
    counts = {",": sample.count(","), ";": sample.count(";"), "\t": sample.count("\t"), "|": sample.count("|")}
    return max(counts, key=counts.get) if counts else ","


def choose_column(columns, candidates):
    lowered = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lowered:
            return lowered[cand.lower()]
    for c in columns:
        if any(cand.lower() in c.lower() for cand in candidates):
            return c
    return None


def map_columns(columns):
    return {
        "period": choose_column(columns, ["period", "time", "month", "time_period"]),
        "flow": choose_column(columns, ["flow", "trade_flow", "stk_flow", "indic_et", "trade"]),
        "geo": choose_column(columns, ["geo", "reporter", "reporting_country"]),
        "partner": choose_column(columns, ["partner", "partner_country", "partner_geo"]),
        "product": choose_column(columns, ["product_nc", "product", "prod", "commodity", "cn", "hs"]),
        "value": choose_column(columns, ["value_eur", "value", "obs_value", "trade_value"]),
    }


def process_archive_rows(reader, cmap, debug):
    debug["column_map"] = cmap
    missing = [k for k in ["period", "flow", "partner", "product", "value"] if not cmap.get(k)]
    if missing:
        raise RuntimeError(f"Required columns missing: {', '.join(missing)}")
    rows_seen = rows_kept = 0
    fail_counts = {"period": 0, "flow": 0, "partner": 0, "product": 0, "value": 0, "geo": 0}
    sample_partners, sample_geos, sample_products, sample_flows = [], [], [], []
    top_partners, top_products, top_geos, top_flows = Counter(), Counter(), Counter(), Counter()
    kept_examples = []
    exp, imp = {}, {}
    geo_col = cmap.get("geo")

    for row in reader:
        rows_seen += 1
        period = normalize_period(row.get(cmap["period"]))
        if not period:
            fail_counts["period"] += 1
            continue
        raw_flow = str(row.get(cmap["flow"], "")).strip()
        if raw_flow and len(sample_flows) < 20:
            sample_flows.append(raw_flow)
        flow = normalize_flow(raw_flow)
        if flow:
            top_flows[flow] += 1
        if flow not in {"EXP", "IMP"}:
            fail_counts["flow"] += 1
            continue
        raw_partner = str(row.get(cmap["partner"], "")).strip()
        if raw_partner and len(sample_partners) < 20:
            sample_partners.append(raw_partner)
        partner = normalize_partner(raw_partner)
        if partner:
            top_partners[partner] += 1
        if partner not in PARTNERS:
            fail_counts["partner"] += 1
            continue
        prod_raw = str(row.get(cmap["product"], "")).strip()
        if prod_raw and len(sample_products) < 20:
            sample_products.append(prod_raw)
        prod4 = re.sub(r"\D", "", prod_raw)[:4]
        if prod4:
            top_products[prod4] += 1
        if prod4 not in HS4_CODES:
            fail_counts["product"] += 1
            continue
        val = numeric_value(row.get(cmap["value"]))
        if val is None:
            fail_counts["value"] += 1
            continue
        geo = str(row.get(geo_col, "")).upper().strip() if geo_col else ""
        if geo and len(sample_geos) < 20:
            sample_geos.append(geo)
        if geo:
            top_geos[geo] += 1
        if geo_col and geo and geo not in EU_REPORTERS:
            fail_counts["geo"] += 1
            continue
        rows_kept += 1
        if len(kept_examples) < 10:
            kept_examples.append({"period": period, "flow": flow, "partner": partner, "prod4": prod4, "geo": geo, "value": val})
        target = exp if flow == "EXP" else imp
        target[period] = target.get(period, 0.0) + val

    debug.update({
        "rows_seen": rows_seen,
        "rows_kept": rows_kept,
        "fail_counts": fail_counts,
        "kept_examples": kept_examples,
        "sample_partners": sample_partners,
        "sample_geos": sample_geos,
        "sample_products": sample_products,
        "sample_flows": sample_flows,
        "top_partners": top_partners.most_common(20),
        "top_products": top_products.most_common(20),
        "top_geos": top_geos.most_common(20),
        "top_flows": top_flows.most_common(20),
    })
    return exp, imp


def ensure_monthly_scaffold(data):
    data["trade_signals"] = {
        "balanced_signal": {"status": "Pending live run", "scope": "EU + selected partners", "flows": "imports + exports", "weights": {"exports": 0.6, "imports": 0.4}, "note": "Primary monthly trade pulse. Will update after the agent completes a successful COMEXT monthly extraction."},
        "exports_signal": {"status": "Pending live run", "scope": "EU + selected partners", "flows": "exports", "note": "Closer commercial-direction signal for an EU-based supplier."},
        "imports_signal": {"status": "Pending live run", "scope": "EU + selected partners", "flows": "imports", "note": "Demand-environment signal."},
        "macro_signal": {"status": "Live", "dataset_target": "tet00013", "value": data.get("customs_monitor", {}).get("latest_index"), "yoy_pct": data.get("customs_monitor", {}).get("yoy_pct"), "note": "Annual macro/proxy backdrop."},
    }
    data["monthly_trade_pulse"] = {
        "status": "Pending live run",
        "scope": "EU + selected partners | imports + exports | 60/40 weighting",
        "partners": sorted(PARTNERS),
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
        "debug": {},
    }


def recompute_monthly_bundle(data):
    ensure_monthly_scaffold(data)
    debug = {"updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}
    data["monthly_trade_pulse"]["debug"] = debug
    try:
        rows = list_files_api()
        debug["files_api"] = {"listing_url": FILES_API_LISTING, "listing_count": len(rows)}
        selected = choose_recent_archives(rows, debug["files_api"], n=ARCHIVES_TO_LOAD)
        exp_total, imp_total = {}, {}
        processed_archives, archive_row_counts = [], []

        for yyyymm, archive_name in selected:
            archive_debug = {"archive": archive_name, "yyyymm": yyyymm}
            blob = download_archive_bytes(archive_name)
            archive_debug["download_url"] = FILES_API_DOWNLOAD.format(file=urllib.parse.quote(f"comext/COMEXT_DATA/PRODUCTS/{archive_name}", safe=""))
            workdir = Path(f"/tmp/comext_monthly_{yyyymm}")
            if workdir.exists():
                import shutil
                shutil.rmtree(workdir)
            payload_path = extract_first_payload(blob, workdir, archive_debug)

            with open(payload_path, "rb") as f:
                raw = f.read(65536)
            sample = raw.decode("utf-8", errors="ignore")
            delimiter = sniff_delimiter(sample)
            archive_debug["delimiter_guess"] = delimiter

            with open(payload_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
                reader = csv.DictReader(f, delimiter=delimiter)
                archive_debug["columns"] = reader.fieldnames or []
                cmap = map_columns(archive_debug["columns"])
                exp_part, imp_part = process_archive_rows(reader, cmap, archive_debug)

            for p, val in exp_part.items():
                exp_total[p] = exp_total.get(p, 0.0) + val
            for p, val in imp_part.items():
                imp_total[p] = imp_total.get(p, 0.0) + val
            processed_archives.append(archive_debug)
            archive_row_counts.append({"archive": archive_name, "rows_seen": archive_debug.get("rows_seen", 0), "rows_kept": archive_debug.get("rows_kept", 0)})

        debug["processed_archives"] = processed_archives[-6:]
        debug["archive_row_counts"] = archive_row_counts
        if processed_archives:
            latest = processed_archives[-1]
            for k in ["columns", "column_map", "sample_partners", "sample_geos", "sample_products", "sample_flows", "top_partners", "top_products", "top_geos", "top_flows", "fail_counts", "kept_examples"]:
                if k in latest:
                    debug[k] = latest[k]

        periods = sorted(set(exp_total.keys()) | set(imp_total.keys()))
        exports_series = [{"period": p, "value": round(exp_total.get(p, 0.0), 2)} for p in periods]
        imports_series = [{"period": p, "value": round(imp_total.get(p, 0.0), 2)} for p in periods]
        balanced_raw = [{"period": p, "value": round(0.6 * exp_total.get(p, 0.0) + 0.4 * imp_total.get(p, 0.0), 2)} for p in periods]
        exports_series = last_n(exports_series, WINDOW_MONTHS)
        imports_series = last_n(imports_series, WINDOW_MONTHS)
        balanced_raw = last_n(balanced_raw, WINDOW_MONTHS)
        balanced_ma = moving_average(balanced_raw, 3)

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

        data["monthly_trade_pulse"].update({
            "status": "Live",
            "latest_period": latest_ma["period"],
            "latest_value": latest_ma["value"],
            "latest_raw_balanced": latest_raw,
            "yoy_pct": yoy,
            "note": "Monthly balanced bundle computed from COMEXT bulk files.",
            "balanced_series": balanced_ma,
            "balanced_raw_series": balanced_raw,
            "exports_series": exports_series,
            "imports_series": imports_series,
        })
        data["trade_signals"]["balanced_signal"].update({"status": "Live", "note": "Primary monthly trade pulse from COMEXT bulk files."})
        data["trade_signals"]["exports_signal"].update({"status": "Live", "note": "Exports slice from monthly COMEXT bundle."})
        data["trade_signals"]["imports_signal"].update({"status": "Live", "note": "Imports slice from monthly COMEXT bundle."})

    except Exception as e:
        msg = f"Monthly COMEXT bulk extraction did not yield a usable series yet. {type(e).__name__}: {e}"
        for k in ("balanced_signal", "exports_signal", "imports_signal"):
            data["trade_signals"][k]["status"] = "Fetch failed"
            data["trade_signals"][k]["note"] = msg
        data["monthly_trade_pulse"]["status"] = "Fetch failed"
        data["monthly_trade_pulse"]["note"] = msg
        data["monthly_trade_pulse"]["debug"]["message"] = msg


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
            data.setdefault("market_signals", {})
            data["ir_headlines"] = headlines[:3]
            data["market_signals"]["latest_ir_signal"] = {"title": headlines[0]["title"], "meta": "Latest headline from Cell Impact investor page", "status": "Watch"}
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
    recompute_monthly_bundle(data)
    if data.get("monthly_trade_pulse", {}).get("status") == "Live":
        changes.append("Monthly balanced bundle updated")
    build_what_matters_now(data)
    changes.append("What matters now updated")
    data["changes"] = changes
    save_data(data)
    print("Tracker JSON updated.")


if __name__ == "__main__":
    main()
