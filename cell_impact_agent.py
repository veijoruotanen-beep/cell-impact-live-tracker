#!/usr/bin/env python3
import json, re, ssl, urllib.request
from datetime import datetime, timezone

DATA_PATH = "data/cell_impact_facts.json"
IR_URL = "https://investor.cellimpact.com/en/investor-relations"
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/CI.ST?range=5d&interval=1d"

# Official Eurostat Statistics API base
EUROSTAT_API_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
# Locked dataset target
EUROSTAT_DATASET = "tet00013"
EUROSTAT_QUERY = {}  # add verified filters later if needed

def fetch_text(url, timeout=20):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
        return r.read().decode("utf-8", errors="ignore")

def fetch_json_url(url, timeout=20):
    return json.loads(fetch_text(url, timeout=timeout))

def build_eurostat_url(dataset, params):
    if not dataset:
        return None
    if not params:
        return f"{EUROSTAT_API_BASE}/{dataset}?format=JSON"
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{EUROSTAT_API_BASE}/{dataset}?{query}&format=JSON"

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
        "what_matters_now": []
    }

def _resolve_data_path():
    for p in DATA_PATHS:
        if Path(p).exists():
            return p
    return DATA_PATHS[0]

def load_data():
    path = _resolve_data_path()
    p = Path(path)
    if not p.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
        data = _default_data()
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return data
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def save_data(data):
    path = _resolve_data_path()
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def parse_ir_headlines(html):
    matches = re.findall(r'href="([^"]+)".{0,300}?>([^<]{12,140})<', html, flags=re.I | re.S)
    out, seen = [], set()
    for href, title in matches:
        t = re.sub(r"\s+", " ", title).strip()
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
        # rare case: ordered list
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
    # fallback: search labels with year pattern
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
    """
    Best-effort payload mapper for JSON-stat 2.0.
    It flattens tet00013 and tries to extract a time series from rows that look most relevant
    to an energy / equipment / fuel-cell customs proxy. If nothing reliable is found, returns None.
    """
    try:
        rows = jsonstat_to_rows(payload)
        if not rows:
            return None

        time_field = choose_time_field(rows[0])
        if not time_field:
            return None

        # Score all rows by label relevance and group by year
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

        # For each year, take the highest-scoring observation. This is heuristic.
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
    data["meta"]["last_update"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    data["meta"]["data_source"] = "GitHub Pages / same-repo JSON"
    changes.append("Tracker refreshed")
    try:
        html = fetch_text(IR_URL)
        headlines = parse_ir_headlines(html)
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
    build_what_matters_now(data)
    changes.append("What matters now updated")
    data["changes"] = changes
    save_data(data)
    print("Tracker JSON updated.")

if __name__ == "__main__":
    main()
