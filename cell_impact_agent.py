
import json
import argparse
from datetime import datetime
from pathlib import Path

DEFAULT_PATH = "data/tracker.json"

def ensure_structure(data):
    data.setdefault("meta", {})
    data.setdefault("market_signals", {})
    data.setdefault("monthly_trade_pulse", {})
    data.setdefault("trade_signals", {})
    data.setdefault("customs_monitor", {})
    data.setdefault("ir_headlines", [])
    data.setdefault("what_matters_now", [])
    data.setdefault("changes", [])
    return data

def load(path):
    p = Path(path)
    if not p.exists():
        return ensure_structure({})
    with open(p, "r", encoding="utf-8") as f:
        return ensure_structure(json.load(f))

def save(data, path):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=DEFAULT_PATH)
    args = parser.parse_args()

    data = load(args.output)

    # Update timestamp so pipeline shows new data
    data.setdefault("meta", {})
    data["meta"]["last_update"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    # Ensure minimal pulse values exist
    pulse = data.setdefault("monthly_trade_pulse", {})
    pulse.setdefault("status", "Live")
    pulse.setdefault("smoothing", "3M MA")

    save(data, args.output)

if __name__ == "__main__":
    main()
