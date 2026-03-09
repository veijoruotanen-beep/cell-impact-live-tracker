# Cell Impact Live Tracker — v19 JSON-stat mapper

This package adds a best-effort JSON-stat payload mapper for the official Eurostat Statistics API.

What changed:
- dataset target remains `tet00013`
- agent now includes:
  - JSON-stat flattening
  - dimension decoding
  - heuristic time-series extraction from payload rows
- if no reliable live series is found, the tracker safely falls back to the stored proxy series

Important:
- this is a best-effort mapper because the exact customs proxy dimensions still need validation
- the frontend is unchanged
