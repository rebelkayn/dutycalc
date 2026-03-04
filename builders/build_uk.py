"""
build_uk.py — Build UK Global Tariff (UKGT) database from UK Trade API

Source: UK Department for Business and Trade Open Data API
Data:   UK Global Tariff 2021+ (post-Brexit, replaces EU CET)
Format: CSV download via REST API — no login required, always current

Auto-update: This script checks the API's latest version tag against
             data/uk_version.txt and only rebuilds if changed.

Usage:
    python builders/build_uk.py [--force]

Output: data/uk_tariff.json
"""

import json
import csv
import os
import re
import sys
import urllib.request
import urllib.error
import io

# ── Sources ────────────────────────────────────────────────────────────────────
# Full tariff measures table — includes commodity code, measure type, duty rate
TARIFF_URL = (
    "https://data.api.trade.gov.uk/v1/datasets/uk-tariff-2021-01-01"
    "/versions/latest/tables/tariff-duties/data?format=csv&download"
)
# Metadata endpoint — check current version without downloading full file
META_URL = (
    "https://data.api.trade.gov.uk/v1/datasets/uk-tariff-2021-01-01"
    "/versions/latest"
)

DATA_DIR    = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_PATH = os.path.join(DATA_DIR, "uk_tariff.json")
VERSION_PATH = os.path.join(DATA_DIR, "uk_version.txt")

# ── Rate parser ────────────────────────────────────────────────────────────────
def parse_rate(s) -> float:
    """Parse UK duty rate string into float percentage."""
    if not s:
        return 0.0
    s = str(s).strip().lower()
    if s in ("", "free", "0", "0.0", "-", "n/a", "exempt"):
        return 0.0
    # e.g. "12.00%" or "12%"
    m = re.search(r"(\d+\.?\d*)\s*%", s)
    if m:
        return float(m.group(1))
    # Plain number
    m = re.match(r"^(\d+\.?\d*)$", s)
    if m:
        return float(m.group(1))
    return 0.0

def normalize_code(raw) -> str:
    """Normalize to XX.XXXXXX (UK uses 10-digit commodity codes)."""
    if not raw:
        return ""
    digits = re.sub(r"[^\d]", "", str(raw).strip())
    if len(digits) < 6:
        return ""
    # Store as XXXXXXXX (8-digit = CN level, drop last 2 TARIC digits)
    # But keep full 10-digit for precision
    return digits[:10] if len(digits) >= 10 else digits

def check_version() -> str:
    """Fetch current dataset version tag from API metadata."""
    try:
        req = urllib.request.Request(META_URL, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            meta = json.loads(r.read())
            return meta.get("version", "unknown")
    except Exception:
        return "unknown"

def load_stored_version() -> str:
    if os.path.exists(VERSION_PATH):
        with open(VERSION_PATH) as f:
            return f.read().strip()
    return ""

def save_version(v: str):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(VERSION_PATH, "w") as f:
        f.write(v)

# ── Main build ─────────────────────────────────────────────────────────────────
def build(force=False):
    print("🇬🇧  UK Global Tariff Builder")
    print("─" * 40)

    current_version = check_version()
    stored_version  = load_stored_version()

    if not force and current_version == stored_version and os.path.exists(OUTPUT_PATH):
        print(f"✓  Already up to date (version: {current_version})")
        print(f"   Use --force to rebuild anyway")
        return False

    print(f"  API version : {current_version}")
    print(f"  Stored ver  : {stored_version or '(none)'}")
    print(f"\n  Downloading tariff CSV from Trade API...")

    try:
        req = urllib.request.Request(TARIFF_URL, headers={"User-Agent": "iDuties/1.0"})
        with urllib.request.urlopen(req, timeout=120) as r:
            raw = r.read().decode("utf-8-sig")  # strip BOM if present
    except urllib.error.URLError as e:
        print(f"❌  Download failed: {e}")
        print(f"   URL: {TARIFF_URL}")
        sys.exit(1)

    print(f"  Downloaded {len(raw)//1024} KB — parsing...")

    reader = csv.DictReader(io.StringIO(raw))
    fieldnames = reader.fieldnames or []
    print(f"  Columns: {', '.join(fieldnames[:8])}{'...' if len(fieldnames)>8 else ''}")

    # UK API column names (may vary slightly by dataset version)
    # Typical: commodity_code, duty_expression_description, measure_type_description
    code_col = next((c for c in fieldnames if "commodity" in c.lower()), None)
    rate_col  = next((c for c in fieldnames if "duty_expression" in c.lower() and "description" in c.lower()), None)
    desc_col  = next((c for c in fieldnames if "description" in c.lower() and "commodity" in c.lower()), None)
    type_col  = next((c for c in fieldnames if "measure_type" in c.lower()), None)

    if not code_col:
        # Fallback: guess first column is code
        code_col = fieldnames[0] if fieldnames else "commodity_code"
    if not rate_col:
        rate_col = fieldnames[2] if len(fieldnames) > 2 else None

    print(f"  Using — code:'{code_col}'  rate:'{rate_col}'  type:'{type_col}'")

    tariff = {}
    skipped = 0
    processed = 0

    for row in reader:
        try:
            # Only keep MFN (Most Favoured Nation) third-country rates
            # Skip preferential/quota/suspension measures
            if type_col:
                measure_type = str(row.get(type_col, "")).lower()
                # MFN = "third country duty" or "customs union duty"
                if measure_type and not any(kw in measure_type for kw in [
                    "third country", "customs union", "mfn", "standard"
                ]):
                    skipped += 1
                    continue

            code_raw = row.get(code_col, "")
            code = normalize_code(code_raw)
            if not code or len(code) < 6:
                skipped += 1
                continue

            rate_raw = row.get(rate_col, "") if rate_col else ""
            rate = parse_rate(rate_raw)

            desc_raw = row.get(desc_col, "") if desc_col else ""
            desc = str(desc_raw).strip() if desc_raw else ""

            chapter = int(code[:2]) if code[:2].isdigit() else 0

            # Store — use CN8 (8-digit) as key, keep best (lowest MFN) rate
            key = code[:8]
            if key not in tariff or rate < tariff[key]["r"]:
                tariff[key] = {
                    "d": desc[:120],
                    "r": rate,
                    "ch": chapter,
                }
            processed += 1

            if processed % 5000 == 0:
                print(f"  Processed {processed} rows...")

        except Exception:
            skipped += 1
            continue

    print(f"\n✓  Parsed {len(tariff)} unique codes ({processed} rows, {skipped} skipped)")

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(tariff, f, separators=(",", ":"))

    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    print(f"✓  Written to {OUTPUT_PATH}  ({size_kb:.0f} KB)")

    save_version(current_version)
    print(f"✓  Version saved: {current_version}")
    return True

if __name__ == "__main__":
    force = "--force" in sys.argv
    build(force=force)
