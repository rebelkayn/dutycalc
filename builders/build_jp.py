"""
build_jp.py — Build Japan Customs Tariff database from Japan Customs

Source: Japan Customs (Ministry of Finance)
URL:    https://www.customs.go.jp/english/tariff/2026_01_01/index.htm
Data:   Chapter-by-chapter HTML (sections 01-99)
Format: Web scrape — Japan Customs publishes well-structured HTML tables

Auto-update: Japan updates twice yearly (Jan 1 and Apr 1).
             Checks latest schedule date against stored version.

Usage:
    python builders/build_jp.py [--force] [--chapters 1-10]

Output: data/jp_tariff.json

Notes:
  - Japan uses 9-digit statistical codes for imports
  - WTO (MFN) rate is what we store as base rate
  - EPA (Economic Partnership Agreement) rates available for FTA partners
  - Temporary rates sometimes lower than General rates — we use WTO/MFN
  - Japan has complex compound rates (e.g. "3.9% or ¥60/kg, whichever is higher")
    We extract the ad valorem % component only
"""

import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from html.parser import HTMLParser

DATA_DIR     = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_PATH  = os.path.join(DATA_DIR, "jp_tariff.json")
VERSION_PATH = os.path.join(DATA_DIR, "jp_version.txt")

BASE_URL   = "https://www.customs.go.jp"
INDEX_URL  = f"{BASE_URL}/english/tariff/2026_01_01/index.htm"
# Chapter data pages follow this pattern:
CHAPTER_URL = f"{BASE_URL}/english/tariff/2026_01_01/data/e_{{nn}}.htm"

# ── HTML Parser ────────────────────────────────────────────────────────────────
class TariffTableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.current_row = []
        self.current_cell = ""
        self.rows = []
        self.depth = 0

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.in_table = True
            self.depth += 1
        if self.in_table and tag == "tr":
            self.in_row = True
            self.current_row = []
        if self.in_row and tag in ("td", "th"):
            self.in_cell = True
            self.current_cell = ""

    def handle_endtag(self, tag):
        if tag == "table":
            self.depth -= 1
            if self.depth == 0:
                self.in_table = False
        if self.in_table and tag == "tr":
            if self.current_row:
                self.rows.append(self.current_row[:])
            self.in_row = False
        if self.in_row and tag in ("td", "th"):
            self.current_row.append(self.current_cell.strip())
            self.in_cell = False

    def handle_data(self, data):
        if self.in_cell:
            self.current_cell += data

def parse_rate(s) -> float:
    """Parse Japan duty rate. Handles compound rates like '3.9% or ¥60/kg'."""
    if not s:
        return 0.0
    s = str(s).strip().lower()
    if s in ("", "free", "0", "0.0", "-", "n/a", "nil"):
        return 0.0
    # Extract ad valorem % (ignore specific/compound components)
    m = re.search(r"(\d+\.?\d*)\s*%", s)
    if m:
        return float(m.group(1))
    m = re.match(r"^(\d+\.?\d*)$", s)
    if m:
        return float(m.group(1))
    return 0.0

def normalize_code(raw) -> str:
    """Japan uses 9-digit stat codes for imports. Store as XXXX.XX.XXX"""
    if not raw:
        return ""
    s = str(raw).strip()
    digits = re.sub(r"[^\d]", "", s)
    if len(digits) < 6:
        return ""
    if len(digits) >= 9:
        return f"{digits[:4]}.{digits[4:6]}.{digits[6:9]}"
    if len(digits) >= 8:
        return f"{digits[:4]}.{digits[4:6]}.{digits[6:8]}"
    if len(digits) >= 6:
        return f"{digits[:4]}.{digits[4:6]}"
    return ""

def fetch_chapter(chapter_num: int, retries=3) -> list:
    """Fetch Japan Customs chapter page. Returns list of (code, desc, wto_rate)."""
    ch = f"{chapter_num:02d}"
    url = CHAPTER_URL.format(nn=ch)

    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "iDuties/1.0 (tariff research)",
                "Accept": "text/html",
                "Accept-Language": "en",
            })
            with urllib.request.urlopen(req, timeout=30) as r:
                html = r.read().decode("utf-8", errors="replace")
            break
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return []
            if attempt == retries - 1:
                return []
            time.sleep(2)
        except Exception:
            if attempt == retries - 1:
                return []
            time.sleep(2)

    parser = TariffTableParser()
    parser.feed(html)

    results = []
    header_found = False
    code_col = desc_col = wto_col = None

    for row in parser.rows:
        if not row or len(row) < 3:
            continue

        row_lower = [c.lower() for c in row]
        row_joined = " ".join(row_lower)

        # Japan header: "Statistical code | Description | Unit | General | Temporary | WTO | ..."
        if not header_found and any(k in row_joined for k in
                                     ["statistical", "description", "general", "wto"]):
            header_found = True
            for i, cell in enumerate(row_lower):
                if "statistical" in cell or ("no" in cell and i == 0):
                    if code_col is None: code_col = i
                if "description" in cell and desc_col is None:
                    desc_col = i
                if "wto" in cell and wto_col is None:
                    wto_col = i
                elif "general" in cell and wto_col is None:
                    wto_col = i  # Fallback to general if no WTO col
            if code_col is None: code_col = 0
            if desc_col is None: desc_col = 1
            if wto_col  is None: wto_col  = 4
            continue

        if not header_found:
            # Japan standard layout: code(0) desc(1) unit(2) general(3) temp(4) wto(5)
            code_col, desc_col, wto_col = 0, 1, 5
            header_found = True

        if len(row) <= code_col:
            continue

        code = normalize_code(row[code_col])
        if not code or len(code) < 7:
            continue

        desc = str(row[desc_col]).strip() if desc_col and desc_col < len(row) else ""
        # Try WTO rate first, fall back to general rate
        wto_rate = 0.0
        if wto_col and wto_col < len(row):
            wto_rate = parse_rate(row[wto_col])
        if wto_rate == 0.0 and desc_col and desc_col + 2 < len(row):
            # Try general rate column (usually col 3)
            general_rate = parse_rate(row[3]) if len(row) > 3 else 0.0
            wto_rate = general_rate

        results.append((code, desc, wto_rate))

    return results

def check_version() -> str:
    """Check latest Japan tariff schedule date."""
    try:
        index_url = f"{BASE_URL}/english/tariff/index.htm"
        req = urllib.request.Request(index_url, headers={"User-Agent": "iDuties/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="replace")
        # Latest schedule link e.g. "January 1, 2026"
        m = re.search(r"(January|April)\s+1,\s+(20\d\d)", html)
        if m:
            return f"{m.group(2)}_{m.group(1)[:3]}"
        return "2026_Jan"
    except Exception:
        return "2026_Jan"

def load_stored_version() -> str:
    if os.path.exists(VERSION_PATH):
        with open(VERSION_PATH) as f:
            return f.read().strip()
    return ""

def save_version(v: str):
    with open(VERSION_PATH, "w") as f:
        f.write(v)

def build(force=False, chapter_range=None):
    print("🇯🇵  Japan Customs Tariff Builder")
    print("─" * 40)

    current_version = check_version()
    stored_version  = load_stored_version()

    if not force and current_version == stored_version and os.path.exists(OUTPUT_PATH):
        print(f"✓  Already up to date (version: {current_version})")
        return False

    print(f"  Schedule version : {current_version}")
    print(f"  Stored version   : {stored_version or '(none)'}")
    print(f"  Source URL       : {INDEX_URL}")

    chapters = chapter_range or range(1, 100)
    tariff = {}
    total_codes = 0
    skipped = []

    print(f"\n  Scraping {len(list(chapters))} chapters from Japan Customs...\n")

    for ch in chapters:
        if ch == 77:
            continue

        results = fetch_chapter(ch)

        if not results:
            skipped.append(ch)
            print(f"  Ch{ch:02d}: (empty or not found)")
            time.sleep(0.5)
            continue

        ch_codes = 0
        for code, desc, rate in results:
            if len(code) >= 7:
                chapter = int(code[:2]) if code[:2].isdigit() else ch
                tariff[code] = {"d": desc[:120], "r": rate, "ch": chapter}
                ch_codes += 1

        total_codes += ch_codes
        print(f"  Ch{ch:02d}: {ch_codes} codes  (total: {total_codes})")

        time.sleep(0.8)  # Polite rate limit

    print(f"\n✓  Scraped {len(tariff)} codes across {len(list(chapters)) - len(skipped)} chapters")

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(tariff, f, separators=(",", ":"))

    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    print(f"✓  Written to {OUTPUT_PATH}  ({size_kb:.0f} KB)")

    save_version(current_version)
    return True

if __name__ == "__main__":
    force = "--force" in sys.argv
    chapter_range = None
    if "--chapters" in sys.argv:
        idx = sys.argv.index("--chapters")
        if idx + 1 < len(sys.argv):
            parts = sys.argv[idx + 1].split("-")
            if len(parts) == 2:
                chapter_range = range(int(parts[0]), int(parts[1]) + 1)
    build(force=force, chapter_range=chapter_range)
