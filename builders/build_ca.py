"""
build_ca.py — Build Canada Customs Tariff (T2026) database from CBSA

Source: Canada Border Services Agency (CBSA)
URL:    https://www.cbsa-asfc.gc.ca/trade-commerce/tariff-tarif/2026/menu-eng.html
Data:   Chapter-by-chapter HTML pages (99 chapters)
Format: Web scrape — CBSA publishes chapter-by-chapter HTML, no bulk download

Auto-update: Checks CBSA menu page for the current tariff year link.
             Re-scrapes if year in URL changes (annual update = new T20XX URL).

Usage:
    python builders/build_ca.py [--force] [--chapters 1-10]

Output: data/ca_tariff.json

Notes:
  - MFN rate = "Most-Favoured-Nation" tariff treatment
  - Canada also has GPT (General Preferential Tariff) for developing countries
  - USMCA/CUSMA rates are 0% for US/Mexico origin goods
  - We store MFN rate as base; FTA handled separately in app logic
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
OUTPUT_PATH  = os.path.join(DATA_DIR, "ca_tariff.json")
VERSION_PATH = os.path.join(DATA_DIR, "ca_version.txt")

BASE_URL     = "https://www.cbsa-asfc.gc.ca"
MENU_URL     = f"{BASE_URL}/trade-commerce/tariff-tarif/2026/menu-eng.html"
CHAPTER_BASE = f"{BASE_URL}/trade-commerce/tariff-tarif/2026/html/00"

# ── HTML Parser ────────────────────────────────────────────────────────────────
class TariffTableParser(HTMLParser):
    """Extract tariff table rows from CBSA chapter HTML pages."""

    def __init__(self):
        super().__init__()
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.current_row = []
        self.current_cell = ""
        self.rows = []
        self.table_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.in_table = True
            self.table_depth += 1
        if self.in_table and tag == "tr":
            self.in_row = True
            self.current_row = []
        if self.in_row and tag in ("td", "th"):
            self.in_cell = True
            self.current_cell = ""

    def handle_endtag(self, tag):
        if tag == "table":
            self.table_depth -= 1
            if self.table_depth == 0:
                self.in_table = False
        if self.in_table and tag == "tr":
            if self.current_row:
                self.rows.append(self.current_row[:])
            self.in_row = False
            self.current_row = []
        if self.in_row and tag in ("td", "th"):
            self.current_row.append(self.current_cell.strip())
            self.in_cell = False
            self.current_cell = ""

    def handle_data(self, data):
        if self.in_cell:
            self.current_cell += data

def parse_rate(s) -> float:
    if not s:
        return 0.0
    s = str(s).strip().lower()
    if s in ("", "free", "0", "0.0", "-", "n/a", "exempt", "nil"):
        return 0.0
    m = re.search(r"(\d+\.?\d*)\s*%", s)
    if m:
        return float(m.group(1))
    m = re.match(r"^(\d+\.?\d*)$", s)
    if m:
        return float(m.group(1))
    return 0.0

def normalize_code(raw) -> str:
    if not raw:
        return ""
    s = str(raw).strip()
    digits = re.sub(r"[^\d]", "", s)
    if len(digits) < 6:
        return ""
    if len(digits) >= 8:
        return f"{digits[:4]}.{digits[4:6]}.{digits[6:8]}"
    return f"{digits[:4]}.{digits[4:6]}"

def fetch_chapter(chapter_num: int, retries=3) -> list:
    """Fetch and parse one chapter page. Returns list of (code, desc, mfn_rate)."""
    ch = f"{chapter_num:02d}"
    url = f"{CHAPTER_BASE}/ch{ch}-eng.html"

    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "iDuties/1.0 (tariff research tool)",
                "Accept": "text/html",
            })
            with urllib.request.urlopen(req, timeout=30) as r:
                html = r.read().decode("utf-8", errors="replace")
                break
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return []  # Chapter doesn't exist (e.g. ch77 is reserved)
            if attempt == retries - 1:
                print(f"    ⚠️  Ch{ch}: HTTP {e.code}")
                return []
            time.sleep(1)
        except Exception as e:
            if attempt == retries - 1:
                print(f"    ⚠️  Ch{ch}: {e}")
                return []
            time.sleep(1)

    parser = TariffTableParser()
    parser.feed(html)

    results = []
    header_found = False
    code_col = desc_col = mfn_col = None

    for row in parser.rows:
        if not row:
            continue

        row_lower = [c.lower() for c in row]
        row_joined = " ".join(row_lower)

        # Detect header row
        if not header_found and any(k in row_joined for k in
                                     ["tariff item", "description", "mfn", "most-favoured"]):
            header_found = True
            for i, cell in enumerate(row_lower):
                if "tariff" in cell or "item" in cell or "classification" in cell:
                    code_col = i
                if "description" in cell or "article" in cell:
                    desc_col = i
                if "mfn" in cell or "most-favoured" in cell or "most favoured" in cell:
                    mfn_col = i
            # Fallback defaults for CBSA format
            if code_col is None: code_col = 0
            if desc_col is None: desc_col = 1
            if mfn_col  is None: mfn_col  = 3
            continue

        if not header_found:
            # Assume standard CBSA column layout: code(0) desc(1) unit(2) mfn(3)
            code_col, desc_col, mfn_col = 0, 1, 3
            header_found = True

        if len(row) <= max(filter(None, [code_col, mfn_col])):
            continue

        code = normalize_code(row[code_col])
        if not code:
            continue

        desc = str(row[desc_col]).strip() if desc_col and desc_col < len(row) else ""
        rate = parse_rate(row[mfn_col]) if mfn_col and mfn_col < len(row) else 0.0

        results.append((code, desc, rate))

    return results

def check_version() -> str:
    """Check current tariff year from CBSA menu page."""
    try:
        req = urllib.request.Request(MENU_URL, headers={"User-Agent": "iDuties/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="replace")
        # Look for T20XX pattern in page
        m = re.search(r"T(20\d\d)", html)
        return m.group(1) if m else "2026"
    except Exception:
        return "2026"

def load_stored_version() -> str:
    if os.path.exists(VERSION_PATH):
        with open(VERSION_PATH) as f:
            return f.read().strip()
    return ""

def save_version(v: str):
    with open(VERSION_PATH, "w") as f:
        f.write(v)

def build(force=False, chapter_range=None):
    print("🇨🇦  Canada Customs Tariff Builder (CBSA)")
    print("─" * 40)

    current_version = check_version()
    stored_version  = load_stored_version()

    if not force and current_version == stored_version and os.path.exists(OUTPUT_PATH):
        print(f"✓  Already up to date (T{current_version})")
        return False

    print(f"  Tariff year : T{current_version}")
    print(f"  Stored year : {stored_version or '(none)'}")

    chapters = chapter_range or range(1, 100)
    tariff = {}
    total_codes = 0
    skipped_chapters = []

    print(f"\n  Scraping {len(list(chapters))} chapters from CBSA...")
    print(f"  (Rate limit: ~1 req/sec to be polite)\n")

    for ch in chapters:
        if ch == 77:  # Reserved chapter in HS
            continue

        results = fetch_chapter(ch)

        if not results:
            skipped_chapters.append(ch)
            print(f"  Ch{ch:02d}: (empty or not found)")
            continue

        ch_codes = 0
        for code, desc, rate in results:
            if len(code) >= 7:
                chapter = int(code[:2]) if code[:2].isdigit() else ch
                tariff[code] = {"d": desc[:120], "r": rate, "ch": chapter}
                ch_codes += 1

        total_codes += ch_codes
        print(f"  Ch{ch:02d}: {ch_codes} codes  (total: {total_codes})")

        time.sleep(0.8)  # Be polite to CBSA servers

    print(f"\n✓  Scraped {len(tariff)} codes across {99 - len(skipped_chapters)} chapters")
    if skipped_chapters:
        print(f"   Skipped chapters: {skipped_chapters}")

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(tariff, f, separators=(",", ":"))

    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    print(f"✓  Written to {OUTPUT_PATH}  ({size_kb:.0f} KB)")

    save_version(current_version)
    return True

if __name__ == "__main__":
    force = "--force" in sys.argv

    # Optional: --chapters 1-20 to scrape subset
    chapter_range = None
    if "--chapters" in sys.argv:
        idx = sys.argv.index("--chapters")
        if idx + 1 < len(sys.argv):
            parts = sys.argv[idx + 1].split("-")
            if len(parts) == 2:
                chapter_range = range(int(parts[0]), int(parts[1]) + 1)

    build(force=force, chapter_range=chapter_range)
