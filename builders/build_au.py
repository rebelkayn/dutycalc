"""
build_au.py — Build Australia Customs Tariff database from ABF (Australian Border Force)

Source: Australian Border Force — Working Tariff (Schedule 3)
URL:    https://www.abf.gov.au/importing-exporting-and-manufacturing/tariff-classification/current-tariff/schedule-3
Data:   Chapter-by-chapter HTML (99 chapters, 10-digit Australian tariff codes)
Format: Web scrape — no bulk download available

Auto-update: Checks ABF schedule page for amendment date changes.
             Australia updates periodically (not just annually).

Usage:
    python builders/build_au.py [--force] [--chapters 1-10]

Output: data/au_tariff.json

Notes:
  - Australia uses 10-digit tariff codes
  - General rate = MFN rate
  - Most goods: 0% or 5% (very low tariff country)
  - Exceptions: some agricultural products, textiles/clothing
  - Free Trade Agreements: AUSFTA (US), AUSFTA (Japan), ChAFTA (China), etc.
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
OUTPUT_PATH  = os.path.join(DATA_DIR, "au_tariff.json")
VERSION_PATH = os.path.join(DATA_DIR, "au_version.txt")

BASE_URL    = "https://www.abf.gov.au"
MENU_URL    = f"{BASE_URL}/importing-exporting-and-manufacturing/tariff-classification/current-tariff/schedule-3"

# Chapter URL pattern: /schedule-3/chapter-{nn}
# e.g. https://www.abf.gov.au/.../schedule-3/chapter-61

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
    if not s:
        return 0.0
    s = str(s).strip().lower()
    if s in ("", "free", "0", "0.0", "-", "n/a", "nil", "exempt"):
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
    if len(digits) >= 10:
        return f"{digits[:4]}.{digits[4:6]}.{digits[6:10]}"
    if len(digits) >= 8:
        return f"{digits[:4]}.{digits[4:6]}.{digits[6:8]}"
    return f"{digits[:4]}.{digits[4:6]}"

def get_chapter_urls(retries=3) -> list:
    """Scrape the schedule-3 menu page to get all chapter URLs."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(MENU_URL, headers={"User-Agent": "iDuties/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                html = r.read().decode("utf-8", errors="replace")

            # Find links to chapters
            urls = re.findall(r'href="(/[^"]*schedule-3/chapter-\d+[^"]*)"', html)
            if not urls:
                # Fallback: construct URLs for chapters 1-99
                return [f"{MENU_URL}/chapter-{n:02d}" for n in range(1, 100)]

            seen = set()
            result = []
            for u in urls:
                full = f"{BASE_URL}{u}"
                if full not in seen:
                    seen.add(full)
                    result.append(full)
            return sorted(result)

        except Exception as e:
            if attempt == retries - 1:
                print(f"  ⚠️  Could not load menu: {e}")
                # Return constructed URLs as fallback
                return [f"{MENU_URL}/chapter-{n:02d}" for n in range(1, 100)]
            time.sleep(1)

def fetch_chapter(url: str, chapter_num: int, retries=3) -> list:
    """Fetch and parse one chapter. Returns list of (code, desc, rate)."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "iDuties/1.0 (tariff research)",
                "Accept": "text/html",
            })
            with urllib.request.urlopen(req, timeout=30) as r:
                html = r.read().decode("utf-8", errors="replace")
            break
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return []
            if attempt == retries - 1:
                return []
            time.sleep(1)
        except Exception:
            if attempt == retries - 1:
                return []
            time.sleep(1)

    parser = TariffTableParser()
    parser.feed(html)

    results = []
    header_found = False
    code_col = desc_col = rate_col = None

    for row in parser.rows:
        if not row:
            continue

        row_lower = [c.lower() for c in row]
        row_joined = " ".join(row_lower)

        # Detect header
        if not header_found and any(k in row_joined for k in
                                     ["tariff classification", "rate of duty", "description"]):
            header_found = True
            for i, cell in enumerate(row_lower):
                if any(k in cell for k in ["tariff", "classification", "statistical"]):
                    if code_col is None:
                        code_col = i
                if "description" in cell or "article" in cell:
                    if desc_col is None:
                        desc_col = i
                if any(k in cell for k in ["general", "rate", "duty"]):
                    if rate_col is None:
                        rate_col = i
            if code_col is None: code_col = 0
            if desc_col is None: desc_col = 1
            if rate_col is None: rate_col = 2
            continue

        if not header_found:
            code_col, desc_col, rate_col = 0, 1, 2
            header_found = True

        if len(row) <= max(c for c in [code_col, rate_col] if c is not None):
            continue

        code = normalize_code(row[code_col])
        if not code:
            continue

        desc = str(row[desc_col]).strip() if desc_col and desc_col < len(row) else ""
        rate = parse_rate(row[rate_col]) if rate_col and rate_col < len(row) else 0.0

        results.append((code, desc, rate))

    return results

def check_version() -> str:
    """Check for updates by looking at schedule-3 page last-modified."""
    try:
        req = urllib.request.Request(MENU_URL, headers={"User-Agent": "iDuties/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            last_mod = r.headers.get("Last-Modified", "")
            if last_mod:
                return last_mod[:16]
            html = r.read().decode("utf-8", errors="replace")
            # Look for date in page
            m = re.search(r"(\d{1,2}\s+\w+\s+20\d\d)", html)
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
    print("🇦🇺  Australia Working Tariff Builder (ABF)")
    print("─" * 40)

    current_version = check_version()
    stored_version  = load_stored_version()

    if not force and current_version == stored_version and os.path.exists(OUTPUT_PATH):
        print(f"✓  Already up to date (version: {current_version})")
        return False

    print(f"  Tariff version : {current_version}")
    print(f"  Stored version : {stored_version or '(none)'}")

    print(f"\n  Loading chapter URLs from ABF schedule...")
    chapter_urls = get_chapter_urls()

    # Filter by chapter range if specified
    if chapter_range:
        chapter_urls = [u for u in chapter_urls
                       if any(f"chapter-{n:02d}" in u or f"chapter-{n}" in u
                              for n in chapter_range)]

    print(f"  Found {len(chapter_urls)} chapters to scrape\n")

    tariff = {}
    total_codes = 0
    skipped = []

    for url in chapter_urls:
        # Extract chapter number from URL
        m = re.search(r"chapter-(\d+)", url)
        ch_num = int(m.group(1)) if m else 0

        if ch_num == 77:
            continue

        results = fetch_chapter(url, ch_num)

        if not results:
            skipped.append(ch_num)
            print(f"  Ch{ch_num:02d}: (empty)")
            time.sleep(0.5)
            continue

        ch_codes = 0
        for code, desc, rate in results:
            if len(code) >= 7:
                chapter = int(code[:2]) if code[:2].isdigit() else ch_num
                tariff[code] = {"d": desc[:120], "r": rate, "ch": chapter}
                ch_codes += 1

        total_codes += ch_codes
        print(f"  Ch{ch_num:02d}: {ch_codes} codes  (total: {total_codes})")

        time.sleep(0.8)  # Polite rate limit

    print(f"\n✓  Scraped {len(tariff)} codes across {len(chapter_urls) - len(skipped)} chapters")

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
