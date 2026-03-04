"""
update_all.py — Master tariff database updater for iDuties

Checks all 6 country tariff sources for new releases and rebuilds
JSON databases only when updates are detected. Designed to run as
a scheduled job (cron/GitHub Actions) or manually.

Usage:
    python update_all.py              # Check all, rebuild if updated
    python update_all.py --force      # Force rebuild all
    python update_all.py --country us # Update specific country only
    python update_all.py --dry-run    # Check versions only, no build
    python update_all.py --status     # Show current database status

Scheduled: Add to cron for weekly checks
    0 6 * * 1 cd /path/to/dutycalc && python update_all.py >> logs/update.log 2>&1

Countries:
    us  — USITC HTS (annual, ~Jan)
    uk  — UK Global Tariff API (continuous)
    eu  — EU TARIC (annual, ~Nov/Dec)
    ca  — Canada CBSA (annual + amendments)
    au  — Australia ABF (continuous amendments)
    jp  — Japan Customs (Jan 1 + Apr 1)
    sg  — Singapore (GST rate changes only)
"""

import json
import os
import sys
import subprocess
import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT, "data")
BUILDERS_DIR = os.path.join(ROOT, "builders")
LOGS_DIR = os.path.join(ROOT, "logs")

# ── Country registry ────────────────────────────────────────────────────────────
COUNTRIES = {
    "us": {
        "name": "United States",
        "flag": "🇺🇸",
        "builder": os.path.join(ROOT, "build_hts.py"),
        "output": os.path.join(DATA_DIR, "hts_data.json"),
        "version": os.path.join(DATA_DIR, "us_version.txt"),
        "source": "https://www.usitc.gov/sites/default/files/tata/hts/bychapter/",
        "update_freq": "Annual (January) + revisions throughout year",
        "method": "auto",  # Auto-downloads latest revision from USITC
    },
    "uk": {
        "name": "United Kingdom",
        "flag": "🇬🇧",
        "builder": os.path.join(BUILDERS_DIR, "build_uk.py"),
        "output": os.path.join(DATA_DIR, "uk_tariff.json"),
        "version": os.path.join(DATA_DIR, "uk_version.txt"),
        "source": "https://data.api.trade.gov.uk/v1/datasets/uk-tariff-2021-01-01/",
        "update_freq": "Continuous (API auto-updates)",
        "method": "auto",  # Fully automatic
    },
    "eu": {
        "name": "European Union",
        "flag": "🇪🇺",
        "builder": os.path.join(BUILDERS_DIR, "build_eu.py"),
        "output": os.path.join(DATA_DIR, "eu_tariff.json"),
        "version": os.path.join(DATA_DIR, "eu_version.txt"),
        "source": "https://taxation-customs.ec.europa.eu/customs/customs-tariff/eu-customs-tariff-taric_en",
        "update_freq": "Annual (November/December for next Jan 1)",
        "method": "semi_auto",  # Auto-download attempt, fallback to manual Excel
        "notes": "If auto-download fails: download TARIC Excel and run: python builders/build_eu.py --excel path/to/taric.xlsx",
    },
    "ca": {
        "name": "Canada",
        "flag": "🇨🇦",
        "builder": os.path.join(BUILDERS_DIR, "build_ca.py"),
        "output": os.path.join(DATA_DIR, "ca_tariff.json"),
        "version": os.path.join(DATA_DIR, "ca_version.txt"),
        "source": "https://www.cbsa-asfc.gc.ca/trade-commerce/tariff-tarif/2026/menu-eng.html",
        "update_freq": "Annual (January) + amendments",
        "method": "auto",  # Web scraper — fully automatic
    },
    "au": {
        "name": "Australia",
        "flag": "🇦🇺",
        "builder": os.path.join(BUILDERS_DIR, "build_au.py"),
        "output": os.path.join(DATA_DIR, "au_tariff.json"),
        "version": os.path.join(DATA_DIR, "au_version.txt"),
        "source": "https://www.abf.gov.au/importing-exporting-and-manufacturing/tariff-classification/current-tariff/schedule-3",
        "update_freq": "Periodic amendments throughout year",
        "method": "auto",  # Web scraper — fully automatic
    },
    "jp": {
        "name": "Japan",
        "flag": "🇯🇵",
        "builder": os.path.join(BUILDERS_DIR, "build_jp.py"),
        "output": os.path.join(DATA_DIR, "jp_tariff.json"),
        "version": os.path.join(DATA_DIR, "jp_version.txt"),
        "source": "https://www.customs.go.jp/english/tariff/2026_01_01/index.htm",
        "update_freq": "Twice yearly (Jan 1 and Apr 1)",
        "method": "auto",  # Web scraper — fully automatic
    },
    "sg": {
        "name": "Singapore",
        "flag": "🇸🇬",
        "builder": os.path.join(BUILDERS_DIR, "build_sg.py"),
        "output": os.path.join(DATA_DIR, "sg_tariff.json"),
        "version": os.path.join(DATA_DIR, "sg_version.txt"),
        "source": "https://www.customs.gov.sg",
        "update_freq": "Rarely (GST rate changes only)",
        "method": "auto",  # Hardcoded — no download needed
    },
}

# ── Helpers ────────────────────────────────────────────────────────────────────
def file_size_kb(path: str) -> str:
    if not os.path.exists(path):
        return "—"
    return f"{os.path.getsize(path) / 1024:.0f} KB"

def file_modified(path: str) -> str:
    if not os.path.exists(path):
        return "—"
    ts = os.path.getmtime(path)
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")

def read_version(path: str) -> str:
    if os.path.exists(path):
        with open(path) as f:
            return f.read().strip()
    return "—"

def count_codes(json_path: str) -> str:
    if not os.path.exists(json_path):
        return "—"
    try:
        with open(json_path) as f:
            data = json.load(f)
        if isinstance(data, dict) and "tariff" in data:
            return str(len(data["tariff"]))  # Singapore nested format
        return str(len(data))
    except Exception:
        return "?"

def run_builder(country_code: str, force=False) -> bool:
    """Run a country's builder script."""
    c = COUNTRIES[country_code]
    builder = c["builder"]

    if not os.path.exists(builder):
        print(f"  ❌  Builder not found: {builder}")
        return False

    if c["method"] == "manual_download":
        print(f"\n  ⚠️   {c['flag']} {c['name']}: Requires manual download")
        print(f"  {c.get('notes', '')}")
        print(f"  Source: {c['source']}")
        return False

    cmd = [sys.executable, builder]
    if force:
        cmd.append("--force")

    print(f"\n  Running: python {os.path.basename(builder)} {'--force' if force else ''}...")
    result = subprocess.run(cmd, capture_output=False)
    return result.returncode == 0

def show_status():
    """Show current status of all databases."""
    print("\n╔══════════════════════════════════════════════════════════════╗")
    print("║              iDuties — Tariff Database Status                ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print(f"║  {'Country':<20} {'Codes':>7}  {'Size':>8}  {'Updated':<16} {'Version':<12} ║")
    print("╠══════════════════════════════════════════════════════════════╣")

    for code, c in COUNTRIES.items():
        codes = count_codes(c["output"])
        size  = file_size_kb(c["output"])
        mod   = file_modified(c["output"])
        ver   = read_version(c["version"])[:10]
        exists = "✓" if os.path.exists(c["output"]) else "✗"
        name = f"{c['flag']} {c['name']}"
        print(f"║  {exists} {name:<18} {codes:>7}  {size:>8}  {mod:<16} {ver:<12} ║")

    print("╠══════════════════════════════════════════════════════════════╣")
    print(f"║  Data dir: {DATA_DIR:<51} ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()

    for code, c in COUNTRIES.items():
        if not os.path.exists(c["output"]):
            print(f"  ⚠️   {c['flag']} {c['name']}: NOT BUILT — Run: python update_all.py --country {code}")
    print()

def update_country(code: str, force=False, dry_run=False):
    c = COUNTRIES[code]
    print(f"\n{'='*50}")
    print(f"{c['flag']}  {c['name']} ({code.upper()})")
    print(f"  Source : {c['source']}")
    print(f"  Update : {c['update_freq']}")
    print(f"  Method : {c['method']}")

    if dry_run:
        ver = read_version(c["version"])
        exists = "✓ exists" if os.path.exists(c["output"]) else "✗ missing"
        print(f"  Status : {exists}  |  stored version: {ver}")
        return

    success = run_builder(code, force=force)
    if success:
        codes = count_codes(c["output"])
        size  = file_size_kb(c["output"])
        print(f"\n  ✓  {c['name']}: {codes} codes, {size}")
    else:
        if c["method"] != "manual_download":
            print(f"\n  ❌  {c['name']}: Build failed")

def main():
    args = sys.argv[1:]
    force    = "--force"   in args
    dry_run  = "--dry-run" in args
    status   = "--status"  in args

    country = None
    if "--country" in args:
        idx = args.index("--country")
        if idx + 1 < len(args):
            country = args[idx + 1].lower()

    os.makedirs(LOGS_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)

    print(f"\n{'='*50}")
    print(f"  iDuties Tariff Auto-Updater")
    print(f"  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Mode: {'DRY RUN' if dry_run else 'FORCE REBUILD' if force else 'CHECK & UPDATE'}")
    print(f"{'='*50}")

    if status:
        show_status()
        return

    show_status()

    # Determine which countries to update
    targets = [country] if country else list(COUNTRIES.keys())

    invalid = [c for c in targets if c not in COUNTRIES]
    if invalid:
        print(f"❌  Unknown country codes: {invalid}")
        print(f"   Valid: {list(COUNTRIES.keys())}")
        sys.exit(1)

    updated = []
    failed  = []
    skipped = []

    for code in targets:
        c = COUNTRIES[code]
        try:
            if c["method"] == "manual_download":
                skipped.append(code)
                print(f"\n⚠️   {c['flag']} {c['name']}: Manual download required")
                print(f"    {c.get('notes','')}")
            else:
                update_country(code, force=force, dry_run=dry_run)
                if not dry_run and os.path.exists(c["output"]):
                    updated.append(code)
        except KeyboardInterrupt:
            print(f"\n⚠️  Interrupted during {code}")
            break
        except Exception as e:
            print(f"\n❌  {code}: Unexpected error: {e}")
            failed.append(code)

    # ── Summary ────────────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print(f"  Update Summary")
    print(f"{'='*50}")
    if dry_run:
        print("  DRY RUN — no files modified")
    else:
        if updated:  print(f"  ✓  Updated : {', '.join(c.upper() for c in updated)}")
        if skipped:  print(f"  ⚠  Manual  : {', '.join(c.upper() for c in skipped)}")
        if failed:   print(f"  ✗  Failed  : {', '.join(c.upper() for c in failed)}")

    print(f"\n  Next steps:")
    if not dry_run and updated:
        print(f"  git add data/")
        print(f"  git commit -m 'tariff update: {\" \".join(c.upper() for c in updated)}'")
        print(f"  git push")

    if "eu" in (updated + failed):

        eu = COUNTRIES["eu"]
        print(f"\n  🇪🇺 EU manual fallback:")
        print(f"  1. Visit: {eu['source']}")
        print(f"  2. Download TARIC Excel file")
        print(f"  3. Run: python builders/build_eu.py --excel path/to/taric.xlsx")

    print()

if __name__ == "__main__":
    main()
