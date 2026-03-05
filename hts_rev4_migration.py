#!/usr/bin/env python3
"""
HTS Rev 4 (2026) Migration Script
===================================
Downloads the official USITC Rev 4 CSV, parses all codes with proper
rate inheritance, and loads them into PostgreSQL.

Steps implemented:
  1. Download Rev 4 CSV from USITC
  2. Handle inheritance (10-digit codes inherit from 8-digit parents)
  3. Store all codes as 10-digit
  4. Parse General Rate of Duty string into numeric value

Usage:
  # Dry run - outputs SQL file:
  python3 hts_rev4_migration.py --dry-run

  # Live migration (requires DATABASE_URL env var):
  python3 hts_rev4_migration.py

  # Point at specific DB:
  python3 hts_rev4_migration.py --db "postgresql://user:pass@host:5432/dbname"
"""

import csv
import io
import os
import re
import sys
import argparse
import urllib.request
from decimal import Decimal, InvalidOperation

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
CSV_URL = "https://www.usitc.gov/sites/default/files/tata/hts/hts_2026_revision_4_csv.csv"
SQL_OUTPUT_FILE = "hts_rev4_insert.sql"

# ---------------------------------------------------------------------------
# RATE PARSER  (Step 4)
# ---------------------------------------------------------------------------

def parse_rate_string(rate_str):
    """
    Parse HTS General Rate of Duty strings into structured data.

    Returns dict with:
      - rate_pct:     ad valorem percentage as float (e.g., 17.5) or 0.0
      - rate_specific: specific duty string (e.g., "23.5¢/liter") or ""
      - rate_raw:     original string
      - is_free:      bool

    Examples:
      "Free"                -> rate_pct=0.0, is_free=True
      "17.5%"              -> rate_pct=17.5
      "6.8%"               -> rate_pct=6.8
      "1¢/kg"              -> rate_specific="1¢/kg", rate_pct=0.0
      "23.5¢/liter + 14.9%" -> rate_specific="23.5¢/liter", rate_pct=14.9
      "0.9¢ each"          -> rate_specific="0.9¢ each", rate_pct=0.0
      "68¢/head"           -> rate_specific="68¢/head", rate_pct=0.0
      "$3/head"            -> rate_specific="$3/head", rate_pct=0.0
      ""                   -> rate_pct=0.0 (inherited - should not happen after fixup)
    """
    result = {
        "rate_pct": 0.0,
        "rate_specific": "",
        "rate_raw": rate_str.strip(),
        "is_free": False,
    }

    s = rate_str.strip()
    if not s:
        return result

    # "Free" or "free"
    if s.lower() == "free":
        result["is_free"] = True
        return result

    # Compound rate: "23.5¢/liter + 14.9%"  or  "38.5¢/kg + 8.5%"
    # Split on " + " and process each part
    parts = [p.strip() for p in s.split("+")]

    for part in parts:
        part = part.strip()
        # Pure percentage: "17.5%" or "14.9%"
        pct_match = re.match(r'^(\d+(?:\.\d+)?)\s*%$', part)
        if pct_match:
            result["rate_pct"] = float(pct_match.group(1))
            continue

        # Specific duty with ¢ or $: "23.5¢/liter", "1¢/kg", "0.9¢ each", "$3/head"
        if '¢' in part or '$' in part:
            if result["rate_specific"]:
                result["rate_specific"] += " + " + part
            else:
                result["rate_specific"] = part
            continue

        # Percentage buried in text (rare edge cases)
        pct_search = re.search(r'(\d+(?:\.\d+)?)\s*%', part)
        if pct_search:
            result["rate_pct"] = float(pct_search.group(1))
            # Keep the rest as specific
            remainder = part.replace(pct_search.group(0), "").strip()
            if remainder:
                result["rate_specific"] = remainder
            continue

        # Anything else — store as specific
        if part:
            result["rate_specific"] = part

    return result


# ---------------------------------------------------------------------------
# CSV DOWNLOAD & PARSE  (Steps 1-3)
# ---------------------------------------------------------------------------

def download_csv(url):
    """Download CSV from USITC and return as string."""
    print(f"Downloading: {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "iduties-migration/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read()
    # Handle BOM
    text = raw.decode("utf-8-sig")
    print(f"Downloaded {len(raw):,} bytes")
    return text


def normalize_hts_number(htsno):
    """
    Normalize HTS number to consistent format.
    Remove dots for comparison, keep original for display.
    Returns (display_format, digits_only, digit_count)
    """
    digits = htsno.replace(".", "")
    return htsno, digits, len(digits)


def parse_csv(csv_text):
    """
    Parse the USITC CSV with proper inheritance.

    Returns list of dicts, one per line that has an HTS number,
    with inherited rates filled in for 10-digit codes.
    """
    reader = csv.DictReader(io.StringIO(csv_text))

    # We'll track the "rate stack" — a dict keyed by indent level
    # containing the most recent rate values at each level.
    # When we encounter a code with rates, we store them.
    # When we encounter a code without rates, we inherit from parent.
    rate_stack = {}
    all_codes = []
    row_count = 0
    codes_with_rates = 0
    codes_inherited = 0

    for row in reader:
        row_count += 1
        htsno = row.get("HTS Number", "").strip()
        indent = row.get("Indent", "").strip()
        description = row.get("Description", "").strip()
        general = row.get("General Rate of Duty", "").strip()
        special = row.get("Special Rate of Duty", "").strip()
        col2 = row.get("Column 2 Rate of Duty", "").strip()
        units = row.get("Unit of Quantity", "").strip()
        quota = row.get("Quota Quantity", "").strip()
        additional = row.get("Additional Duties", "").strip()

        # Skip rows with no HTS number (these are hierarchy headers)
        if not htsno:
            continue

        try:
            indent_int = int(indent)
        except (ValueError, TypeError):
            indent_int = 0

        # Determine digit count
        display, digits, digit_count = normalize_hts_number(htsno)

        # If this row has a General Rate, store it in the rate stack
        if general:
            rate_stack[indent_int] = {
                "general": general,
                "special": special,
                "col2": col2,
            }
            # Clear any deeper indent levels (they're no longer valid)
            for k in list(rate_stack.keys()):
                if k > indent_int:
                    del rate_stack[k]

            inherited = False
            codes_with_rates += 1
        else:
            # Inherit: walk up from (indent_int - 1) to 0
            inherited_rate = None
            for look_indent in range(indent_int - 1, -1, -1):
                if look_indent in rate_stack:
                    inherited_rate = rate_stack[look_indent]
                    break

            if inherited_rate:
                general = inherited_rate["general"]
                if not special:
                    special = inherited_rate["special"]
                if not col2:
                    col2 = inherited_rate["col2"]
                inherited = True
                codes_inherited += 1
            else:
                # No parent found — this can happen for chapter headers
                inherited = False

        # Parse the rate
        parsed = parse_rate_string(general)

        code_entry = {
            "htsno": display,
            "htsno_digits": digits,
            "digit_count": digit_count,
            "indent": indent_int,
            "description": description,
            "general_raw": general,
            "general_pct": parsed["rate_pct"],
            "general_specific": parsed["rate_specific"],
            "is_free": parsed["is_free"],
            "special": special,
            "col2": col2,
            "units": units,
            "quota": quota,
            "additional_duties": additional,
            "inherited": inherited,
        }
        all_codes.append(code_entry)

    print(f"\nParsed {row_count:,} CSV rows")
    print(f"Codes extracted: {len(all_codes):,}")
    print(f"  With own rates: {codes_with_rates:,}")
    print(f"  Inherited rates: {codes_inherited:,}")

    return all_codes


# ---------------------------------------------------------------------------
# VALIDATION
# ---------------------------------------------------------------------------

def validate_sample(codes):
    """Spot-check known codes against expected values."""
    checks = {
        # (htsno, expected_general_raw, expected_pct, expected_is_free)
        "2202.99.24.00": ("17.5%", 17.5, False),    # From user's screenshot
        "2202.99.10.00": ("17%", 17.0, False),       # Chocolate milk
        "0101.30.00.00": ("6.8%", 6.8, False),       # Asses
        "0101.21.00.10": ("Free", 0.0, True),        # Purebred horses (inherited)
        "8517.13.00.00": ("Free", 0.0, True),        # Smartphones
    }

    lookup = {c["htsno"]: c for c in codes}
    print("\n--- VALIDATION ---")
    all_pass = True

    for htsno, (exp_raw, exp_pct, exp_free) in checks.items():
        entry = lookup.get(htsno)
        if not entry:
            # Try without last .00 suffix for 8-digit match
            print(f"  SKIP {htsno}: not found in parsed data")
            continue

        raw_ok = entry["general_raw"].strip() == exp_raw.strip() or \
                 (exp_free and entry["is_free"])
        pct_ok = abs(entry["general_pct"] - exp_pct) < 0.01
        free_ok = entry["is_free"] == exp_free

        status = "PASS" if (pct_ok and free_ok) else "FAIL"
        if status == "FAIL":
            all_pass = False

        print(f"  {status} {htsno}: "
              f"raw='{entry['general_raw']}' pct={entry['general_pct']} "
              f"free={entry['is_free']} inherited={entry['inherited']} "
              f"(expected: raw='{exp_raw}' pct={exp_pct} free={exp_free})")

    if all_pass:
        print("  All checks passed!")
    return all_pass


# ---------------------------------------------------------------------------
# STATS
# ---------------------------------------------------------------------------

def print_stats(codes):
    """Print distribution stats about the parsed codes."""
    digit_dist = {}
    free_count = 0
    pct_count = 0
    specific_count = 0
    compound_count = 0

    for c in codes:
        d = c["digit_count"]
        digit_dist[d] = digit_dist.get(d, 0) + 1
        if c["is_free"]:
            free_count += 1
        elif c["general_pct"] > 0 and c["general_specific"]:
            compound_count += 1
        elif c["general_pct"] > 0:
            pct_count += 1
        elif c["general_specific"]:
            specific_count += 1

    print("\n--- CODE DISTRIBUTION BY DIGIT COUNT ---")
    for d in sorted(digit_dist.keys()):
        print(f"  {d}-digit: {digit_dist[d]:,} codes")

    print(f"\n--- RATE TYPE DISTRIBUTION ---")
    print(f"  Free:      {free_count:,}")
    print(f"  Ad valorem (% only): {pct_count:,}")
    print(f"  Specific only:       {specific_count:,}")
    print(f"  Compound (% + spec): {compound_count:,}")
    print(f"  Total:     {len(codes):,}")


# ---------------------------------------------------------------------------
# SQL GENERATION
# ---------------------------------------------------------------------------

def escape_sql(s):
    """Escape single quotes for SQL."""
    if s is None:
        return ""
    return s.replace("'", "''")


def generate_sql(codes, output_file=None):
    """Generate SQL statements to create/populate the table."""
    lines = []

    # Drop and recreate table
    lines.append("-- HTS Rev 4 (2026) Migration")
    lines.append("-- Generated by hts_rev4_migration.py")
    lines.append("-- Source: USITC hts_2026_revision_4_csv.csv")
    lines.append("")
    lines.append("BEGIN;")
    lines.append("")
    lines.append("DROP TABLE IF EXISTS hts_codes_rev4;")
    lines.append("")
    lines.append("""CREATE TABLE hts_codes_rev4 (
    id SERIAL PRIMARY KEY,
    htsno VARCHAR(14) NOT NULL,
    htsno_digits VARCHAR(10) NOT NULL,
    digit_count SMALLINT NOT NULL,
    indent SMALLINT NOT NULL DEFAULT 0,
    description TEXT NOT NULL DEFAULT '',
    general_raw VARCHAR(100) NOT NULL DEFAULT '',
    general_pct REAL NOT NULL DEFAULT 0.0,
    general_specific VARCHAR(100) NOT NULL DEFAULT '',
    is_free BOOLEAN NOT NULL DEFAULT FALSE,
    special TEXT NOT NULL DEFAULT '',
    col2 VARCHAR(100) NOT NULL DEFAULT '',
    units VARCHAR(100) NOT NULL DEFAULT '',
    quota VARCHAR(100) NOT NULL DEFAULT '',
    additional_duties TEXT NOT NULL DEFAULT '',
    inherited BOOLEAN NOT NULL DEFAULT FALSE
);""")
    lines.append("")

    # Batch inserts (500 per batch for performance)
    batch_size = 500
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i + batch_size]
        lines.append(
            "INSERT INTO hts_codes_rev4 "
            "(htsno, htsno_digits, digit_count, indent, description, "
            "general_raw, general_pct, general_specific, is_free, "
            "special, col2, units, quota, additional_duties, inherited) VALUES"
        )
        values = []
        for c in batch:
            val = (
                f"('{escape_sql(c['htsno'])}', "
                f"'{escape_sql(c['htsno_digits'])}', "
                f"{c['digit_count']}, "
                f"{c['indent']}, "
                f"'{escape_sql(c['description'])}', "
                f"'{escape_sql(c['general_raw'])}', "
                f"{c['general_pct']}, "
                f"'{escape_sql(c['general_specific'])}', "
                f"{'TRUE' if c['is_free'] else 'FALSE'}, "
                f"'{escape_sql(c['special'])}', "
                f"'{escape_sql(c['col2'])}', "
                f"'{escape_sql(c['units'])}', "
                f"'{escape_sql(c['quota'])}', "
                f"'{escape_sql(c['additional_duties'])}', "
                f"{'TRUE' if c['inherited'] else 'FALSE'})"
            )
            values.append(val)
        lines.append(",\n".join(values) + ";")
        lines.append("")

    # Create indexes
    lines.append("CREATE INDEX idx_hts_htsno ON hts_codes_rev4(htsno);")
    lines.append("CREATE INDEX idx_hts_digits ON hts_codes_rev4(htsno_digits);")
    lines.append("CREATE INDEX idx_hts_digit_count ON hts_codes_rev4(digit_count);")
    lines.append("")

    # Helpful view: just 8-digit and 10-digit codes (the ones importers use)
    lines.append("""-- View: Only importable codes (8 and 10 digit)
CREATE OR REPLACE VIEW hts_importable AS
SELECT * FROM hts_codes_rev4
WHERE digit_count IN (8, 10);""")
    lines.append("")

    lines.append("COMMIT;")
    lines.append("")

    # Stats comment
    lines.append(f"-- Total codes inserted: {len(codes)}")
    ten_digit = sum(1 for c in codes if c["digit_count"] == 10)
    eight_digit = sum(1 for c in codes if c["digit_count"] == 8)
    lines.append(f"-- 10-digit codes: {ten_digit}")
    lines.append(f"-- 8-digit codes: {eight_digit}")

    sql = "\n".join(lines)

    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(sql)
        print(f"\nSQL written to: {output_file}")
        print(f"File size: {len(sql):,} bytes")

    return sql


# ---------------------------------------------------------------------------
# DIRECT DB MIGRATION (via psycopg2)
# ---------------------------------------------------------------------------

def migrate_to_db(codes, db_url):
    """Directly insert into PostgreSQL using psycopg2."""
    try:
        import psycopg2
        from psycopg2.extras import execute_values
    except ImportError:
        print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
        print("Falling back to SQL file generation...")
        return False

    print(f"\nConnecting to database...")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    try:
        # Drop old table, create new
        cur.execute("DROP TABLE IF EXISTS hts_codes_rev4 CASCADE;")
        cur.execute("""CREATE TABLE hts_codes_rev4 (
            id SERIAL PRIMARY KEY,
            htsno VARCHAR(14) NOT NULL,
            htsno_digits VARCHAR(10) NOT NULL,
            digit_count SMALLINT NOT NULL,
            indent SMALLINT NOT NULL DEFAULT 0,
            description TEXT NOT NULL DEFAULT '',
            general_raw VARCHAR(100) NOT NULL DEFAULT '',
            general_pct REAL NOT NULL DEFAULT 0.0,
            general_specific VARCHAR(100) NOT NULL DEFAULT '',
            is_free BOOLEAN NOT NULL DEFAULT FALSE,
            special TEXT NOT NULL DEFAULT '',
            col2 VARCHAR(100) NOT NULL DEFAULT '',
            units VARCHAR(100) NOT NULL DEFAULT '',
            quota VARCHAR(100) NOT NULL DEFAULT '',
            additional_duties TEXT NOT NULL DEFAULT '',
            inherited BOOLEAN NOT NULL DEFAULT FALSE
        );""")

        # Batch insert using execute_values (fast)
        insert_sql = """INSERT INTO hts_codes_rev4
            (htsno, htsno_digits, digit_count, indent, description,
             general_raw, general_pct, general_specific, is_free,
             special, col2, units, quota, additional_duties, inherited)
            VALUES %s"""

        data = [
            (c["htsno"], c["htsno_digits"], c["digit_count"], c["indent"],
             c["description"], c["general_raw"], c["general_pct"],
             c["general_specific"], c["is_free"], c["special"], c["col2"],
             c["units"], c["quota"], c["additional_duties"], c["inherited"])
            for c in codes
        ]

        print(f"Inserting {len(data):,} codes...")
        execute_values(cur, insert_sql, data, page_size=1000)

        # Create indexes
        cur.execute("CREATE INDEX idx_hts_htsno ON hts_codes_rev4(htsno);")
        cur.execute("CREATE INDEX idx_hts_digits ON hts_codes_rev4(htsno_digits);")
        cur.execute("CREATE INDEX idx_hts_digit_count ON hts_codes_rev4(digit_count);")

        # Create view
        cur.execute("""CREATE OR REPLACE VIEW hts_importable AS
            SELECT * FROM hts_codes_rev4
            WHERE digit_count IN (8, 10);""")

        conn.commit()
        print("Migration complete!")

        # Verify
        cur.execute("SELECT COUNT(*) FROM hts_codes_rev4;")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM hts_codes_rev4 WHERE digit_count = 10;")
        ten_d = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM hts_codes_rev4 WHERE digit_count = 8;")
        eight_d = cur.fetchone()[0]

        print(f"\n--- DB VERIFICATION ---")
        print(f"  Total codes:   {total:,}")
        print(f"  10-digit:      {ten_d:,}")
        print(f"  8-digit:       {eight_d:,}")

        # Spot check 2202.99.24
        cur.execute(
            "SELECT htsno, general_raw, general_pct, is_free, description "
            "FROM hts_codes_rev4 WHERE htsno LIKE '2202.99.24%' ORDER BY htsno;"
        )
        rows = cur.fetchall()
        if rows:
            print(f"\n--- SPOT CHECK: 2202.99.24 ---")
            for r in rows:
                print(f"  {r[0]}: raw='{r[1]}' pct={r[2]} free={r[3]} desc='{r[4][:60]}'")

        return True

    except Exception as e:
        conn.rollback()
        print(f"ERROR: {e}")
        return False

    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="HTS Rev 4 Migration")
    parser.add_argument("--dry-run", action="store_true",
                        help="Only generate SQL file, don't touch DB")
    parser.add_argument("--db", type=str, default=None,
                        help="PostgreSQL connection URL (overrides DATABASE_URL)")
    parser.add_argument("--csv-file", type=str, default=None,
                        help="Use local CSV file instead of downloading")
    parser.add_argument("--sql-out", type=str, default=SQL_OUTPUT_FILE,
                        help=f"SQL output file (default: {SQL_OUTPUT_FILE})")
    args = parser.parse_args()

    # Step 1: Get the CSV
    if args.csv_file:
        print(f"Reading local CSV: {args.csv_file}")
        with open(args.csv_file, "r", encoding="utf-8-sig") as f:
            csv_text = f.read()
    else:
        csv_text = download_csv(CSV_URL)

    # Step 2-3: Parse with inheritance
    codes = parse_csv(csv_text)

    # Stats
    print_stats(codes)

    # Validate
    validate_sample(codes)

    # Output
    if args.dry_run:
        generate_sql(codes, args.sql_out)
        print(f"\nDry run complete. SQL file: {args.sql_out}")
        print("To apply: psql $DATABASE_URL < " + args.sql_out)
    else:
        db_url = args.db or os.environ.get("DATABASE_URL")
        if not db_url:
            print("\nNo database URL provided. Generating SQL file instead.")
            generate_sql(codes, args.sql_out)
            print(f"To apply: psql $DATABASE_URL < {args.sql_out}")
        else:
            success = migrate_to_db(codes, db_url)
            if not success:
                print("DB migration failed, generating SQL fallback...")
                generate_sql(codes, args.sql_out)


if __name__ == "__main__":
    main()
