#!/usr/bin/env python3
"""
One-time migration: Load HTS data from JSON into PostgreSQL.
Run this locally with DATABASE_URL set, or on Render shell.

Usage:
  DATABASE_URL="postgresql://..." python3 migrate_hts.py
"""
import json, os, sys, psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    print("ERROR: Set DATABASE_URL environment variable")
    sys.exit(1)

DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "hts_data.json")
if not os.path.exists(DATA_FILE):
    print(f"ERROR: {DATA_FILE} not found")
    sys.exit(1)

print(f"Connecting to database...")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Create table
print("Creating hts_codes table...")
cur.execute("""
    CREATE TABLE IF NOT EXISTS hts_codes (
        code TEXT PRIMARY KEY,
        description TEXT NOT NULL DEFAULT '',
        rate NUMERIC NOT NULL DEFAULT 0,
        cn_301 NUMERIC NOT NULL DEFAULT 0,
        chapter INTEGER NOT NULL DEFAULT 0,
        dest TEXT NOT NULL DEFAULT 'US'
    );
    CREATE INDEX IF NOT EXISTS idx_hts_codes_dest ON hts_codes(dest);
    CREATE INDEX IF NOT EXISTS idx_hts_codes_chapter ON hts_codes(chapter);
    CREATE INDEX IF NOT EXISTS idx_hts_desc_trgm ON hts_codes USING gin (description gin_trgm_ops);
""")
conn.commit()

# Load JSON
print(f"Loading {DATA_FILE}...")
with open(DATA_FILE) as f:
    raw = json.load(f)

print(f"Inserting {len(raw)} codes...")
count = 0
for code, v in raw.items():
    desc = v.get("d", v.get("desc", ""))
    rate = float(v.get("r", v.get("rate", 0)))
    cn_301 = float(v.get("c", v.get("cn_301", 0)))
    chapter = int(v.get("ch", v.get("chapter", 0)))
    
    cur.execute("""
        INSERT INTO hts_codes (code, description, rate, cn_301, chapter, dest)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (code) DO UPDATE SET
            description = EXCLUDED.description,
            rate = EXCLUDED.rate,
            cn_301 = EXCLUDED.cn_301,
            chapter = EXCLUDED.chapter,
            dest = EXCLUDED.dest
    """, (code, desc, rate, cn_301, chapter, "US"))
    count += 1
    if count % 2000 == 0:
        print(f"  ...{count}/{len(raw)}")

conn.commit()
cur.close()
conn.close()
print(f"Done! Loaded {count} HTS codes into PostgreSQL.")
