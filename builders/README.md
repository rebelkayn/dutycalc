# iDuties — Global Tariff Database

This directory contains builder scripts that download, parse, and normalize
official tariff data from each country's customs authority into a common JSON
format loaded by `app.py` at startup.

## Quick Start

```bash
# Check status of all databases
python update_all.py --status

# Update all auto-updatable sources
python update_all.py

# Force rebuild everything
python update_all.py --force

# Update a specific country
python update_all.py --country uk
python update_all.py --country eu --force
```

## Country Sources

| Country | File | Source | Method | Update Freq |
|---------|------|--------|--------|-------------|
| 🇺🇸 US  | `data/hts_data.json`  | USITC HTS | Manual Excel | Annual (Jan) |
| 🇬🇧 UK  | `data/uk_tariff.json` | UK Trade API | Auto download | Continuous |
| 🇪🇺 EU  | `data/eu_tariff.json` | TARIC / data.europa.eu | Auto + manual fallback | Annual (Nov) |
| 🇨🇦 CA  | `data/ca_tariff.json` | CBSA website | Web scraper | Annual (Jan) |
| 🇦🇺 AU  | `data/au_tariff.json` | ABF Working Tariff | Web scraper | Periodic |
| 🇯🇵 JP  | `data/jp_tariff.json` | Japan Customs | Web scraper | Jan 1 + Apr 1 |
| 🇸🇬 SG  | `data/sg_tariff.json` | Singapore Customs | Hardcoded (0%) | GST changes only |

## Individual Builder Scripts

### 🇺🇸 United States — `build_hts.py` (root level)
```bash
# 1. Download from: https://www.usitc.gov/harmonized_tariff_information
# 2. Save as: data/hts_2026.xlsx
# 3. Run:
python build_hts.py
```

### 🇬🇧 United Kingdom — `builders/build_uk.py`
```bash
python builders/build_uk.py          # Auto-downloads from UK Trade API
python builders/build_uk.py --force  # Force rebuild
```
Source: `https://data.api.trade.gov.uk/v1/datasets/uk-tariff-2021-01-01/`

### 🇪🇺 European Union — `builders/build_eu.py`
```bash
python builders/build_eu.py                        # Auto-download attempt
python builders/build_eu.py --excel taric.xlsx     # Manual Excel path
```
Source: `https://taxation-customs.ec.europa.eu/customs/customs-tariff/eu-customs-tariff-taric_en`
Manual download page: `https://data.europa.eu/data/datasets/eu-customs-tariff-taric`

### 🇨🇦 Canada — `builders/build_ca.py`
```bash
python builders/build_ca.py                  # Scrapes all 99 chapters (~5 min)
python builders/build_ca.py --chapters 1-10  # Scrape subset (testing)
python builders/build_ca.py --force          # Force rebuild
```
Source: `https://www.cbsa-asfc.gc.ca/trade-commerce/tariff-tarif/2026/menu-eng.html`

### 🇦🇺 Australia — `builders/build_au.py`
```bash
python builders/build_au.py                  # Scrapes ABF schedule-3 (~5 min)
python builders/build_au.py --chapters 1-10  # Scrape subset
```
Source: `https://www.abf.gov.au/.../tariff-classification/current-tariff/schedule-3`

### 🇯🇵 Japan — `builders/build_jp.py`
```bash
python builders/build_jp.py                  # Scrapes Japan Customs (~5 min)
python builders/build_jp.py --chapters 1-10  # Scrape subset
```
Source: `https://www.customs.go.jp/english/tariff/2026_01_01/index.htm`

### 🇸🇬 Singapore — `builders/build_sg.py`
```bash
python builders/build_sg.py  # Generates from hardcoded 0% structure
```
Singapore charges 0% on all goods except: liquor (ch22), tobacco (ch24),
petroleum (ch27 specific), motor vehicles (ch87 specific).
GST of 9% applies to all imports.

## JSON Format

All output files use this compact format:
```json
{
  "XXXX.XX.XX": {
    "d": "Product description (max 120 chars)",
    "r": 12.5,
    "ch": 61
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `d` | string | Product description |
| `r` | float | MFN duty rate (%) |
| `ch` | int | HS chapter (1–99) |

Singapore JSON has additional fields: `sg_gst`, `sg_note`, `sg_dutiable`.

## Auto-Update (GitHub Actions)

The workflow `.github/workflows/tariff-update.yml` runs every Monday at 06:00 UTC.
It checks for new versions and commits updated JSON files automatically.

Manual trigger available from GitHub → Actions → "Tariff Auto-Update" → Run workflow.

## Adding a New Country

1. Create `builders/build_XX.py` following the existing pattern
2. Add entry to `COUNTRIES` dict in `update_all.py`
3. Update `app.py` to load `data/xx_tariff.json`
4. Add country to the destination dropdown in `templates/index.html`
