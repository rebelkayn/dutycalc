"""
build_sg.py — Build Singapore Customs Tariff database

Source: Singapore Customs / SingStat STCCED
URL:    https://www.singstat.gov.sg/standards/standards-and-classifications/stcced

Singapore is unique: it charges ZERO DUTY on virtually all imports.
Only 4 categories are dutiable:
  1. Intoxicating liquors (Chapter 22)
  2. Tobacco products (Chapter 24)
  3. Motor vehicles (Chapter 87, specific headings)
  4. Petroleum products (Chapter 27, specific headings)

For iDuties purposes:
  - All goods: 0% base duty
  - Dutiable categories: flag with note (rates are complex/specific-based, not ad valorem %)
  - GST: 9% applies to all imports (separate from duty — shown as a note)

This script generates a minimal Singapore tariff file optimized for fast lookup.
Instead of scraping 17,000+ codes that are all 0%, we generate from HS structure
and flag the 4 dutiable categories.

Usage:
    python builders/build_sg.py [--force]

Output: data/sg_tariff.json
"""

import json
import os
import sys

DATA_DIR     = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_PATH  = os.path.join(DATA_DIR, "sg_tariff.json")
VERSION_PATH = os.path.join(DATA_DIR, "sg_version.txt")

# Singapore GST rate (as of 2024+)
SG_GST = 9.0

# Dutiable categories in Singapore (specific-based rates, not simple ad valorem)
# We flag these chapters/headings rather than store exact rates
# (exact rates require volume/strength calculations — out of scope for MVP)
DUTIABLE_CHAPTERS = {
    22: "Intoxicating liquors — duty based on alcohol content (S$/litre of alcohol)",
    24: "Tobacco products — duty based on weight (S$/kg)",
    27: "Petroleum products — duty based on volume (S$/litre)",  # specific headings only
    87: "Motor vehicles — duty rate varies (20% or excise duty)",  # specific headings only
}

# More specific: only certain headings in ch27 and ch87 are dutiable
DUTIABLE_HEADINGS = {
    "2710": "Petroleum oils — S$ specific duty per litre",
    "2711": "LPG / petroleum gas — S$ specific duty",
    "8702": "Motor vehicles (buses) — 20% + ARF",
    "8703": "Motor vehicles (cars) — 20% + ARF",
    "8704": "Commercial vehicles — 20% + ARF",
}

# Singapore chapter descriptions (abridged — for 0% goods we just need this)
CHAPTER_DESCS = {
    1: "Live animals", 2: "Meat and edible offal", 3: "Fish",
    4: "Dairy produce, eggs, honey", 5: "Other animal products",
    6: "Live trees and plants", 7: "Vegetables", 8: "Edible fruit and nuts",
    9: "Coffee, tea, spices", 10: "Cereals", 11: "Milling products",
    12: "Oil seeds", 13: "Lac, gums", 14: "Vegetable plaiting materials",
    15: "Animal/vegetable fats", 16: "Preparations of meat/fish",
    17: "Sugars", 18: "Cocoa", 19: "Cereal preparations",
    20: "Preparations of vegetables/fruit", 21: "Miscellaneous edible preparations",
    22: "Beverages and spirits", 23: "Residues and food waste",
    24: "Tobacco", 25: "Salt, sulphur, stone", 26: "Ores and slag",
    27: "Mineral fuels", 28: "Inorganic chemicals", 29: "Organic chemicals",
    30: "Pharmaceutical products", 31: "Fertilisers", 32: "Dyes and pigments",
    33: "Essential oils and cosmetics", 34: "Soap and waxes",
    35: "Albuminoidal substances", 36: "Explosives",
    37: "Photographic goods", 38: "Miscellaneous chemical products",
    39: "Plastics", 40: "Rubber", 41: "Raw hides and skins",
    42: "Articles of leather", 43: "Furskins", 44: "Wood", 45: "Cork",
    46: "Manufactures of straw", 47: "Pulp of wood", 48: "Paper",
    49: "Books and printed matter", 50: "Silk", 51: "Wool",
    52: "Cotton", 53: "Other vegetable textile fibres", 54: "Man-made filaments",
    55: "Man-made staple fibres", 56: "Wadding and felt",
    57: "Carpets", 58: "Special woven fabrics", 59: "Impregnated fabrics",
    60: "Knitted fabrics", 61: "Knitted clothing", 62: "Woven clothing",
    63: "Other made-up textile articles", 64: "Footwear",
    65: "Headgear", 66: "Umbrellas", 67: "Prepared feathers",
    68: "Articles of stone/plaster", 69: "Ceramic products",
    70: "Glass and glassware", 71: "Precious stones and metals",
    72: "Iron and steel", 73: "Articles of iron or steel",
    74: "Copper", 75: "Nickel", 76: "Aluminium",
    78: "Lead", 79: "Zinc", 80: "Tin", 81: "Other base metals",
    82: "Tools and cutlery", 83: "Miscellaneous articles of base metal",
    84: "Machinery and mechanical appliances", 85: "Electrical machinery",
    86: "Railway locomotives", 87: "Motor vehicles",
    88: "Aircraft", 89: "Ships and boats",
    90: "Optical instruments", 91: "Clocks and watches",
    92: "Musical instruments", 93: "Arms and ammunition",
    94: "Furniture", 95: "Toys and games", 96: "Miscellaneous manufactures",
    97: "Works of art",
}

def load_stored_version() -> str:
    if os.path.exists(VERSION_PATH):
        with open(VERSION_PATH) as f:
            return f.read().strip()
    return ""

def save_version(v: str):
    with open(VERSION_PATH, "w") as f:
        f.write(v)

def build(force=False):
    print("🇸🇬  Singapore Customs Tariff Builder")
    print("─" * 40)
    print("  Note: Singapore charges 0% import duty on almost all goods.")
    print(f"  GST of {SG_GST}% applies to all imports (tracked separately).")

    # Version = "SG_GST{rate}" — update if GST rate changes
    current_version = f"SG_GST{SG_GST}_2026"
    stored_version  = load_stored_version()

    if not force and current_version == stored_version and os.path.exists(OUTPUT_PATH):
        print(f"✓  Already up to date")
        return False

    # Build Singapore tariff — all chapters, all 0% except flagged ones
    tariff = {}

    # Generate chapter-level entries (Singapore mostly doesn't need 10-digit precision
    # since everything is 0% anyway — but we add chapter markers for UI)
    for ch, desc in CHAPTER_DESCS.items():
        code = f"{ch:04d}.00.00"
        is_dutiable = ch in DUTIABLE_CHAPTERS

        tariff[code] = {
            "d": desc,
            "r": 0.0,  # Base rate is 0% for all
            "ch": ch,
            "sg_gst": SG_GST,
            "sg_note": DUTIABLE_CHAPTERS[ch] if is_dutiable else None,
        }

    # Add specific dutiable headings with notes
    for heading, note in DUTIABLE_HEADINGS.items():
        digits = heading.replace(".", "")
        code = f"{digits[:4]}.{digits[4:6]}.00" if len(digits) >= 6 else f"{digits}.00.00"
        ch = int(digits[:2]) if len(digits) >= 2 else 0
        tariff[f"{digits[:4]}.00.00"] = {
            "d": note,
            "r": 0.0,  # Specific-based, not ad valorem — flag with note
            "ch": ch,
            "sg_gst": SG_GST,
            "sg_note": note,
            "sg_dutiable": True,
        }

    print(f"\n✓  Generated {len(tariff)} Singapore tariff entries")
    print(f"  Dutiable categories flagged: {list(DUTIABLE_CHAPTERS.keys())}")

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump({
            "meta": {
                "country": "SG",
                "gst_rate": SG_GST,
                "note": "Singapore charges 0% import duty on all goods except liquor, tobacco, petroleum, and motor vehicles.",
                "source": "https://www.customs.gov.sg",
                "version": current_version,
            },
            "tariff": tariff,
        }, f, separators=(",", ":"), indent=None)

    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    print(f"✓  Written to {OUTPUT_PATH}  ({size_kb:.0f} KB)")

    save_version(current_version)
    return True

if __name__ == "__main__":
    force = "--force" in sys.argv
    build(force=force)
