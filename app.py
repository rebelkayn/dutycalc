import os
import json
import re
import logging
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import google.generativeai as genai

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

# ── Per-destination config ─────────────────────────────────
DEST_CONFIG = {
    "US": {
        "name": "United States", "db_file": "hts_data.json", "db_compact": True,
        "valuation": "FOB", "de_minimis": 800.0, "de_minimis_currency": "USD",
        "vat_rate": 0.0, "vat_name": None, "vat_on": None, "mpf": True,
        "notes": "FOB valuation. No federal VAT. State sales tax excluded.",
    },
    "GB": {
        "name": "United Kingdom", "db_file": "uk_tariff.json", "db_compact": True,
        "valuation": "CIF", "de_minimis": 135.0, "de_minimis_currency": "GBP",
        "vat_rate": 20.0, "vat_name": "VAT", "vat_on": "duty_inclusive", "mpf": False,
        "notes": "CIF valuation. VAT 20% on CIF + duty. De minimis GBP 135.",
    },
    "DE": {
        "name": "Germany (EU)", "db_file": "eu_tariff.json", "db_compact": True,
        "valuation": "CIF", "de_minimis": 150.0, "de_minimis_currency": "EUR",
        "vat_rate": 19.0, "vat_name": "MwSt", "vat_on": "duty_inclusive", "mpf": False,
        "notes": "CIF valuation. MwSt 19% on CIF + duty. De minimis EUR 150.",
    },
    "FR": {
        "name": "France (EU)", "db_file": "eu_tariff.json", "db_compact": True,
        "valuation": "CIF", "de_minimis": 150.0, "de_minimis_currency": "EUR",
        "vat_rate": 20.0, "vat_name": "TVA", "vat_on": "duty_inclusive", "mpf": False,
        "notes": "CIF valuation. TVA 20% on CIF + duty. De minimis EUR 150.",
    },
    "NL": {
        "name": "Netherlands (EU)", "db_file": "eu_tariff.json", "db_compact": True,
        "valuation": "CIF", "de_minimis": 150.0, "de_minimis_currency": "EUR",
        "vat_rate": 21.0, "vat_name": "BTW", "vat_on": "duty_inclusive", "mpf": False,
        "notes": "CIF valuation. BTW 21% on CIF + duty. De minimis EUR 150.",
    },
    "IT": {
        "name": "Italy (EU)", "db_file": "eu_tariff.json", "db_compact": True,
        "valuation": "CIF", "de_minimis": 150.0, "de_minimis_currency": "EUR",
        "vat_rate": 22.0, "vat_name": "IVA", "vat_on": "duty_inclusive", "mpf": False,
        "notes": "CIF valuation. IVA 22% on CIF + duty. De minimis EUR 150.",
    },
    "ES": {
        "name": "Spain (EU)", "db_file": "eu_tariff.json", "db_compact": True,
        "valuation": "CIF", "de_minimis": 150.0, "de_minimis_currency": "EUR",
        "vat_rate": 21.0, "vat_name": "IVA", "vat_on": "duty_inclusive", "mpf": False,
        "notes": "CIF valuation. IVA 21% on CIF + duty. De minimis EUR 150.",
    },
    "SE": {
        "name": "Sweden (EU)", "db_file": "eu_tariff.json", "db_compact": True,
        "valuation": "CIF", "de_minimis": 150.0, "de_minimis_currency": "EUR",
        "vat_rate": 25.0, "vat_name": "Moms", "vat_on": "duty_inclusive", "mpf": False,
        "notes": "CIF valuation. Moms 25% on CIF + duty. De minimis EUR 150.",
    },
    "CA": {
        "name": "Canada", "db_file": "ca_tariff.json", "db_compact": True,
        "valuation": "FOB", "de_minimis": 20.0, "de_minimis_currency": "CAD",
        "vat_rate": 5.0, "vat_name": "GST", "vat_on": "duty_inclusive", "mpf": False,
        "notes": "FOB valuation. GST 5% federal. Provincial tax excluded. De minimis CAD 20.",
    },
    "AU": {
        "name": "Australia", "db_file": "au_tariff.json", "db_compact": True,
        "valuation": "CIF", "de_minimis": 1000.0, "de_minimis_currency": "AUD",
        "vat_rate": 10.0, "vat_name": "GST", "vat_on": "duty_inclusive", "mpf": False,
        "notes": "CIF valuation. GST 10% on CIF + duty. De minimis AUD 1,000.",
    },
    "JP": {
        "name": "Japan", "db_file": "jp_tariff.json", "db_compact": True,
        "valuation": "CIF", "de_minimis": 10000.0, "de_minimis_currency": "JPY",
        "vat_rate": 10.0, "vat_name": "Consumption Tax", "vat_on": "duty_inclusive", "mpf": False,
        "notes": "CIF valuation. Consumption tax 10%. De minimis JPY 10,000.",
    },
    "SG": {
        "name": "Singapore", "db_file": "sg_tariff.json", "db_compact": False,
        "valuation": "CIF", "de_minimis": 400.0, "de_minimis_currency": "SGD",
        "vat_rate": 9.0, "vat_name": "GST", "vat_on": "cif_only", "mpf": False,
        "notes": "0% duty on most goods. GST 9% on CIF. De minimis SGD 400.",
    },
    "AE": {
        "name": "UAE", "db_file": None, "valuation": "CIF",
        "de_minimis": 1000.0, "de_minimis_currency": "AED",
        "vat_rate": 5.0, "vat_name": "VAT", "vat_on": "duty_inclusive", "mpf": False,
        "default_rate": 5.0, "notes": "CIF. VAT 5%. Std duty ~5%. Estimate only.",
    },
    "IN": {
        "name": "India", "db_file": None, "valuation": "CIF",
        "de_minimis": 5000.0, "de_minimis_currency": "INR",
        "vat_rate": 18.0, "vat_name": "IGST", "vat_on": "duty_inclusive", "mpf": False,
        "default_rate": 10.0, "notes": "CIF. IGST 18% std. Rates vary. Estimate only.",
    },
    "BR": {
        "name": "Brazil", "db_file": None, "valuation": "CIF",
        "de_minimis": 50.0, "de_minimis_currency": "USD",
        "vat_rate": 17.0, "vat_name": "ICMS+IPI", "vat_on": "duty_inclusive", "mpf": False,
        "default_rate": 18.0, "notes": "CIF. Brazilian taxes complex — estimate only.",
    },
    "KR": {
        "name": "South Korea", "db_file": None, "valuation": "CIF",
        "de_minimis": 150000.0, "de_minimis_currency": "KRW",
        "vat_rate": 10.0, "vat_name": "VAT", "vat_on": "duty_inclusive", "mpf": False,
        "default_rate": 8.0, "notes": "CIF. VAT 10%. Estimate only.",
    },
    "MX": {
        "name": "Mexico", "db_file": None, "valuation": "FOB",
        "de_minimis": 117.0, "de_minimis_currency": "USD",
        "vat_rate": 16.0, "vat_name": "IVA", "vat_on": "duty_inclusive", "mpf": False,
        "default_rate": 10.0, "notes": "FOB. IVA 16%. Estimate only.",
    },
    "CH": {
        "name": "Switzerland", "db_file": None, "valuation": "CIF",
        "de_minimis": 65.0, "de_minimis_currency": "CHF",
        "vat_rate": 8.1, "vat_name": "MWST", "vat_on": "duty_inclusive", "mpf": False,
        "default_rate": 3.0, "notes": "CIF. MWST 8.1%. Estimate only.",
    },
}

# ── Tariff DB loader (lazy, cached) ────────────────────────
_tariff_cache = {}

def _load_tariff_db(dest: str) -> dict:
    if dest in _tariff_cache:
        return _tariff_cache[dest]
    config = DEST_CONFIG.get(dest, {})
    db_file = config.get("db_file")
    if not db_file:
        _tariff_cache[dest] = {}
        return {}
    path = os.path.join(DATA_DIR, db_file)
    if not os.path.exists(path):
        logger.warning(f"No tariff DB for {dest}: {path}")
        _tariff_cache[dest] = {}
        return {}
    try:
        with open(path) as f:
            raw = json.load(f)
        if dest == "SG":
            raw = raw.get("tariff", {})
        compact = config.get("db_compact", True)
        db = {}
        for code, v in raw.items():
            if compact:
                db[code] = {
                    "desc":    v.get("d", v.get("desc", "")),
                    "rate":    float(v.get("r", v.get("rate", 0.0))),
                    "cn_301":  float(v.get("c", v.get("cn_301", 0.0))),
                    "chapter": v.get("ch", v.get("chapter", 0)),
                }
            else:
                db[code] = v
        logger.info(f"Loaded {len(db)} codes for {dest}")
        _tariff_cache[dest] = db
        return db
    except Exception as e:
        logger.warning(f"Could not load DB for {dest}: {e}")
        _tariff_cache[dest] = {}
        return {}

# Load US on startup
HTS_DB = _load_tariff_db("US")
if not HTS_DB:
    HTS_DB = {
        "6109.10.00": {"desc": "T-shirts, cotton, knitted", "rate": 16.5, "cn_301": 7.5, "chapter": 61},
        "8517.13.00": {"desc": "Smartphones", "rate": 0.0, "cn_301": 0.0, "chapter": 85},
        "8471.30.01": {"desc": "Laptops", "rate": 0.0, "cn_301": 0.0, "chapter": 84},
        "9503.00.00": {"desc": "Toys and games", "rate": 0.0, "cn_301": 7.5, "chapter": 95},
        "4202.92.08": {"desc": "Backpacks, textile", "rate": 17.6, "cn_301": 7.5, "chapter": 42},
        "6403.99.60": {"desc": "Footwear, leather upper", "rate": 10.0, "cn_301": 7.5, "chapter": 64},
        "6203.42.40": {"desc": "Men's trousers, cotton", "rate": 17.0, "cn_301": 7.5, "chapter": 62},
        "9401.61.40": {"desc": "Upholstered seats, wood frame", "rate": 0.0, "cn_301": 25.0, "chapter": 94},
    }
    _tariff_cache["US"] = HTS_DB

FTA_RATES = {
    "usmca":     {"name": "USMCA",           "countries": ["MX","CA"], "override": True},
    "korus":     {"name": "KORUS",            "countries": ["KR"],      "rate_modifier": 0.5},
    "singapore": {"name": "US-Singapore FTA", "countries": ["SG"],      "override": True},
}

# ── Routes ─────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/refund-guide.html")
def refund_guide():
    return render_template("refund-guide.html")

@app.route("/api/classify", methods=["POST"])
def classify():
    data       = request.json or {}
    description = data.get("description", "")
    image_b64  = data.get("image_b64")
    image_mime = data.get("image_mime", "image/jpeg")
    api_key    = data.get("api_key") or GEMINI_API_KEY
    if not api_key:
        return jsonify({"error": "No Gemini API key configured"}), 400
    if not description and not image_b64:
        return jsonify({"error": "Provide a description or invoice image"}), 400
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-2.0-flash")
        hint_codes = list(HTS_DB.items())[:200]
        hts_list = "\n".join([f"{k}: {v['desc']}" for k, v in hint_codes])
        if image_b64:
            prompt = f"""You are a customs classification expert. Analyze this commercial invoice.
Extract all fields and return ONLY valid JSON (no markdown):
{{"product_description":"...","quantity":"...","unit_value":0,"total_value":0,"currency":"USD","country_of_origin":"CN","shipping_cost":0,"insurance":0,"incoterms":"FOB","seller_name":"...","hts_code":"...","hts_description":"...","classification_confidence":"high/medium/low","classification_notes":"..."}}
Reference HTS codes:\n{hts_list}\nReturn ONLY the JSON object."""
            response = model.generate_content([prompt, {"mime_type": image_mime, "data": image_b64}])
        else:
            prompt = f"""You are a customs classification expert. Classify this product.
Return ONLY valid JSON (no markdown):
{{"hts_code":"...","hts_description":"...","classification_confidence":"high/medium/low","classification_notes":"...","alternative_codes":[]}}
Product: {description}\nReference HTS codes:\n{hts_list}\nReturn ONLY the JSON object."""
            response = model.generate_content(prompt)
        raw = re.sub(r"```(?:json)?", "", response.text.strip()).strip().rstrip("`").strip()
        result = json.loads(raw)
        hts_code = result.get("hts_code", "")
        if hts_code in HTS_DB:
            result["db_match"]    = True
            result["base_rate"]   = HTS_DB[hts_code]["rate"]
            result["cn_301_rate"] = HTS_DB[hts_code]["cn_301"]
        else:
            result["db_match"] = False
        return jsonify(result)
    except json.JSONDecodeError:
        return jsonify({"error": "Could not parse AI response"}), 500
    except Exception as e:
        logger.error(f"Classify error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/calculate", methods=["POST"])
def calculate():
    d = request.json or {}
    try:
        product_value      = float(d.get("product_value", 0))
        shipping           = float(d.get("shipping", 0))
        insurance          = float(d.get("insurance", 0))
        hts_code           = d.get("hts_code", "").strip()
        duty_rate_override = d.get("duty_rate_override")
        origin_country     = d.get("origin_country", "").upper()
        dest_country       = d.get("dest_country", "US").upper()
        fta                = d.get("fta", "none")
        currency           = d.get("currency", "USD")
        exchange_rate      = float(d.get("exchange_rate", 1.0))

        if product_value <= 0:
            return jsonify({"error": "Product value must be greater than 0"}), 400

        config = DEST_CONFIG.get(dest_country, DEST_CONFIG["US"])

        value_usd     = product_value * exchange_rate
        shipping_usd  = shipping * exchange_rate
        insurance_usd = insurance * exchange_rate

        # Valuation basis
        if config["valuation"] == "CIF":
            taxable_base = value_usd + shipping_usd + insurance_usd
        else:
            taxable_base = value_usd  # FOB

        # De minimis (USD comparison only for now)
        de_minimis = config.get("de_minimis", 0)
        de_min_curr = config.get("de_minimis_currency", "USD")
        if de_minimis > 0 and de_min_curr == "USD" and value_usd <= de_minimis:
            return jsonify({
                "de_minimis": True,
                "message": f"Value (${value_usd:,.2f}) is below the {config['name']} de minimis of ${de_minimis:,.0f}. No duties apply.",
                "total_landed_usd": round(value_usd + shipping_usd + insurance_usd, 2),
                "duty_amount": 0, "vat_amount": 0, "total_duties": 0,
                "dest_country": dest_country, "dest_name": config["name"],
            })

        # Tariff lookup
        tariff_db = _load_tariff_db(dest_country)
        db_entry  = tariff_db.get(hts_code, {})
        base_rate = float(db_entry.get("rate", config.get("default_rate", 12.0))) if db_entry else config.get("default_rate", 12.0)
        cn_301    = float(db_entry.get("cn_301", 0.0)) if db_entry else 0.0

        if duty_rate_override is not None:
            base_rate = float(duty_rate_override)
            cn_301    = 0.0

        # FTA
        fta_applied = False
        fta_name    = ""
        if fta != "none" and fta in FTA_RATES:
            fi = FTA_RATES[fta]
            if origin_country in fi.get("countries", []):
                if fi.get("override"):
                    base_rate = 0.0; cn_301 = 0.0; fta_applied = True; fta_name = fi["name"]
                else:
                    base_rate = base_rate * fi.get("rate_modifier", 1.0); fta_applied = True; fta_name = fi["name"]

        # Section 301 (US→CN only)
        china_surcharge = 0.0
        if dest_country == "US" and origin_country == "CN" and not fta_applied and cn_301 > 0:
            china_surcharge = cn_301

        effective_duty_rate = base_rate + china_surcharge
        duty_amount = taxable_base * (effective_duty_rate / 100)

        # MPF (US only)
        mpf = 0.0
        if config.get("mpf"):
            mpf = min(614.35, max(32.71, taxable_base * 0.003464))

        # VAT/GST
        vat_rate   = config.get("vat_rate", 0.0)
        vat_amount = 0.0
        if vat_rate > 0:
            vat_on = config.get("vat_on", "duty_inclusive")
            if vat_on == "duty_inclusive":
                vat_amount = (taxable_base + duty_amount) * (vat_rate / 100)
            elif vat_on == "cif_only":
                vat_amount = taxable_base * (vat_rate / 100)

        total_duties = duty_amount + mpf + vat_amount
        total_landed = value_usd + shipping_usd + insurance_usd + duty_amount + mpf + vat_amount

        return jsonify({
            "de_minimis":          False,
            "dest_country":        dest_country,
            "dest_name":           config["name"],
            "valuation_method":    config["valuation"],
            "currency":            currency,
            "exchange_rate":       exchange_rate,
            "product_value_usd":   round(value_usd, 2),
            "shipping_usd":        round(shipping_usd, 2),
            "insurance_usd":       round(insurance_usd, 2),
            "taxable_base_usd":    round(taxable_base, 2),
            "base_duty_rate":      round(base_rate, 2),
            "china_301_surcharge": round(china_surcharge, 2),
            "effective_duty_rate": round(effective_duty_rate, 2),
            "duty_amount":         round(duty_amount, 2),
            "mpf":                 round(mpf, 2),
            "vat_rate":            vat_rate,
            "vat_name":            config.get("vat_name"),
            "vat_amount":          round(vat_amount, 2),
            "total_duties":        round(total_duties, 2),
            "total_landed_usd":    round(total_landed, 2),
            "fta_applied":         fta_applied,
            "fta_name":            fta_name,
            "hts_code":            hts_code,
            "hts_desc":            db_entry.get("desc", "") if db_entry else "",
            "db_match":            bool(db_entry),
            "notes":               config.get("notes", ""),
        })

    except Exception as e:
        logger.error(f"Calculation error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/hts-search", methods=["GET"])
def hts_search():
    query = request.args.get("q", "").lower().strip()
    dest  = request.args.get("dest", "US").upper()
    if not query or len(query) < 2:
        return jsonify([])
    db = _load_tariff_db(dest) or HTS_DB
    results = []
    for code, info in db.items():
        desc = info.get("desc", info.get("d", ""))
        if query in desc.lower() or query in code:
            results.append({"code": code, "desc": desc,
                            "rate": info.get("rate", 0), "cn_301": info.get("cn_301", 0)})
        if len(results) >= 8:
            break
    return jsonify(results)


@app.route("/api/dest-config", methods=["GET"])
def dest_config_api():
    dest   = request.args.get("dest", "US").upper()
    config = DEST_CONFIG.get(dest, DEST_CONFIG["US"])
    db     = _load_tariff_db(dest)
    return jsonify({
        "dest": dest, "name": config["name"],
        "valuation": config["valuation"],
        "de_minimis": config.get("de_minimis", 0),
        "de_minimis_currency": config.get("de_minimis_currency", "USD"),
        "vat_rate": config.get("vat_rate", 0),
        "vat_name": config.get("vat_name"),
        "mpf": config.get("mpf", False),
        "db_loaded": len(db) > 0,
        "db_codes": len(db),
        "notes": config.get("notes", ""),
    })


@app.route("/health")
def health():
    dbs = {}
    for dest, cfg in DEST_CONFIG.items():
        cached = _tariff_cache.get(dest)
        if cached is None:
            path = os.path.join(DATA_DIR, cfg["db_file"]) if cfg.get("db_file") else None
            dbs[dest] = {"loaded": False, "file_exists": bool(path and os.path.exists(path)), "codes": 0}
        else:
            dbs[dest] = {"loaded": True, "codes": len(cached)}
    return jsonify({"status": "ok", "databases": dbs, "us_codes": len(HTS_DB)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
