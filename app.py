import os
import json
import re
import logging
import hashlib
import secrets
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, request, jsonify, render_template, redirect, url_for, session, g
from flask_cors import CORS
import google.generativeai as genai
import psycopg2
import psycopg2.extras
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))
app.permanent_session_lifetime = timedelta(days=30)
CORS(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ── Global error handlers (prevent worker death) ─────────
@app.errorhandler(500)
def internal_error(e):
    logger.error(f"500 error: {e}")
    return "Internal server error", 500
@app.errorhandler(Exception)
def unhandled_exception(e):
    logger.error(f"Unhandled exception: {e}", exc_info=True)
    return "Internal server error", 500
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
# Render gives postgres:// but psycopg2 requires postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
# ── Database ──────────────────────────────────────────────
def get_db():
    if not DATABASE_URL:
        return None
    if "db" not in g:
        try:
            g.db = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        except Exception as e:
            logger.error(f"DB connection failed: {e}")
            return None
    return g.db
@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()
def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            company_name TEXT DEFAULT '',
            broker_name TEXT DEFAULT '',
            broker_email TEXT DEFAULT '',
            ace_id TEXT DEFAULT '',
            phone TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS calculations (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            hts_code TEXT DEFAULT '',
            hts_desc TEXT DEFAULT '',
            origin TEXT DEFAULT '',
            destination TEXT DEFAULT 'US',
            product_value NUMERIC DEFAULT 0,
            currency TEXT DEFAULT 'USD',
            duty_rate NUMERIC DEFAULT 0,
            effective_rate NUMERIC DEFAULT 0,
            duty_amount NUMERIC DEFAULT 0,
            total_landed NUMERIC DEFAULT 0,
            result_json TEXT DEFAULT '{}',
            created_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS refund_entries (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            origin TEXT DEFAULT '',
            origin_label TEXT DEFAULT '',
            product_value NUMERIC DEFAULT 0,
            entry_period TEXT DEFAULT '',
            entry_status TEXT DEFAULT '',
            ieepa_rate NUMERIC DEFAULT 0,
            ieepa_amount NUMERIC DEFAULT 0,
            surviving_amount NUMERIC DEFAULT 0,
            est_liquidation TEXT DEFAULT '',
            protest_deadline TEXT DEFAULT '',
            filing_status TEXT DEFAULT 'pending',
            notes TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS scan_results (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            filename TEXT DEFAULT '',
            file_count INTEGER DEFAULT 0,
            total_refundable NUMERIC DEFAULT 0,
            total_entries INTEGER DEFAULT 0,
            scan_json TEXT DEFAULT '{}',
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
try:
    if DATABASE_URL:
        init_db()
        logger.info("PostgreSQL tables initialized")
    else:
        logger.warning("DATABASE_URL not set — running without database")
except Exception as e:
    logger.warning(f"DB init skipped: {e}")
logger.info(f"iDuties app starting, DB configured: {bool(DATABASE_URL)}")
# ── Auth helpers ──────────────────────────────────────────
def hash_password(password):
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 200000)
    return salt + ":" + h.hex()
def verify_password(password, stored):
    salt, h = stored.split(":", 1)
    check = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 200000)
    return check.hex() == h
def get_current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    db = get_db()
    if not db:
        return None
    cur = db.cursor()
    cur.execute("SELECT * FROM users WHERE id = %s", (uid,))
    user = cur.fetchone()
    cur.close()
    return user
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("signin"))
        return f(*args, **kwargs)
    return decorated
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
# ── HTS Database (PostgreSQL — Rev 4) ─────────────────────
# Reads from hts_codes_rev4 table (official USITC 2026 Rev 4 data)
# Returns dicts with same keys the rest of the app expects:
#   code, desc, rate, cn_301, chapter

def _get_chapter(htsno):
    """Extract chapter number (first 2 digits) from an HTS code."""
    digits = htsno.replace(".", "")
    if len(digits) >= 2 and digits[:2].isdigit():
        return int(digits[:2])
    return 0

# Section 301 chapter-level fallback rates (used when old table has no entry)
# Source: USTR Section 301 Lists 1-4 (as of 2026)
# 25% = Lists 1-3 + List 4B | 7.5% = List 4A (Phase 1 deal goods)
S301_CHAPTER_RATES = {
    28: 25.0, 29: 25.0,                          # Chemicals
    38: 25.0, 39: 25.0, 40: 25.0,                # Plastics, rubber
    44: 25.0, 48: 25.0,                          # Wood, paper
    68: 25.0, 69: 25.0, 70: 25.0,                # Stone, ceramics, glass
    72: 25.0, 73: 25.0,                          # Steel
    74: 25.0, 75: 25.0, 76: 25.0, 79: 25.0,     # Base metals
    82: 25.0, 83: 25.0,                          # Tools, misc metal
    84: 25.0, 85: 25.0,                          # Machinery, electronics
    86: 25.0, 87: 25.0, 88: 25.0, 89: 25.0,     # Transport
    90: 25.0, 91: 25.0, 94: 25.0, 95: 25.0,     # Optical, furniture, toys
    # List 4A — 7.5% (Phase 1 deal, lower rate)
    61: 7.5,  62: 7.5,  63: 7.5,                # Apparel & textiles
    64: 7.5,  65: 7.5,                           # Footwear, headgear
}

def _get_cn301_from_old_table(db, code_8digit):
    """Look up Section 301 rate from old hts_codes table.
    Falls back to chapter-level rate if not found."""
    try:
        cur = db.cursor()
        cur.execute("SELECT cn_301 FROM hts_codes WHERE code = %s LIMIT 1", (code_8digit,))
        row = cur.fetchone()
        cur.close()
        if row and float(row["cn_301"]) > 0:
            return float(row["cn_301"])
    except Exception:
        pass  # Old table might not exist — that is fine

    # Fallback: derive chapter from code and apply known S.301 rate
    try:
        digits = code_8digit.replace(".", "")
        chapter = int(digits[:2])
        return S301_CHAPTER_RATES.get(chapter, 0.0)
    except Exception:
        return 0.0

def hts_lookup(code, dest="US"):
    """Look up a single HTS code from Rev 4 table.
    Tries exact match, then 10-digit with .00 suffix, then prefix search."""
    if dest != "US":
        # Non-US destinations still use old table if it exists
        db = get_db()
        if not db:
            return None
        cur = db.cursor()
        try:
            cur.execute("SELECT code, description AS desc, rate, cn_301, chapter FROM hts_codes WHERE code=%s AND dest=%s", (code, dest))
            row = cur.fetchone()
            cur.close()
            return dict(row) if row else None
        except Exception:
            cur.close()
            return None

    db = get_db()
    if not db:
        return None
    cur = db.cursor()

    code = code.strip()

    # Try exact match first
    cur.execute("""SELECT htsno, description, general_pct, general_raw, general_specific, is_free
                   FROM hts_codes_rev4 WHERE htsno = %s LIMIT 1""", (code,))
    row = cur.fetchone()

    # If no match and code is 8 digits, try adding .00
    if not row:
        digits = code.replace(".", "")
        if len(digits) == 8:
            code_10 = code + ".00"
            cur.execute("""SELECT htsno, description, general_pct, general_raw, general_specific, is_free
                           FROM hts_codes_rev4 WHERE htsno = %s LIMIT 1""", (code_10,))
            row = cur.fetchone()

    # If still no match, try prefix search
    if not row:
        cur.execute("""SELECT htsno, description, general_pct, general_raw, general_specific, is_free
                       FROM hts_codes_rev4 WHERE htsno LIKE %s ORDER BY htsno LIMIT 1""", (code + "%",))
        row = cur.fetchone()

    cur.close()
    if not row:
        return None

    # Build 8-digit version for cn_301 lookup against old table
    digits = row["htsno"].replace(".", "")
    if len(digits) >= 8:
        code_8 = digits[:4] + "." + digits[4:6] + "." + digits[6:8]
    else:
        code_8 = row["htsno"]
    cn_301 = _get_cn301_from_old_table(db, code_8)

    return {
        "code": row["htsno"],
        "desc": row["description"],
        "rate": float(row["general_pct"]),
        "rate_raw": row["general_raw"],
        "rate_specific": row["general_specific"],
        "is_free": row["is_free"],
        "cn_301": cn_301,
        "chapter": _get_chapter(row["htsno"]),
    }

def hts_search_db(query, dest="US", limit=8):
    """Search HTS codes by code prefix or description text."""
    if dest != "US":
        db = get_db()
        if not db:
            return []
        cur = db.cursor()
        q = f"%{query.lower()}%"
        try:
            cur.execute("""SELECT code, description AS desc, rate, cn_301
                           FROM hts_codes WHERE dest=%s AND (LOWER(description) LIKE %s OR code LIKE %s)
                           ORDER BY code LIMIT %s""", (dest, q, q, limit))
            rows = cur.fetchall()
            cur.close()
            return [dict(r) for r in rows]
        except Exception:
            cur.close()
            return []

    db = get_db()
    if not db:
        return []
    cur = db.cursor()
    q = f"%{query.lower()}%"
    cur.execute("""SELECT htsno AS code, description AS desc, general_pct AS rate, is_free
                   FROM hts_codes_rev4
                   WHERE (LOWER(description) LIKE %s OR htsno LIKE %s)
                   AND digit_count IN (8, 10)
                   ORDER BY htsno LIMIT %s""", (q, q, limit))
    rows = cur.fetchall()
    cur.close()
    results = []
    for r in rows:
        results.append({
            "code": r["code"],
            "desc": r["desc"],
            "rate": float(r["rate"]),
            "cn_301": 0,
        })
    return results

def hts_sample(dest="US", n=200):
    """Get evenly spaced sample of codes for AI hints."""
    if dest != "US":
        db = get_db()
        if not db:
            return []
        cur = db.cursor()
        try:
            cur.execute("SELECT code, description AS desc FROM hts_codes WHERE dest=%s ORDER BY code", (dest,))
            rows = cur.fetchall()
            cur.close()
            return [dict(r) for r in rows]
        except Exception:
            cur.close()
            return []

    db = get_db()
    if not db:
        return []
    cur = db.cursor()
    total = hts_count(dest)
    if total <= n:
        cur.execute("""SELECT htsno AS code, description AS desc
                       FROM hts_codes_rev4
                       WHERE digit_count IN (8, 10)
                       ORDER BY htsno""")
    else:
        step = max(1, total // n)
        cur.execute("""SELECT code, desc FROM (
                           SELECT htsno AS code, description AS desc,
                                  ROW_NUMBER() OVER (ORDER BY htsno) AS rn
                           FROM hts_codes_rev4 WHERE digit_count IN (8, 10)
                       ) sub WHERE rn %% %s = 0 LIMIT %s""", (step, n))
    rows = cur.fetchall()
    cur.close()
    return [dict(r) for r in rows]

def hts_count(dest="US"):
    """Count codes for a destination."""
    db = get_db()
    if not db:
        return 0
    cur = db.cursor()
    if dest == "US":
        cur.execute("SELECT COUNT(*) AS cnt FROM hts_codes_rev4 WHERE digit_count IN (8, 10)")
    else:
        try:
            cur.execute("SELECT COUNT(*) AS cnt FROM hts_codes WHERE dest=%s", (dest,))
        except Exception:
            cur.close()
            return 0
    row = cur.fetchone()
    cur.close()
    return row["cnt"] if row else 0

FTA_RATES = {
    "usmca":     {"name": "USMCA",           "countries": ["MX","CA"], "override": True},
    "korus":     {"name": "KORUS",            "countries": ["KR"],      "rate_modifier": 0.5},
    "singapore": {"name": "US-Singapore FTA", "countries": ["SG"],      "override": True},
}
# ── Routes ─────────────────────────────────────────────────
@app.route("/")
def landing():
    try:
        user = get_current_user()
        return render_template("landing.html", user=user)
    except Exception as e:
        logger.error(f"Landing route error: {e}", exc_info=True)
        return f"Error: {e}", 500
@app.route("/calculator")
def calculator():
    try:
        user = get_current_user()
        embed = request.args.get("embed", "")
        return render_template("index.html", user=user, embed=embed)
    except Exception as e:
        logger.error(f"Calculator route error: {e}", exc_info=True)
        return f"Error: {e}", 500
@app.route("/refund-guide.html")
def refund_guide():
    user = get_current_user()
    return render_template("refund-guide.html", user=user)
@app.route("/terms")
def terms():
    user = get_current_user()
    return render_template("terms.html", user=user)
@app.route("/privacy")
def privacy():
    user = get_current_user()
    return render_template("privacy.html", user=user)
@app.route("/contact", methods=["GET", "POST"])
def contact():
    user = get_current_user()
    if request.method == "POST":
        return render_template("contact.html", user=user, submitted=True)
    return render_template("contact.html", user=user, submitted=False)
# ── Auth Routes ───────────────────────────────────────────
@app.route("/signup", methods=["GET", "POST"])
def signup():
    if session.get("user_id"):
        return redirect(url_for("dashboard"))
    if request.method == "GET":
        return render_template("signup.html")
    data = request.form
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    company = (data.get("company_name") or "").strip()
    if not email or not password:
        return render_template("signup.html", error="Email and password are required.")
    if len(password) < 8:
        return render_template("signup.html", error="Password must be at least 8 characters.", email=email, company=company)
    db = get_db()
    if not db:
        return render_template("signup.html", error="Database temporarily unavailable. Please try again.", email=email, company=company)
    cur = db.cursor()
    cur.execute("SELECT id FROM users WHERE email = %s", (email,))
    existing = cur.fetchone()
    if existing:
        cur.close()
        return render_template("signup.html", error="An account with this email already exists.", email=email, company=company)
    pw_hash = hash_password(password)
    cur.execute("INSERT INTO users (email, password_hash, company_name) VALUES (%s, %s, %s) RETURNING id", (email, pw_hash, company))
    new_id = cur.fetchone()["id"]
    db.commit()
    cur.close()
    session["user_id"] = new_id
    session.permanent = True
    return redirect(url_for("dashboard"))
@app.route("/signin", methods=["GET", "POST"])
def signin():
    if session.get("user_id"):
        return redirect(url_for("dashboard"))
    if request.method == "GET":
        return render_template("signin.html")
    data = request.form
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    if not email or not password:
        return render_template("signin.html", error="Email and password are required.")
    db = get_db()
    if not db:
        return render_template("signin.html", error="Database temporarily unavailable. Please try again.", email=email)
    cur = db.cursor()
    cur.execute("SELECT * FROM users WHERE email = %s", (email,))
    user = cur.fetchone()
    cur.close()
    if not user or not verify_password(password, user["password_hash"]):
        return render_template("signin.html", error="Invalid email or password.", email=email)
    session["user_id"] = user["id"]
    session.permanent = True
    next_url = request.args.get("next") or url_for("dashboard")
    return redirect(next_url)
@app.route("/signout")
def signout():
    session.clear()
    return redirect(url_for("landing"))
# ── Dashboard ─────────────────────────────────────────────
@app.route("/dashboard")
@login_required
def dashboard():
    try:
        user = get_current_user()
        db = get_db()
        if not db or not user:
            return redirect(url_for("landing"))
        cur = db.cursor()
        cur.execute("SELECT * FROM calculations WHERE user_id = %s ORDER BY created_at DESC LIMIT 50", (user["id"],))
        calcs = cur.fetchall()
        cur.execute("SELECT * FROM refund_entries WHERE user_id = %s ORDER BY created_at DESC", (user["id"],))
        refunds = cur.fetchall()
        cur.execute("SELECT * FROM scan_results WHERE user_id = %s ORDER BY created_at DESC", (user["id"],))
        scans = cur.fetchall()
        cur.close()
        total_refund = sum(float(r.get("ieepa_amount", 0) or 0) for r in refunds)
        pending_count = sum(1 for r in refunds if r.get("filing_status") == "pending")
        return render_template("dashboard.html", user=user, calcs=calcs, refunds=refunds, scans=scans,
                               total_refund=total_refund, pending_count=pending_count)
    except Exception as e:
        logger.error(f"Dashboard error: {e}", exc_info=True)
        return f"Dashboard error: {e}", 500
# ── API: Save Calculation ─────────────────────────────────
@app.route("/api/save-calculation", methods=["POST"])
def save_calculation():
    if not session.get("user_id"):
        return jsonify({"error": "login_required", "signin_url": "/signin"}), 401
    data = request.json or {}
    db = get_db()
    cur = db.cursor()
    cur.execute("""INSERT INTO calculations
        (user_id, hts_code, hts_desc, origin, destination, product_value,
         currency, duty_rate, effective_rate, duty_amount, total_landed, result_json)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (session["user_id"], data.get("hts_code",""), data.get("hts_desc",""),
         data.get("origin",""), data.get("destination","US"),
         data.get("product_value",0), data.get("currency","USD"),
         data.get("duty_rate",0), data.get("effective_rate",0),
         data.get("duty_amount",0), data.get("total_landed",0),
         json.dumps(data.get("full_result",{}))))
    db.commit()
    cur.close()
    return jsonify({"ok": True})
# ── API: Save Refund Entry ────────────────────────────────
@app.route("/api/save-refund", methods=["POST"])
def save_refund():
    if not session.get("user_id"):
        return jsonify({"error": "login_required", "signin_url": "/signin"}), 401
    data = request.json or {}
    db = get_db()
    cur = db.cursor()
    cur.execute("""INSERT INTO refund_entries
        (user_id, origin, origin_label, product_value, entry_period, entry_status,
         ieepa_rate, ieepa_amount, surviving_amount, est_liquidation, protest_deadline)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (session["user_id"], data.get("origin",""), data.get("origin_label",""),
         data.get("product_value",0), data.get("entry_period",""),
         data.get("entry_status",""), data.get("ieepa_rate",0),
         data.get("ieepa_amount",0), data.get("surviving_amount",0),
         data.get("est_liquidation",""), data.get("protest_deadline","")))
    db.commit()
    cur.close()
    return jsonify({"ok": True})
# ── API: Update Refund Status ─────────────────────────────
@app.route("/api/update-refund/<int:entry_id>", methods=["POST"])
@login_required
def update_refund(entry_id):
    data = request.json or {}
    db = get_db()
    cur = db.cursor()
    cur.execute("UPDATE refund_entries SET filing_status = %s, notes = %s WHERE id = %s AND user_id = %s",
        (data.get("filing_status","pending"), data.get("notes",""), entry_id, session["user_id"]))
    db.commit()
    cur.close()
    return jsonify({"ok": True})
# ── API: Delete Entries ───────────────────────────────────
@app.route("/api/delete-calculation/<int:calc_id>", methods=["POST"])
@login_required
def delete_calculation(calc_id):
    db = get_db()
    cur = db.cursor()
    cur.execute("DELETE FROM calculations WHERE id = %s AND user_id = %s", (calc_id, session["user_id"]))
    db.commit()
    cur.close()
    return jsonify({"ok": True})
@app.route("/api/delete-refund/<int:entry_id>", methods=["POST"])
@login_required
def delete_refund(entry_id):
    db = get_db()
    cur = db.cursor()
    cur.execute("DELETE FROM refund_entries WHERE id = %s AND user_id = %s", (entry_id, session["user_id"]))
    db.commit()
    cur.close()
    return jsonify({"ok": True})
# ── API: Update Profile ──────────────────────────────────
@app.route("/api/update-profile", methods=["POST"])
@login_required
def update_profile():
    data = request.json or {}
    db = get_db()
    cur = db.cursor()
    cur.execute("""UPDATE users SET company_name=%s, broker_name=%s, broker_email=%s, ace_id=%s, phone=%s
        WHERE id=%s""",
        (data.get("company_name",""), data.get("broker_name",""),
         data.get("broker_email",""), data.get("ace_id",""),
         data.get("phone",""), session["user_id"]))
    db.commit()
    cur.close()
    return jsonify({"ok": True})
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
        # Sample hint codes from DB
        hints = hts_sample("US", 200)
        hts_list = "\n".join([f"{h['code']}: {h['desc']}" for h in hints])
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
        entry = hts_lookup(hts_code, "US") if hts_code else None
        if entry:
            result["db_match"]    = True
            result["base_rate"]   = float(entry["rate"])
            result["cn_301_rate"] = float(entry["cn_301"])
        else:
            result["db_match"] = False
            result.setdefault("base_rate", 0)
            result.setdefault("cn_301_rate", 0)
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
        entry = hts_lookup(hts_code, dest_country)
        base_rate = float(entry["rate"]) if entry else config.get("default_rate", 12.0)
        cn_301    = float(entry["cn_301"]) if entry else 0.0
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
        # Section 301 (US from CN only)
        china_surcharge = 0.0
        if dest_country == "US" and origin_country == "CN" and not fta_applied and cn_301 > 0:
            china_surcharge = cn_301
        # Section 232 (US steel/aluminum ch 72,73,76 + auto parts ch 87)
        section_232 = 0.0
        chapter = entry.get("chapter", 0) if entry else 0
        if not chapter and hts_code:
            chapter = _get_chapter(hts_code)
        if dest_country == "US" and not fta_applied and chapter in (72, 73, 76, 87):
            section_232 = 25.0
        # Section 122 temporary surcharge (10%, effective Feb 24 2026, 150 days)
        # S.232 goods are exempt from S.122
        section_122 = 0.0
        if dest_country == "US" and not fta_applied and section_232 == 0:
            section_122 = 10.0
        effective_duty_rate = base_rate + china_surcharge + section_232 + section_122
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
            "section_232":         round(section_232, 2),
            "section_122":         round(section_122, 2),
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
            "hts_desc":            entry.get("desc", "") if entry else "",
            "db_match":            bool(entry),
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
    results = hts_search_db(query, dest, 8)
    return jsonify(results)
@app.route("/api/dest-config", methods=["GET"])
def dest_config_api():
    dest   = request.args.get("dest", "US").upper()
    config = DEST_CONFIG.get(dest, DEST_CONFIG["US"])
    count  = hts_count(dest)
    return jsonify({
        "dest": dest, "name": config["name"],
        "valuation": config["valuation"],
        "de_minimis": config.get("de_minimis", 0),
        "de_minimis_currency": config.get("de_minimis_currency", "USD"),
        "vat_rate": config.get("vat_rate", 0),
        "vat_name": config.get("vat_name"),
        "mpf": config.get("mpf", False),
        "db_loaded": count > 0,
        "db_codes": count,
        "notes": config.get("notes", ""),
    })
@app.route("/health")
def health():
    us_count = hts_count("US")
    return jsonify({"status": "ok", "us_codes": us_count, "db_connected": us_count > 0})
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
