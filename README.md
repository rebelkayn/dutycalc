# DutyCalc — US Import Duty Calculator

AI-powered landed cost calculator for US importers. Upload a commercial invoice photo → get instant duty breakdown.

## Project Structure

```
dutycalc/
├── app.py                          # Flask backend + HTS database + API routes
├── templates/
│   └── index.html                  # Frontend UI (Apple-style)
├── data/
│   └── finalCopy_2026HTSRev4.pdf   # HTS schedule (you provide this)
├── requirements.txt
├── render.yaml                     # Render.com deploy config
├── Procfile
└── .gitignore
```

## Local Setup (Mac/Linux)

```bash
# 1. Install Python 3.11+ if needed
python3 --version

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate  # Mac/Linux
# OR: venv\Scripts\activate  (Windows)

# 3. Install dependencies
pip install -r requirements.txt

# 4. Add your Gemini API key (optional, enables AI extraction)
export GEMINI_API_KEY="your_key_here"

# 5. Run locally
python app.py

# Visit: http://localhost:5000
```

## Deploy to Render.com (Free)

```bash
# 1. Create GitHub repo and push code
git init
git add .
git commit -m "Initial DutyCalc MVP"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/dutycalc.git
git push -u origin main

# 2. Go to render.com → New → Web Service → Connect your GitHub repo
# 3. Add environment variable: GEMINI_API_KEY = your_key
# 4. Deploy — your site will be live at https://dutycalc.onrender.com
```

## Adding the HTS PDF

Place `finalCopy_2026HTSRev4.pdf` in the `data/` folder.
The app currently uses an embedded HTS database for the top ~120 SMB codes.
The PDF is available for future expansion of the database.

## Data Sources Used

- **HTS Codes & Rates**: USITC HTS 2026 Rev4 (hts.usitc.gov)
- **Section 301 China Tariffs**: USTR lists (ustr.gov)
- **FTA Rates**: US FTA Tariff Tool (ustr.gov/trade-agreements)
- **De Minimis**: $800 USD (CBP 19 U.S.C. § 1321)

## Accuracy Note

This tool covers ~120 high-frequency SMB HTS codes verified against USITC data.
For production use, integrate the Dutify or Zonos API for full 12,000-code coverage.
Always consult a licensed customs broker for binding classification rulings.
