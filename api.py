from flask import Flask, Response, jsonify
from flask_cors import CORS
import pandas as pd
import subprocess
import os
import threading
import time
import sys

app = Flask(__name__)
CORS(app)

# Define columns expected in CSVs
OPERATIONAL_COLUMNS = [
    "Company", 
    "Ticker",
    "Capacity (MTPA)",
    "Yearly Capacity Utilisation (%)",
    "Realisation/Tonne (₹)",
    "Cost/Tonne (₹)",
    "EBIDTA/Tonne (₹)",
    "Power & Fuel Cost",
    "Logistics Cost",
    "Raw Material Cost",
    "Other Costs"
]

FINANCIAL_COLUMNS = [
    "Company", "Ticker",
    "Market Cap",
    "Current Price",
    "Stock P/E",
    "Book Value",
    "ROCE",
    "Dividend Yield"
]

# Paths
base_dir = os.path.dirname(os.path.abspath(__file__))
scraper_path = os.path.join(base_dir, 'scraper.py')
operational_csv = os.path.join(base_dir, 'cement_companies_summary.csv')
financial_csv = os.path.join(base_dir, 'cement_companies_summary2.csv')


def clean_numeric(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str)
                    .str.replace(",", "", regex=False)
                    .str.replace("₹", "", regex=False)
                    .str.replace("%", "", regex=False)
                    .str.strip(),
                errors="coerce"
            ).fillna(0)
    return df


# Function to run scraper
def run_scraper():
    try:
        print("[Scraper] Running scraper.py...")
        result = subprocess.run([sys.executable, scraper_path], check=True)
        print("[Scraper] Scraper ran successfully.")
    except subprocess.CalledProcessError as e:
        print("[Scraper] ERROR during execution:", str(e))


# Background thread to rerun scraper every 30 minutes
def schedule_scraper():
    while True:
        run_scraper()
        time.sleep(1800)  # 1800 seconds = 30 minutes


@app.route('/api/operational', methods=['GET'])
def get_operational():
    try:
        df = pd.read_csv(operational_csv)
        numeric_cols = [col for col in OPERATIONAL_COLUMNS if col not in ["Company", "Ticker"]]
        df = clean_numeric(df, numeric_cols)
        df = df.reindex(columns=OPERATIONAL_COLUMNS, fill_value=None)
        print("[/api/operational] Data loaded successfully.")
        return Response(df.to_json(orient='records'), mimetype='application/json')
    except Exception as e:
        print("[/api/operational] ERROR:", str(e))
        return Response("[]", mimetype='application/json')


@app.route('/api/financial', methods=['GET'])
def get_financial():
    try:
        df = pd.read_csv(financial_csv)
        numeric_cols = [col for col in FINANCIAL_COLUMNS if col not in ["Company", "Ticker"]]
        df = clean_numeric(df, numeric_cols)
        df = df.reindex(columns=FINANCIAL_COLUMNS, fill_value=None)
        print("[/api/financial] Data loaded successfully.")
        return Response(df.to_json(orient='records'), mimetype='application/json')
    except Exception as e:
        print("[/api/financial] ERROR:", str(e))
        return Response("[]", mimetype='application/json')


@app.route('/api/status', methods=['GET'])
def status():
    return jsonify({"status": "ok"}), 200


# Start Flask app + background scraper thread
if __name__ == '__main__':
    # Run once at start
    threading.Thread(target=run_scraper, daemon=True).start()
    # Start timed recurring execution
    threading.Thread(target=schedule_scraper, daemon=True).start()

    # Start Flask server
    app.run(port=5050)
