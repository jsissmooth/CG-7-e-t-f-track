import requests
import pandas as pd
import pandas_market_calendars as mcal
import json
import os
import sys
from datetime import date
from io import BytesIO

ETFS = {
    "CGBL": "https://www.capitalgroup.com/api/investments/investment-service/v1/etfs/cgbl/download/daily-holdings?audience=advisor&redirect=true",
    "CGIE": "https://www.capitalgroup.com/api/investments/investment-service/v1/etfs/cgie/download/daily-holdings?audience=advisor&redirect=true",
    "CGUS": "https://www.capitalgroup.com/api/investments/investment-service/v1/etfs/cgus/download/daily-holdings?audience=advisor&redirect=true",
    "CGMM": "https://www.capitalgroup.com/api/investments/investment-service/v1/etfs/cgmm/download/daily-holdings?audience=advisor&redirect=true",
    "CGXU": "https://www.capitalgroup.com/api/investments/investment-service/v1/etfs/cgxu/download/daily-holdings?audience=advisor&redirect=true",
    "CGGO": "https://www.capitalgroup.com/api/investments/investment-service/v1/etfs/cggo/download/daily-holdings?audience=advisor&redirect=true",
    "CGNG": "https://www.capitalgroup.com/api/investments/investment-service/v1/etfs/cgng/download/daily-holdings?audience=advisor&redirect=true",
    "CGGR": "https://www.capitalgroup.com/api/investments/investment-service/v1/etfs/cggr/download/daily-holdings?audience=advisor&redirect=true",
    "CGDV": "https://www.capitalgroup.com/api/investments/investment-service/v1/etfs/cgdv/download/daily-holdings?audience=advisor&redirect=true",
    "CGCV": "https://www.capitalgroup.com/api/investments/investment-service/v1/etfs/cgcv/download/daily-holdings?audience=advisor&redirect=true",
    "CGDG": "https://www.capitalgroup.com/api/investments/investment-service/v1/etfs/cgdg/download/daily-holdings?audience=advisor&redirect=true",
    "CGHY": "https://www.capitalgroup.com/api/investments/investment-service/v1/etfs/cghy/download/daily-holdings?audience=advisor&redirect=true",
    "CGMS": "https://www.capitalgroup.com/api/investments/investment-service/v1/etfs/cgms/download/daily-holdings?audience=advisor&redirect=true",
    "CGCP": "https://www.capitalgroup.com/api/investments/investment-service/v1/etfs/cgcp/download/daily-holdings?audience=advisor&redirect=true",
    "CGHM": "https://www.capitalgroup.com/api/investments/investment-service/v1/etfs/cghm/download/daily-holdings?audience=advisor&redirect=true",
    "CGMU": "https://www.capitalgroup.com/api/investments/investment-service/v1/etfs/cgmu/download/daily-holdings?audience=advisor&redirect=true",
}

SHEET_NAME    = "Daily Fund Holdings"
HEADER_ROW    = 2
BASE_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def is_nyse_trading_day(d):
    nyse = mcal.get_calendar("NYSE")
    return not nyse.schedule(start_date=d.isoformat(), end_date=d.isoformat()).empty


def download_excel(url):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
        "Referer": "https://www.capitalgroup.com/",
    }
    resp = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
    resp.raise_for_status()
    return BytesIO(resp.content)


def parse_holdings(excel_bytes):
    df = pd.read_excel(excel_bytes, sheet_name=SHEET_NAME, header=HEADER_ROW, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]

    ticker_col = next((c for c in df.columns if "ticker" in c.lower()), None)
    pct_col    = next((c for c in df.columns if "net assets" in c.lower() or "% of" in c.lower()), None)
    shares_col = next((c for c in df.columns if "shares" in c.lower() or "principal" in c.lower()), None)
    name_col   = next((c for c in df.columns if "name" in c.lower() or "description" in c.lower()), None)

    records = []
    for _, row in df.iterrows():
        ticker = str(row[ticker_col]).strip() if ticker_col else ""
        if not ticker or ticker.lower() in ("nan", "ticker", ""):
            continue
        pct    = None
        shares = None
        name   = ""
        try:
            pct = round(float(row[pct_col]), 6) if pct_col else None
        except (ValueError, TypeError):
            pass
        try:
            shares = float(str(row[shares_col]).replace(",", "")) if shares_col else None
        except (ValueError, TypeError):
            pass
        if name_col:
            name = str(row[name_col]).strip()
            if name.lower() == "nan":
                name = ""
        records.append({
            "ticker":       ticker,
            "name":         name,
            "identifier":   ticker,
            "pct_of_fund":  pct,
            "quantity":     shares,
            "market_value": None,
            "sector":       "",
        })
    return records


def get_etf_data_dir(etf_ticker):
    d = os.path.join(BASE_DATA_DIR, etf_ticker)
    os.makedirs(d, exist_ok=True)
    return d


def save_snapshot(records, today_str, etf_ticker):
    data_dir = get_etf_data_dir(etf_ticker)
    payload = {"date": today_str, "ticker": etf_ticker, "holdings": records}
    with open(os.path.join(data_dir, "{}.json".format(today_str)), "w") as f:
        json.dump(payload, f, indent=2)
    with open(os.path.join(data_dir, "latest.json"), "w") as f:
        json.dump(payload, f, indent=2)


def find_prior_snapshot(today_str, etf_ticker):
    data_dir = get_etf_data_dir(etf_ticker)
    files = sorted(
        f for f in os.listdir(data_dir)
        if f.endswith(".json") and f not in ("latest.json", "diff.json", "history.json")
    )
    prior = [f for f in files if f.replace(".json", "") < today_str]
    return os.path.join(data_dir, prior[-1]) if prior else None


def compute_diff(today_records, prior_records, today_str, prior_date_str, etf_ticker):
    today_map = {r["ticker"]: r for r in today_records}
    prior_map = {r["ticker"]: r for r in prior_records}
    all_keys  = sorted(set(today_map) | set(prior_map))
    rows = []
    for key in all_keys:
        t = today_map.get(key)
        p = prior_map.get(key)
        if t and p:
            q_today   = t["quantity"] or 0
            q_prior   = p["quantity"] or 0
            pct_today = t["pct_of_fund"] or 0
            pct_prior = p["pct_of_fund"] or 0
            qty_chg   = ((q_today - q_prior) / q_prior * 100) if q_prior != 0 else 0
            rows.append({
                "ticker":              t["ticker"],
                "name":                t.get("name") or p.get("name") or "",
                "identifier":          t.get("identifier") or "",
                "sector":              t.get("sector") or "",
                "status":              "changed" if round(qty_chg, 6) != 0 else "unchanged",
                "quantity_today":      q_today,
                "quantity_prior":      q_prior,
                "quantity_pct_change": round(qty_chg, 4),
                "pct_of_fund_today":   pct_today,
                "pct_of_fund_prior":   pct_prior,
                "pct_of_fund_change":  round(pct_today - pct_prior, 4),
                "market_value_today":  t.get("market_value"),
            })
        elif t:
            rows.append({
