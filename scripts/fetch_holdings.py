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
}

SHEET_NAME = "Daily Fund Holdings"
HEADER_ROW = 2
BASE_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def download_file(url):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
        "Referer": "https://www.capitalgroup.com/",
    }
    resp = requests.get(url, headers=headers, allow_redirects=True, timeout=30)
    resp.raise_for_status()
    return resp.content


def find_column(columns, *keywords):
    for col in columns:
        col_lower = str(col).lower()
        if any(kw.lower() in col_lower for kw in keywords):
            return col
    return None


def parse_holdings(content, ticker):
    df = pd.read_excel(BytesIO(content), sheet_name=SHEET_NAME, header=HEADER_ROW)
    cols = df.columns.tolist()
    print("  [{}] Columns: {}".format(ticker, cols), file=sys.stderr)

    ticker_col = find_column(cols, "ticker", "symbol")
    name_col   = find_column(cols, "name", "description", "security", "holding", "issuer")
    pct_col    = find_column(cols, "net assets", "% of", "weight", "percent")
    shares_col = find_column(cols, "shares", "principal")

    missing = [n for n, c in [("ticker", ticker_col), ("pct_net_assets", pct_col), ("shares", shares_col)] if c is None]
    if missing:
        raise ValueError("[{}] Could not locate columns: {}. Available: {}".format(ticker, missing, cols))

    keep = [ticker_col, pct_col, shares_col]
    if name_col:
        keep = [ticker_col, name_col, pct_col, shares_col]

    df = df[keep].copy()

    if name_col:
        df.columns = ["ticker", "name", "pct_net_assets", "shares"]
    else:
        df.columns = ["ticker", "pct_net_assets", "shares"]
        df["name"] = ""

    df["ticker"] = df["ticker"].astype(str).str.strip()
    df = df[df["ticker"].notna() & (df["ticker"] != "") & (df["ticker"] != "nan")]

    def safe_float(x):
        try:
            return round(float(x), 6)
        except (TypeError, ValueError):
            return None

    records = []
    for _, row in df.iterrows():
        records.append({
            "ticker":         row["ticker"],
            "name":           str(row["name"]).strip() if row["name"] else "",
            "pct_net_assets": safe_float(row["pct_net_assets"]),
            "shares":         safe_float(row["shares"]),
        })

    return records


def get_data_dir(ticker):
    d = os.path.join(BASE_DATA_DIR, ticker)
    os.makedirs(d, exist_ok=True)
    return d


def save_snapshot(records, today_str, ticker):
    data_dir = get_data_dir(ticker)
    path = os.path.join(data_dir, "{}.json".format(today_str))
    payload = {"date": today_str, "ticker": ticker, "holdings": records}
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)
    with open(os.path.join(data_dir, "latest.json"), "w") as f:
        json.dump(payload, f, indent=2)
    print("  [{}] Saved {} holdings".format(ticker, len(records)), file=sys.stderr)


def find_prior_snapshot(today_str, ticker):
    data_dir = get_data_dir(ticker)
    files = sorted(
        f for f in os.listdir(data_dir)
        if f.endswith(".json") and f not in ("latest.json", "diff.json", "history.json")
    )
    prior = [f for f in files if f.replace(".json", "") < today_str]
    return os.path.join(data_dir, prior[-1]) if prior else None


def compute_diff(today_records, prior_records, today_str, prior_date_str, ticker):
    today_map   = {r["ticker"]: r for r in today_records}
    prior_map   = {r["ticker"]: r for r in prior_records}
    all_tickers = sorted(set(today_map) | set(prior_map))

    rows = []
    for t in all_tickers:
        td = today_map.get(t)
        pr = prior_map.get(t)

        if td and pr:
            s_today   = td["shares"] or 0
            s_prior   = pr["shares"] or 0
            pct_today = td["pct_net_assets"] or 0
            pct_prior = pr["pct_net_assets"] or 0
            shares_chg = ((s_today - s_prior) / s_prior * 100) if s_prior != 0 else 0
            pct_chg    = round(pct_today - pct_prior, 4)
            rows.append({
                "ticker":                t,
                "name":                  td.get("name") or pr.get("name") or "",
                "status":                "changed" if shares_chg != 0 else "unchanged",
                "shares_today":          s_today,
                "shares_prior":          s_prior,
                "shares_pct_change":     round(shares_chg, 4),
                "pct_net_assets_today":  pct_today,
                "pct_net_assets_prior":  pct_prior,
                "pct_net_assets_change": pct_chg,
            })
        elif td:
            rows.append({
                "ticker":                t,
                "name":                  td.get("name") or "",
                "status":                "added",
                "shares_today":          td["shares"] or 0,
                "shares_prior":          None,
                "shares_pct_change":     None,
                "pct_net_assets_today":  td["pct_net_assets"] or 0,
                "pct_net_assets_prior":  None,
                "pct_net_assets_change": None,
            })
        else:
            rows.append({
                "ticker":                t,
                "name":                  pr.get("name") or "",
                "status":                "removed",
                "shares_today":          None,
                "shares_prior":          pr["shares"] or 0,
                "shares_pct_change":     None,
                "pct_net_assets_today":  None,
                "pct_net_assets_prior":  pr["pct_net_assets"] or 0,
                "pct_net_assets_change": None,
            })

    return {"date": today_str, "ticker": ticker, "prior_date": prior_date_str, "diff": rows}


def append_history(today_str, diff, ticker):
    data_dir = get_data_dir(ticker)
    history_path = os.path.join(data_dir, "history.json")
    if os.path.exists(history_path):
        with open(history_path) as f:
            history = json.load(f)
    else:
        history = []
    entry = {"date": today_str, "prior_date": diff["prior_date"]}
    if entry not in history:
        history.append(entry)
        history.sort(key=lambda x: x["date"], reverse=True)
    with open(history_path, "w") as f:
        json.dump(history, f, indent=2)


def process_etf(ticker, url, today_str):
    print("Processing {}...".format(ticker), file=sys.stderr)
    try:
        content = download_file(url)
        records = parse_holdings(content, ticker)
        save_snapshot(records, today_str, ticker)

        prior_path = find_prior_snapshot(today_str, ticker)
        if not prior_path:
            print("  [{}] No prior snapshot -- skipping diff.".format(ticker), file=sys.stderr)
            diff = {"date": today_str, "ticker": ticker, "prior_date": None, "diff": []}
        else:
            with open(prior_path) as f:
                prior_data = json.load(f)
            prior_date_str = prior_data["date"]
            diff = compute_diff(records, prior_data["holdings"], today_str, prior_date_str, ticker)

        data_dir = get_data_dir(ticker)
        with open(os.path.join(data_dir, "diff.json"), "w") as f:
            json.dump(diff, f, indent=2)

        append_history(today_str, diff, ticker)

        changed = sum(1 for r in diff["diff"] if r["status"] == "changed")
        added   = sum(1 for r in diff["diff"] if r["status"] == "added")
        removed = sum(1 for r in diff["diff"] if r["status"] == "removed")
        print("  [{}] Done -- {} holdings | {} changed | {} added | {} removed".format(
            ticker, len(records), changed, added, removed), file=sys.stderr)
    except Exception as e:
        print("  [{}] ERROR: {}".format(ticker, e), file=sys.stderr)


def is_nyse_trading_day(d):
    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(start_date=d.isoformat(), end_date=d.isoformat())
    return not schedule.empty


def main():
    today_str = date.today().isoformat()
    today     = date.today()

    if not is_nyse_trading_day(today):
        print("{} is not a NYSE trading day -- skipping.".format(today_str), file=sys.stderr)
        sys.exit(0)

    print("Running for {}...".format(today_str), file=sys.stderr)
    for ticker, url in ETFS.items():
        process_etf(ticker, url, today_str)

    print("All done.", file=sys.stderr)


if __name__ == "__main__":
    main()
