# American Funds ETF Holdings Tracker

Tracks daily holdings changes for 7 Capital Group ETFs:
**CGBL, CGIE, CGUS, CGMM, CGXU, CGGO, CGNG**

## What it does
- Runs every weekday at 9:30 AM ET via GitHub Actions
- Downloads daily holdings Excel for each ETF
- Diffs each fund vs the prior trading day
- Publishes a tabbed dashboard via GitHub Pages
- Skips weekends and NYSE holidays automatically

## Dashboard
Each tab shows: Ticker, Name, Status, Shares (Prior/Today/Δ%), Weight (Prior/Today/Δpp)

## Setup
1. Create a public GitHub repo
2. Upload all files preserving folder structure
3. Settings → Pages → main branch / root
4. Settings → Actions → General → Read and write permissions
5. Actions → Daily Holdings Tracker → Run workflow

## File layout
```
index.html
scripts/fetch_holdings.py
.github/workflows/daily_holdings.yml
data/
  CGBL/
  CGIE/
  CGUS/
  CGMM/
  CGXU/
  CGGO/
  CGNG/
```
