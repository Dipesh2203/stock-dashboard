# Stock Data Intelligence Dashboard

A mini financial data platform built for the internship assignment. It collects market data, cleans and transforms it with Pandas, stores it in SQLite, exposes REST APIs using FastAPI, and visualizes insights in a dashboard.

## Features Completed

- Data collection from yfinance for major NSE symbols
- Data cleaning and transformation with Pandas
- Required metrics:
  - Daily Return = (Close - Open) / Open
  - 7-day moving average
  - 52-week rolling high/low
- Custom metric:
  - 14-day annualized volatility score
- REST APIs:
  - `GET /companies`
  - `GET /data/{symbol}` (defaults to last 30 days, configurable with `days`)
  - `GET /summary/{symbol}`
  - `GET /compare?symbol1=INFY&symbol2=TCS` (bonus)
- Bonus endpoint:
  - `GET /top-movers`
- Dashboard UI with:
  - Company list sidebar
  - Price chart + 7-day MA overlay
  - Compare two stocks
  - Top gainers/losers panel
- Swagger docs available at `/docs`

## Project Structure

```text
app/
  main.py            # FastAPI app and API routes
  db.py              # SQLite schema and query helpers
  data_pipeline.py   # Download, clean, transform logic
  schemas.py         # Pydantic response schemas
  static/
    index.html       # Dashboard frontend (Chart.js)
scripts/
  update_data.py     # Manual refresh script
requirements.txt
README.md
```

## Setup Instructions

1. Create virtual environment:

```bash
python -m venv .venv
```

2. Activate it:

```bash
# Windows PowerShell
.venv\Scripts\Activate.ps1
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Run the app:

```bash
uvicorn app.main:app --reload
```

5. Open in browser:

- API docs: `http://127.0.0.1:8000/docs`
- Dashboard: `http://127.0.0.1:8000/dashboard`

## Data Notes

- On startup, the app attempts to fetch real stock data from yfinance.
- If network/API fails, it auto-generates realistic mock data so APIs and UI still work.
- Use the manual refresh endpoint if needed:

```bash
curl -X POST http://127.0.0.1:8000/refresh
```

or run:

```bash
python scripts/update_data.py
```

## API Samples

### 1) List companies

```bash
curl http://127.0.0.1:8000/companies
```

### 2) Last 30 days data

```bash
curl http://127.0.0.1:8000/data/INFY
```

### 3) Summary stats

```bash
curl http://127.0.0.1:8000/summary/TCS
```

### 4) Compare two stocks

```bash
curl "http://127.0.0.1:8000/compare?symbol1=INFY&symbol2=TCS"
```

## Creativity and Analysis Highlights

- Added volatility score as risk indicator.
- Added stock comparison endpoint with:
  - 30-day return for both symbols
  - Correlation of closing prices
  - Winner determination
- Added top movers panel for quick insight scanning.

## Optional Next Enhancements

- Add PostgreSQL and SQLAlchemy migration
- Add authentication and rate limiting
- Add Dockerfile and deployment on Render
- Add lightweight forecasting line using linear regression or ARIMA

## Submission Checklist

- [x] Python backend with REST APIs
- [x] Data cleaning and derived metrics
- [x] Dashboard visualization
- [x] Documentation with setup and usage
- [x] Bonus comparison and extra insights
