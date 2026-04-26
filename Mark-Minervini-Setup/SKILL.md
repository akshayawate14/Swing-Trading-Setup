## Minervini NSE Swing Trading Screener

A Python-based implementation of Mark Minervini's SEPA (Specific Entry Point Analysis) methodology for screening NSE (National Stock Exchange of India) stocks.

## Overview

This screener identifies stocks in Stage 2 uptrends with valid Volatility Contraction Patterns (VCP) and actionable entry points using Zerodha KiteConnect API for data retrieval.

## Pipeline

```
1. Load watchlist from CSV/Excel, Chartink, or Screener.in
2. Fetch 6 months daily OHLCV via Zerodha KiteConnect
3. VCP Detection — run on BOTH 6M and 3M windows (stock passes if EITHER yields valid VCP)
4. Entry/Breakout filter  (price in buy zone + volume surge)
5. Print ranked table + save dated CSV
```

---

## Core Components

### 1. Data Fetcher (`NSEDataFetcher`)

Fetches daily OHLCV data from Zerodha KiteConnect historical data API.

**Key Features:**
- Instrument token caching for fast lookups
- Configurable request delay to avoid rate limits
- **Retry logic with exponential backoff** for rate-limited requests

```python
# Configuration (config.json)
"request_delay_sec":  0.35,    # delay between API calls
"max_retries":        3,        # retry count for rate limits
"retry_base_delay":   2.0,      # initial delay between retries
```

**Rate Limit Handling:**
- Detects keywords: `"429"`, `"too many"`, `"rate limit"`, `"throttl"`
- Exponential backoff: 2s → 4s → 8s between retries
- Non-retryable errors fail immediately

---

### 2. Watchlist Loader (`MasterScreener`)

Loads symbols from one of three supported sources:

1. Local CSV/Excel file via `csv_path`
2. Chartink screener URL
3. Screener.in screen URL

**Configuration (config.json):**
```json
"csv_path": "Backtest Mark Minervini - Drilldown.csv",
"get_watchlist_from_url": true,
"watchlist_source": "auto",
"watchlist_url": "https://chartink.com/screener/mark-minervini-10466",
"chartink_page_size": 500,
"screener_include_numeric_codes": false,
"screener_page_delay_sec": 1.0,
"screener_max_retries": 3,
"screener_retry_base_delay": 2.0
```

**Source Selection:**
- `get_watchlist_from_url: false` loads symbols from `csv_path`
- `get_watchlist_from_url: true` loads symbols from `watchlist_url`
- `watchlist_source: "auto"` detects the source from the URL domain
- `watchlist_source: "chartink"` forces Chartink parsing
- `watchlist_source: "screener"` forces Screener.in parsing

**Chartink Handling:**
- Opens the screener page with cookie support
- Extracts CSRF token and scanner query from the page
- Calls Chartink's backend screener endpoint
- Fetches all available rows using `chartink_page_size`
- Extracts symbols from `nsecode`, then common fallback column names

**Screener.in Handling:**
- Parses company links like `/company/MCX/`
- Extracts NSE-style symbols from the URL path
- Skips numeric-only BSE codes by default
- Follows only real pagination links inside the pagination block
- Applies request delay and retry/backoff for HTTP 429 rate limits
- If a later page is rate-limited after symbols were already fetched, logs a warning and continues with collected symbols instead of terminating

**Important:** The Minervini Trend Template is not calculated internally. The upstream watchlist source should already represent the desired Trend Template or screening logic.

---

### 3. VCP Detector (`VCPDetector`)

Detects Volatility Contraction Patterns — the signature setup in Minervini's methodology.

**Configuration (config.json):**
```json
"vcp_min_contractions":     2,      // minimum contractions required
"vcp_max_first_depth":      0.40,   // first contraction max depth (40%)
"vcp_contraction_ratio":    1.10,   // each contraction should be shallower (10% tolerance)
"vcp_volume_decline_ratio": 0.90,   // second-half volume < 90% of first-half
"vcp_lookback_candles":     60      // candles to look back for base high
```

**VCP Rules:**
1. Minimum 2 contractions (ideally 3-4)
2. Each contraction depth < previous × contraction_ratio
3. Volume in second half of base < first half × volume_decline_ratio
4. First contraction depth ≤ max_first_depth

**VCP Structure:**
```
Price
  |      T1
  |     /\
  |    /  \   T2
  |   /    \ /\   T3
  |  /      X  \ /\    Pivot
  | /       |   X  \  /----→ BREAKOUT
  |/        |   |   \/
  |         |   |    
  Base      C1  C2   C3 (contractions tighten)

T = Thrust (price expansion)
C = Contraction (price tightening)
```

---

### 4. Entry Filter (`EntryFilter`)

Final gate — checks whether the stock is actionable right now.

**Configuration (config.json):**
```json
"entry_buy_zone_max_pct": 0.05,   // 5% above pivot
"entry_min_vol_ratio":    1.50    // 1.5x 50-day average volume
```

**Entry Rules:**
- **Buy Zone**: 0% to 5% above pivot
- **Breakout Confirmed**: price in buy zone AND volume ≥ 1.5x 50-day avg
- **Extended**: >5% above pivot (too late, skip)
- **Pre-breakout**: price below pivot
- Buy/sell decisions use raw unrounded price and volume values; rounded values are only for display. This prevents false positives around values like `-0.0%` from pivot.
- In-buy-zone stocks with insufficient volume are labelled `WATCH (low vol)` and include a reason such as `Volume 0.98x below required 1.50x`.

---

### 5. Stock Screener (`StockScreener`)

Full pipeline for one symbol:
```
fetch → VCP (6M + 3M) → entry filter → ScreenResult
```

**Note:** Trend Template check is intentionally skipped. Watchlist is assumed to already pass all 8 Trend Template criteria.

---

### 6. Master Screener (`MasterScreener`)

Concurrent orchestrator using ThreadPoolExecutor.

**Configuration (config.json):**
```json
"max_threads": 15    // concurrent threads for scanning
```

---

## Data Models

### `Contraction`
```python
@dataclass
class Contraction:
    high: float       # swing high price
    low: float        # swing low price
    depth_pct: float  # contraction depth as percentage
    high_date: str    # date of high
    low_date: str     # date of low
```

### `VCPResult`
```python
@dataclass
class VCPResult:
    valid: bool
    window: str                    # "6M" | "3M"
    contractions: list[Contraction]
    pivot: float                   # breakout point
    tightness_pct: float           # last contraction depth %
    base_length_days: int
    volume_dryup_pct: float
    reason: str                    # failure reason if invalid
```

### `EntryResult`
```python
@dataclass
class EntryResult:
    in_buy_zone: bool
    extended: bool
    pct_from_pivot: float
    volume_ratio: float            # latest vol / 50d avg vol
    breakout_confirmed: bool
    reason: str
```

### `ScreenResult`
```python
@dataclass
class ScreenResult:
    symbol: str
    vcp_window: str                # "6M" | "3M" | "BOTH"
    contractions: int
    pivot: float
    current_price: float
    pct_from_pivot: float
    volume_ratio: float
    tightness_pct: float
    base_length_days: int
    volume_dryup_pct: float
    action: str                   # "BUY ZONE" | "WATCH" | "EXTENDED" | "PRE-BREAKOUT"
    stop_loss: float
    stop_pct: float
    stop_type: str                # "STRUCTURE" | "FIXED_MAX"
    target_2r: float
    target_3r: float
    skip_reason: str
```

---

## Configuration

All parameters are configurable via `config.json`:

```json
{
    "csv_path": "Backtest Mark Minervini - Drilldown.csv",
    "get_watchlist_from_url": true,
    "watchlist_source": "auto",
    "watchlist_url": "https://chartink.com/screener/mark-minervini-10466",
    "chartink_page_size": 500,
    "screener_include_numeric_codes": false,
    "screener_page_delay_sec": 1.0,
    "screener_max_retries": 3,
    "screener_retry_base_delay": 2.0,
    "max_threads": 15,
    "request_delay_sec": 0.35,
    "max_retries": 3,
    "retry_base_delay": 2.0,
    "vcp_min_contractions": 2,
    "vcp_max_first_depth": 0.40,
    "vcp_contraction_ratio": 1.10,
    "vcp_volume_decline_ratio": 0.90,
    "vcp_lookback_candles": 60,
    "entry_buy_zone_max_pct": 0.05,
    "entry_min_vol_ratio": 1.50,
    "entry_max_stop_pct": 0.08,
    "entry_min_avg_volume": 40000
}
```

---

## Usage

```bash
python minervini_screener.py
```

**Switching Watchlist Sources:**

Use Chartink:
```json
"get_watchlist_from_url": true,
"watchlist_source": "auto",
"watchlist_url": "https://chartink.com/screener/mark-minervini-10466"
```

Use Screener.in:
```json
"get_watchlist_from_url": true,
"watchlist_source": "auto",
"watchlist_url": "https://www.screener.in/screens/3364489/mark-minervini-setup/"
```

Use local file:
```json
"get_watchlist_from_url": false,
"csv_path": "Backtest Mark Minervini - Drilldown.csv"
```

**Output:**
- Console: Ranked table with all passing stocks
- CSV: `minervini_results_YYYY-MM-DD.csv`
- Logs: `logs/minervini_YYYY-MM-DD.log`

---

## Action Labels

| Action | Description |
|--------|-------------|
| `BUY ZONE` | In buy zone (0-5% above pivot) with volume surge |
| `WATCH (low vol)` | In buy zone but volume below threshold; reason includes the actual volume ratio |
| `PRE-BREAKOUT` | Price below pivot |
| `EXTENDED` | >5% above pivot (too late to enter) |

---

## Dependencies

```
pip install pandas tabulate openpyxl jugaad-trader
```

- **pandas**: Data manipulation
- **tabulate**: Formatted table output
- **openpyxl**: Excel file support
- **jugaad-trader**: Zerodha KiteConnect wrapper

URL watchlist loading uses Python standard library modules (`urllib`, `http.cookiejar`, `re`) and does not require `requests`.

---

## Minervini's Core Philosophy

> *"The goal is not to buy low and sell high. It's to buy high and sell higher."*

> *"Risk management is not about avoiding losses—it's about keeping losses small so you can stay in the game."*

### Design Principles

1. **Trend First**: Only buy stocks in confirmed Stage 2 uptrend
2. **Specific Entry Points**: Enter at low-risk pivot points
3. **Volatility Contraction**: Tightening price action precedes explosive moves
4. **Cut Losses Quickly**: 7-8% maximum loss
5. **Let Winners Run**: Sell strength, not weakness

---

## Files

```
markminervini/
├── minervini_screener.py   # Main screener
├── config.json             # Configuration
├── Backtest Mark Minervini - Drilldown.csv  # Optional local watchlist
├── logs/                   # Log files
└── minervini_results_*.csv # Output results
```
