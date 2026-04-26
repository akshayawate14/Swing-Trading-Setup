"""
Minervini NSE Swing Trading Screener
======================================
Mark Minervini SEPA methodology screener for NSE stocks.

Pipeline:
    1. Load watchlist from CSV/Excel  (watchlist already passed Trend Template)
    2. Fetch 6M daily OHLCV via Zerodha KiteConnect historical data API
    3. VCP Detection — run independently on BOTH 6M and 3M windows
       Stock passes if EITHER window yields a valid VCP
    4. Entry / Breakout filter  (price in buy zone + volume surge)
    5. Print ranked table  +  save dated CSV

Usage:
    python minervini_screener.py

Requirements:
    pip install kiteconnect pandas tabulate openpyxl

config.json must contain:
    {
        "csv_path":     "watchlist.csv",
        "api_key":      "<your-zerodha-api-key>",
        "access_token": "<your-zerodha-access-token>",

        // Optional: threading
        "max_threads": 15,

        // Optional: HTTP fetch behaviour
        "request_delay_sec":  0.35,
        "max_retries":        3,
        "retry_base_delay":   2.0,

        // Optional: VCP detection parameters
        "vcp_min_contractions":     2,
        "vcp_max_first_depth":      0.40,
        "vcp_contraction_ratio":    1.10,
        "vcp_volume_decline_ratio": 0.90,
        "vcp_lookback_candles":     60,

        // Optional: Entry / breakout filter parameters
        "entry_buy_zone_max_pct": 0.05,    // max % above pivot still in buy zone
        "entry_min_vol_ratio":    1.50,    // breakout vol >= 150% of 50d avg
        "entry_max_stop_pct":     0.08,    // max stop loss from pivot — Minervini hard rule
        "entry_min_avg_volume":   400000   // skip thinly traded stocks (50d avg vol)
    }

    Generate a fresh access_token each trading day via the KiteConnect
    login flow:  https://kite.trade/docs/connect/v3/user/#login-flow
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from jugaad_trader import Zerodha
from tabulate import tabulate

# Suppress only specific, known-harmless pandas future warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="pandas")

# ─────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────
_LOG_DIR = Path("logs")

def _setup_logging() -> logging.Logger:
    """
    Configure application logging with two handlers:
      • Console  — INFO and above, compact time-only format
      • File     — DEBUG and above, full timestamp + level + message
                   stored in logs/minervini_YYYY-MM-DD.log
                   Rotates at 5 MB, keeps 7 backups.

    Third-party loggers that produce irrelevant noise are silenced:
      • urllib3.connectionpool  — "Connection pool is full" warnings
        (fires when thread-count exceeds the default pool size of 10;
         harmless — requests are still served, old connections are
         simply discarded and new ones opened automatically)
      • requests / urllib3      — general HTTP noise
      • kiteconnect             — internal KiteConnect SDK chatter
    """
    _LOG_DIR.mkdir(exist_ok=True)

    lg = logging.getLogger("minervini")
    lg.setLevel(logging.DEBUG)          # root level: DEBUG — handlers decide what to emit
    lg.propagate = False                 # don't bubble up to the root logger

    # ── Console handler ───────────────────────────────────────────
    _console = logging.StreamHandler()
    _console.setLevel(logging.INFO)
    _console.setFormatter(logging.Formatter(
        fmt     = "%(asctime)s [%(levelname)s] %(message)s",
        datefmt = "%H:%M:%S",
    ))

    # ── Rotating file handler ─────────────────────────────────────
    _log_file = _LOG_DIR / f"minervini_{date.today()}.log"
    _file = logging.handlers.RotatingFileHandler(
        filename    = _log_file,
        maxBytes    = 5 * 1024 * 1024,   # 5 MB per file
        backupCount = 7,
        encoding    = "utf-8",
    )
    _file.setLevel(logging.DEBUG)
    _file.setFormatter(logging.Formatter(
        fmt     = "%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S",
    ))

    lg.addHandler(_console)
    lg.addHandler(_file)

    # ── Silence noisy third-party loggers ─────────────────────────
    # urllib3 emits WARNING-level "Connection pool is full" messages
    # when the thread count exceeds the default pool size (10).
    # These are benign — connections are recycled automatically.
    # Setting this to ERROR hides the chatter without masking real errors.
    for _noisy in (
        "urllib3.connectionpool",
        "urllib3",
        "requests",
        "kiteconnect",
    ):
        logging.getLogger(_noisy).setLevel(logging.ERROR)

    return lg


logger = _setup_logging()

# ─────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────
CONFIG_PATH = Path("config.json")


def load_config() -> dict:
    """
    Load config.json and log the active VCP / entry parameters so every
    run has a clear record of which thresholds were used.
    """
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            "config.json not found. Create it based on config.sample.json."
        )
    with open(CONFIG_PATH) as f:
        cfg = json.load(f)

    logger.info("Config loaded — active parameters:")
    logger.info(
        f"  VCP   | min_contractions={cfg.get('vcp_min_contractions', 2)}  "
        f"max_first_depth={cfg.get('vcp_max_first_depth', 0.35):.0%}  "
        f"contraction_ratio={cfg.get('vcp_contraction_ratio', 1.10)}  "
        f"vol_decline_ratio={cfg.get('vcp_volume_decline_ratio', 0.90)}  "
        f"lookback_candles={cfg.get('vcp_lookback_candles', 60)}"
    )
    logger.info(
        f"  Entry | buy_zone_max={cfg.get('entry_buy_zone_max_pct', 0.05):.0%}  "
        f"min_vol_ratio={cfg.get('entry_min_vol_ratio', 1.50)}x  "
        f"max_stop={cfg.get('entry_max_stop_pct', 0.08):.0%}  "
        f"min_avg_vol={cfg.get('entry_min_avg_volume', 400000):,}"
    )
    logger.info(
        f"  Fetch | request_delay={cfg.get('request_delay_sec', 0.35)}s  "
        f"max_retries={cfg.get('max_retries', 3)}  "
        f"retry_base_delay={cfg.get('retry_base_delay', 2.0)}s"
    )
    return cfg


# ─────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────
@dataclass
class Contraction:
    high: float
    low: float
    depth_pct: float        # e.g. 18.5  (percent)
    high_date: str
    low_date: str


@dataclass
class VCPResult:
    valid: bool
    window: str = ""        # "6M" | "3M"
    contractions: list[Contraction] = field(default_factory=list)
    pivot: float = 0.0
    tightness_pct: float = 0.0      # last contraction depth %
    base_length_days: int = 0
    volume_dryup_pct: float = 0.0
    reason: str = ""


@dataclass
class EntryResult:
    in_buy_zone: bool
    extended: bool
    pct_from_pivot: float
    volume_ratio: float             # latest vol / 50d avg vol
    breakout_confirmed: bool
    stop_loss: float = 0.0          # absolute stop price (based on pivot as entry)
    stop_pct: float  = 0.0          # % risk from pivot to stop
    stop_type: str   = ""           # "STRUCTURE" | "FIXED_MAX"
    target_2r: float = 0.0          # pivot + 2 × risk
    target_3r: float = 0.0          # pivot + 3 × risk
    reason: str      = ""


@dataclass
class ScreenResult:
    symbol: str
    vcp_window: str                 # "6M" | "3M" | "BOTH"
    contractions: int
    pivot: float
    current_price: float
    pct_from_pivot: float
    volume_ratio: float
    tightness_pct: float
    base_length_days: int
    volume_dryup_pct: float
    action: str
    stop_loss: float  = 0.0         # stop price (anchored to pivot entry)
    stop_pct: float   = 0.0         # risk % from pivot to stop
    stop_type: str    = ""          # "STRUCTURE" | "FIXED_MAX"
    target_2r: float  = 0.0         # 2R profit target
    target_3r: float  = 0.0         # 3R profit target
    skip_reason: str  = ""


# ─────────────────────────────────────────────────────────────────
# Data Fetcher  (Zerodha KiteConnect — requires api_key + access_token)
# ─────────────────────────────────────────────────────────────────
class NSEDataFetcher:
    """
    Fetches daily OHLCV data from Zerodha KiteConnect historical data API.

    Requires a valid api_key and access_token in config.json.
    Generate a fresh access_token each day via the KiteConnect login flow:
        https://kite.trade/docs/connect/v3/user/#login-flow

    KiteConnect historical_data() returns a list of dicts with keys:
        date, open, high, low, close, volume
    We normalise to a DatetimeIndex DataFrame with float columns.
    """

    EXCHANGE = "NSE"
    INTERVAL = "day"

    # Rate-limit keywords used to distinguish retriable errors from hard failures.
    # KiteConnect surfaces 429s as exception messages containing these strings.
    _RATE_LIMIT_KEYWORDS = ("429", "too many", "rate limit", "throttl")

    def __init__(self, kite: KiteConnect, cfg: dict) -> None:
        self.kite              = kite
        self._token_map: dict[str, int] = {}
        self._request_delay    = float(cfg.get("request_delay_sec",  0.35))
        self._max_retries      = int(cfg.get("max_retries",           3))
        self._retry_base_delay = float(cfg.get("retry_base_delay",    2.0))
        self._load_instruments()

    # ── Instrument token cache ────────────────────────────────────

    def _load_instruments(self) -> None:
        """
        Download the full NSE instrument list once and build a
        tradingsymbol -> instrument_token lookup table.
        """
        logger.info("Loading NSE instrument list from Zerodha …")
        try:
            instruments = self.kite.instruments(self.EXCHANGE)
        except Exception as e:
            raise RuntimeError(f"Failed to load NSE instruments: {e}") from e

        for inst in instruments:
            self._token_map[inst["tradingsymbol"].upper()] = inst["instrument_token"]

        logger.info(f"  Loaded {len(self._token_map)} NSE instruments.")

    def _get_token(self, symbol: str) -> Optional[int]:
        token = self._token_map.get(symbol.upper())
        if token is None:
            logger.debug(f"[{symbol}] Instrument token not found in NSE list.")
        return token

    # ── Public fetch ──────────────────────────────────────────────

    def fetch(self, symbol: str, months: int = 6) -> Optional[pd.DataFrame]:
        token = self._get_token(symbol)
        if token is None:
            return None

        end_dt   = date.today()
        start_dt = end_dt - timedelta(days=int(months * 30.5))

        records  = None
        for attempt in range(1, self._max_retries + 1):
            try:
                time.sleep(self._request_delay)
                records = self.kite.historical_data(
                    instrument_token = token,
                    from_date        = start_dt,
                    to_date          = end_dt,
                    interval         = self.INTERVAL,
                    continuous       = False,
                    oi               = False,
                )
                break   # success — exit retry loop

            except Exception as e:
                err_lower = str(e).lower()
                is_rate_limited = any(kw in err_lower for kw in self._RATE_LIMIT_KEYWORDS)

                if is_rate_limited:
                    if attempt < self._max_retries:
                        wait = self._retry_base_delay * (2 ** (attempt - 1))
                        logger.warning(
                            f"[{symbol}] Rate-limited "
                            f"(attempt {attempt}/{self._max_retries}) "
                            f"— waiting {wait:.0f}s before retry"
                        )
                        time.sleep(wait)
                        continue
                    else:
                        logger.warning(
                            f"[{symbol}] Rate-limited — "
                            f"all {self._max_retries} retries exhausted, skipping"
                        )
                        return None
                else:
                    # Non-retriable error (bad token, network drop, etc.)
                    logger.debug(f"[{symbol}] Fetch error (attempt {attempt}): {e}")
                    return None

        if records is None:
            return None

        if not records:
            return None

        df = pd.DataFrame(records)                      # columns: date open high low close volume
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()

        required = {"open", "high", "low", "close", "volume"}
        missing  = required - set(df.columns)
        if missing:
            logger.debug(f"[{symbol}] Missing columns from KiteConnect: {missing}")
            return None

        df = df[["open", "high", "low", "close", "volume"]].copy()
        df[["open", "high", "low", "close", "volume"]] = (
            df[["open", "high", "low", "close", "volume"]]
            .apply(pd.to_numeric, errors="coerce")
        )
        df = df.dropna()
        return df


# ─────────────────────────────────────────────────────────────────
# VCP Detector
# ─────────────────────────────────────────────────────────────────
class VCPDetector:
    """
    Detects Volatility Contraction Patterns on a given DataFrame slice.

    Rules:
        - Minimum MIN_CONTRACTIONS contractions
        - Each contraction depth < previous * CONTRACTION_RATIO  (10% tolerance)
        - Volume in second half of base < first half * VOLUME_DECLINE_RATIO
        - First contraction depth <= MAX_FIRST_DEPTH
    """

    def __init__(self, cfg: dict) -> None:
        self.MIN_CONTRACTIONS     = int(cfg.get("vcp_min_contractions",     2))
        self.MAX_FIRST_DEPTH      = float(cfg.get("vcp_max_first_depth",    0.35))  # ref: 1st contraction max 35%
        self.CONTRACTION_RATIO    = float(cfg.get("vcp_contraction_ratio",  1.10))
        self.VOLUME_DECLINE_RATIO = float(cfg.get("vcp_volume_decline_ratio", 0.90))
        self.LOOKBACK_CANDLES     = int(cfg.get("vcp_lookback_candles",     60))

    def detect(self, df: pd.DataFrame, window_label: str) -> VCPResult:
        if len(df) < 40:
            return VCPResult(False, window_label, reason="Insufficient candles")

        # Find base high (left side of VCP)
        lookback      = min(self.LOOKBACK_CANDLES, len(df) - 1)
        base_high_idx = df["high"].iloc[-lookback:].idxmax()

        contractions = self._find_contractions(df, base_high_idx)

        if len(contractions) < self.MIN_CONTRACTIONS:
            return VCPResult(
                False, window_label,
                reason=f"Only {len(contractions)} contraction(s) found (need {self.MIN_CONTRACTIONS})"
            )

        if not self._validate_depths(contractions):
            return VCPResult(False, window_label, reason="Contractions not progressively tightening")

        if not self._validate_volume(df, base_high_idx):
            return VCPResult(False, window_label, reason="Volume not contracting in base")

        pivot        = round(contractions[-1].high, 2)
        tightness    = contractions[-1].depth_pct
        base_days    = (df.index[-1] - df.index[df.index.get_loc(base_high_idx)]).days
        vol_dryup    = self._volume_dryup(df, base_high_idx)

        return VCPResult(
            valid            = True,
            window           = window_label,
            contractions     = contractions,
            pivot            = pivot,
            tightness_pct    = round(tightness, 2),
            base_length_days = base_days,
            volume_dryup_pct = round(vol_dryup * 100, 2),
        )

    # ── Internal helpers ──────────────────────────────────────────

    def _find_contractions(self, df: pd.DataFrame, start_idx) -> list[Contraction]:
        subset        = df.loc[start_idx:]
        contractions  : list[Contraction] = []
        current_high  = float(subset["high"].iloc[0])
        current_hdate = str(subset.index[0].date())
        i             = 0

        while i < len(subset) - 5:
            win_end = min(i + 10, len(subset))
            window  = subset.iloc[i:win_end]

            sl_loc = window["low"].idxmin()
            sl_val = float(window["low"].loc[sl_loc])

            depth = (current_high - sl_val) / current_high
            if depth <= 0 or depth > self.MAX_FIRST_DEPTH * 1.5:
                i += 1
                continue

            # Next swing high after this swing low
            remaining = subset.loc[sl_loc:]
            if len(remaining) < 5:
                break

            nw     = remaining.iloc[:10]
            sh_loc = nw["high"].idxmax()
            sh_val = float(nw["high"].loc[sh_loc])

            contractions.append(Contraction(
                high      = round(current_high, 2),
                low       = round(sl_val, 2),
                depth_pct = round(depth * 100, 2),
                high_date = current_hdate,
                low_date  = str(sl_loc.date()),
            ))

            current_high  = sh_val
            current_hdate = str(sh_loc.date())
            loc_sh = subset.index.get_loc(sh_loc)
            loc_s0 = subset.index.get_loc(subset.index[0])
            i      = (loc_sh - loc_s0) + 1

        return contractions

    def _validate_depths(self, contractions: list[Contraction]) -> bool:
        """Each depth must be strictly tighter than previous (10% tolerance)."""
        for i in range(1, len(contractions)):
            if contractions[i].depth_pct >= contractions[i - 1].depth_pct * self.CONTRACTION_RATIO:
                return False
        return True

    def _validate_volume(self, df: pd.DataFrame, start_idx) -> bool:
        """Volume should trend lower across the base formation."""
        vol = df["volume"].loc[start_idx:]
        if len(vol) < 20:
            return False
        mid    = len(vol) // 2
        first  = float(vol.iloc[:mid].mean())
        second = float(vol.iloc[mid:].mean())
        return second < first * self.VOLUME_DECLINE_RATIO

    def _volume_dryup(self, df: pd.DataFrame, start_idx) -> float:
        """Fraction of volume that dried up vs pre-base average."""
        vol    = df["volume"]
        before = float(vol.loc[:start_idx].iloc[-20:].mean())
        recent = float(vol.iloc[-5:].mean())
        if before == 0:
            return 0.0
        return max((before - recent) / before, 0.0)


# ─────────────────────────────────────────────────────────────────
# Entry / Breakout Filter
# ─────────────────────────────────────────────────────────────────
class EntryFilter:
    """
    Final gate — checks whether the stock is actionable right now.

    Breakout confirmed : price in buy zone  AND  volume >= MIN_VOL_RATIO x 50d avg
    Buy zone           : 0% to BUY_ZONE_MAX_PCT above pivot
    Extended           : > BUY_ZONE_MAX_PCT above pivot (too late, skip)
    Pre-breakout       : price still below pivot
    """

    def __init__(self, cfg: dict) -> None:
        self.BUY_ZONE_MAX_PCT = float(cfg.get("entry_buy_zone_max_pct", 0.05))
        self.MIN_VOL_RATIO    = float(cfg.get("entry_min_vol_ratio",    1.50))
        # Stop loss cap: max 8% from entry — Minervini hard rule.
        # Structure stop (1% below last contraction low) is preferred when tighter.
        self.MAX_STOP_PCT     = float(cfg.get("entry_max_stop_pct",     0.08))
        # Liquidity gate: skip thinly-traded stocks (ref: avg vol > 400K)
        self.MIN_AVG_VOLUME   = float(cfg.get("entry_min_avg_volume",   400_000))

    # ── Liquidity gate ───────────────────────────────────────────
    def is_liquid(self, df: pd.DataFrame) -> bool:
        """Return False if the 50-day avg volume is below the minimum threshold."""
        avg_vol_50 = float(df["volume"].rolling(50).mean().iloc[-1])
        return avg_vol_50 >= self.MIN_AVG_VOLUME

    # ── Stop loss (anchored to pivot as the assumed entry price) ──
    def _calculate_stop(
        self, entry_price: float, last_contraction_low: float
    ) -> tuple[float, float, str]:
        """
        Return (stop_price, stop_pct, stop_type).

        Two candidates, pick the TIGHTER (higher price / lower % risk):
          • Structure stop  — 1% below the last VCP contraction low.
            This is chart-based and often tighter than the fixed cap.
          • Fixed-max stop  — MAX_STOP_PCT (default 8%) below entry.
            Minervini hard rule: never risk more than 7-8%.
        """
        structure_stop     = last_contraction_low * 0.99
        structure_stop_pct = (entry_price - structure_stop) / entry_price * 100

        fixed_stop     = entry_price * (1 - self.MAX_STOP_PCT)
        fixed_stop_pct = self.MAX_STOP_PCT * 100

        if structure_stop_pct <= fixed_stop_pct:
            return round(structure_stop, 2), round(structure_stop_pct, 2), "STRUCTURE"
        else:
            return round(fixed_stop, 2), round(fixed_stop_pct, 2), "FIXED_MAX"

    def evaluate(self, df: pd.DataFrame, vcp: VCPResult) -> EntryResult:
        current_price = float(df["close"].iloc[-1])
        latest_vol    = float(df["volume"].iloc[-1])
        avg_vol_50    = float(df["volume"].rolling(50).mean().iloc[-1])

        vol_ratio      = round(latest_vol / avg_vol_50, 2) if avg_vol_50 > 0 else 0.0
        pct_from_pivot = round((current_price - vcp.pivot) / vcp.pivot * 100, 2)

        in_buy_zone        = 0 <= pct_from_pivot <= self.BUY_ZONE_MAX_PCT * 100
        extended           = pct_from_pivot > self.BUY_ZONE_MAX_PCT * 100
        breakout_confirmed = in_buy_zone and vol_ratio >= self.MIN_VOL_RATIO

        # ── Stop loss + profit targets (pivot = assumed entry) ────
        last_low = vcp.contractions[-1].low if vcp.contractions else vcp.pivot * 0.92
        stop_loss, stop_pct, stop_type = self._calculate_stop(vcp.pivot, last_low)
        risk          = vcp.pivot - stop_loss
        target_2r     = round(vcp.pivot + risk * 2, 2)
        target_3r     = round(vcp.pivot + risk * 3, 2)

        reason = ""
        if extended:
            reason = f"Extended {pct_from_pivot:.1f}% above pivot"
        elif pct_from_pivot < 0:
            reason = f"Price {abs(pct_from_pivot):.1f}% below pivot"

        return EntryResult(
            in_buy_zone        = in_buy_zone,
            extended           = extended,
            pct_from_pivot     = pct_from_pivot,
            volume_ratio       = vol_ratio,
            breakout_confirmed = breakout_confirmed,
            stop_loss          = stop_loss,
            stop_pct           = stop_pct,
            stop_type          = stop_type,
            target_2r          = target_2r,
            target_3r          = target_3r,
            reason             = reason,
        )


# ─────────────────────────────────────────────────────────────────
# Single-Stock Screener  (called concurrently)
# ─────────────────────────────────────────────────────────────────
class StockScreener:
    """
    Full pipeline for one symbol:
        fetch  ->  VCP (6M + 3M independently)  ->  entry filter  ->  ScreenResult

    Note: Stage 2 check is intentionally skipped.
    Watchlist is assumed to already satisfy all 8 Trend Template criteria.
    """

    THREE_MONTH_CANDLES = 63    # approx trading days in 3 months

    def __init__(self, fetcher: NSEDataFetcher, vcp: VCPDetector, entry: EntryFilter):
        self.fetcher = fetcher
        self.vcp     = vcp
        self.entry   = entry

    def screen(self, symbol: str) -> Optional[ScreenResult]:
        # 1. Fetch 6 months of daily data
        df_6m = self.fetcher.fetch(symbol, months=6)
        if df_6m is None or len(df_6m) < 60:
            logger.debug(f"[{symbol}] SKIP — no/insufficient data")
            return None

        # 2. Liquidity gate — skip thinly traded stocks before heavier computation
        if not self.entry.is_liquid(df_6m):
            avg_vol = df_6m["volume"].rolling(50).mean().iloc[-1]
            logger.debug(
                f"[{symbol}] SKIP — avg vol {avg_vol:,.0f} < "
                f"min {self.entry.MIN_AVG_VOLUME:,.0f}"
            )
            return None

        # 3. VCP on BOTH windows independently
        df_3m  = df_6m.iloc[-self.THREE_MONTH_CANDLES:]
        vcp_6m = self.vcp.detect(df_6m, "6M")
        vcp_3m = self.vcp.detect(df_3m, "3M")

        valid_vcps = [v for v in [vcp_6m, vcp_3m] if v.valid]

        if not valid_vcps:
            logger.debug(
                f"[{symbol}] SKIP — no VCP. "
                f"6M: {vcp_6m.reason} | 3M: {vcp_3m.reason}"
            )
            return None

        # Best VCP = tightest last contraction (most explosive potential)
        best_vcp     = min(valid_vcps, key=lambda v: v.tightness_pct)
        window_label = "BOTH" if len(valid_vcps) == 2 else best_vcp.window

        # 4. Entry / breakout gate + stop loss + targets
        entry_result = self.entry.evaluate(df_6m, best_vcp)

        # 5. Action label
        #
        #  ┌──────────────────────────────────────────────────────────┐
        #  │  RIGID BUY  →  "BUY ZONE"                               │
        #  │  All three conditions must be true simultaneously:       │
        #  │   • Price within 0-5% ABOVE pivot (in buy zone)         │
        #  │   • Volume on breakout day ≥ 1.5× 50-day average        │
        #  │   • VCP confirmed in 6M or 3M window                    │
        #  │  Stop: structure stop (1% below last swing low) or      │
        #  │        8% max from pivot — whichever is tighter.        │
        #  └──────────────────────────────────────────────────────────┘
        if entry_result.breakout_confirmed:
            action = "BUY ZONE"           # ← RIGID BUY signal
        elif entry_result.in_buy_zone:
            action = "WATCH (low vol)"    # wait for volume to confirm
        elif entry_result.extended:
            action = "EXTENDED"           # >5% above pivot — do NOT chase
        else:
            action = "PRE-BREAKOUT"       # below pivot — wait for the break

        return ScreenResult(
            symbol           = symbol,
            vcp_window       = window_label,
            contractions     = len(best_vcp.contractions),
            pivot            = best_vcp.pivot,
            current_price    = round(float(df_6m["close"].iloc[-1]), 2),
            pct_from_pivot   = entry_result.pct_from_pivot,
            volume_ratio     = entry_result.volume_ratio,
            tightness_pct    = best_vcp.tightness_pct,
            base_length_days = best_vcp.base_length_days,
            volume_dryup_pct = best_vcp.volume_dryup_pct,
            action           = action,
            stop_loss        = entry_result.stop_loss,
            stop_pct         = entry_result.stop_pct,
            stop_type        = entry_result.stop_type,
            target_2r        = entry_result.target_2r,
            target_3r        = entry_result.target_3r,
            skip_reason      = entry_result.reason,
        )


# ─────────────────────────────────────────────────────────────────
# Master Screener  (concurrent orchestrator)
# ─────────────────────────────────────────────────────────────────
class MasterScreener:
    """
    Loads the watchlist, fans out StockScreener across all symbols
    concurrently via ThreadPoolExecutor, then collects and prints results.
    """

    def __init__(self, config: dict):
        self.config      = config
        self.max_workers = config.get("max_threads", 20)

        # ── Zerodha KiteConnect setup ──────────────────────────────
        kite = Zerodha()
        kite.set_access_token()

        fetcher          = NSEDataFetcher(kite, config)
        vcp_detector     = VCPDetector(config)
        entry_filter     = EntryFilter(config)
        self.screener    = StockScreener(fetcher, vcp_detector, entry_filter)

    # ── Watchlist ─────────────────────────────────────────────────

    def load_watchlist(self) -> list[str]:
        csv_path = Path(self.config["csv_path"])
        if not csv_path.exists():
            raise FileNotFoundError(f"Watchlist file not found: {csv_path}")

        ext = csv_path.suffix.lower()
        if ext == ".csv":
            df = pd.read_csv(csv_path)
        elif ext in (".xlsx", ".xls"):
            df = pd.read_excel(csv_path)
        else:
            raise ValueError(f"Unsupported file type '{ext}'. Use .csv or .xlsx")

        # Auto-detect symbol column (case-insensitive)
        symbol_col = None
        for col in df.columns:
            if col.strip().lower() in ("symbol", "symbols", "stock", "ticker", "scrip", "name"):
                symbol_col = col
                break
        if symbol_col is None:
            symbol_col = df.columns[0]
            logger.info(f"Symbol column not found by name — using first column: '{symbol_col}'")

        symbols = (
            df[symbol_col]
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
            .tolist()
        )
        logger.info(f"Loaded {len(symbols)} symbols from '{csv_path.name}'")
        return symbols

    # ── Concurrent scan ───────────────────────────────────────────

    def run(self) -> list[ScreenResult]:
        symbols = self.load_watchlist()
        results : list[ScreenResult] = []
        skipped = 0

        logger.info(
            f"Starting scan — {len(symbols)} stocks | "
            f"{self.max_workers} threads | VCP windows: 6M + 3M"
        )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.screener.screen, sym): sym for sym in symbols}

            for i, future in enumerate(as_completed(futures), 1):
                sym = futures[future]
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                    else:
                        skipped += 1
                except Exception as exc:
                    logger.warning(f"[{sym}] Unhandled error: {exc}")
                    skipped += 1

                if i % 50 == 0 or i == len(symbols):
                    logger.info(
                        f"  Progress: {i}/{len(symbols)} | "
                        f"passing: {len(results)} | skipped: {skipped}"
                    )

        logger.info(f"Scan complete — {len(results)} passing | {skipped} skipped")
        return results

    # ── Display ───────────────────────────────────────────────────

    def display(self, results: list[ScreenResult]) -> None:
        if not results:
            print("\n  No stocks passed all filters today.\n")
            return

        _priority = {
            "BUY ZONE":      0,
            "WATCH (low vol)": 1,
            "PRE-BREAKOUT":  2,
            "EXTENDED":      3,
        }
        results.sort(key=lambda r: (_priority.get(r.action, 9), r.tightness_pct))

        rows = [
            {
                "Symbol":        r.symbol,
                "VCP Window":    r.vcp_window,
                "Contractions":  r.contractions,
                "Pivot":         f"{r.pivot:.2f}",
                "Price":         f"{r.current_price:.2f}",
                "% from Pivot":  f"{r.pct_from_pivot:+.2f}%",
                "Vol Ratio":     f"{r.volume_ratio:.2f}x",
                "Tightness %":   f"{r.tightness_pct:.1f}%",
                "Base (days)":   r.base_length_days,
                "Vol Dry-up %":  f"{r.volume_dryup_pct:.1f}%",
                # Risk management columns (pivot = assumed entry price)
                "Stop":          f"{r.stop_loss:.2f} ({r.stop_pct:.1f}%)",
                "Stop Type":     r.stop_type,
                "2R Target":     f"{r.target_2r:.2f}",
                "3R Target":     f"{r.target_3r:.2f}",
                "Action":        r.action,
            }
            for r in results
        ]

        df_out = pd.DataFrame(rows)

        print("\n" + "=" * 160)
        print("  MINERVINI SWING SCREENER — NSE  |  VCP + Entry Filter")
        print(f"  Date : {date.today()}   |   Stocks passing : {len(results)}")
        print("=" * 160)
        print(tabulate(df_out, headers="keys", tablefmt="rounded_outline", showindex=False))
        print("=" * 160)

        counts = df_out["Action"].value_counts()
        print("\n  Summary:")
        for action, cnt in counts.items():
            print(f"    {action:<20}  ->  {cnt} stock(s)")
        print()

    # ── Save CSV ──────────────────────────────────────────────────

    def save_csv(self, results: list[ScreenResult]) -> None:
        if not results:
            return
        out_path = Path(f"minervini_results_{date.today()}.csv")
        pd.DataFrame([r.__dict__ for r in results]).to_csv(out_path, index=False)
        logger.info(f"Results saved -> {out_path}")


# ─────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────
def main() -> None:
    print("\n  Minervini NSE Swing Screener")
    print("  SEPA: VCP (6M + 3M) + Breakout Filter  |  Powered by Zerodha KiteConnect\n")

    log_file = _LOG_DIR / f"minervini_{date.today()}.log"
    logger.info("=" * 60)
    logger.info(f"Session started  |  log -> {log_file}")
    logger.info("=" * 60)

    try:
        config  = load_config()
        master  = MasterScreener(config)
        results = master.run()
        master.display(results)
        master.save_csv(results)
    except Exception:
        logger.exception("Fatal error — screener terminated")
        raise
    finally:
        logger.info("Session ended")


if __name__ == "__main__":
    main()