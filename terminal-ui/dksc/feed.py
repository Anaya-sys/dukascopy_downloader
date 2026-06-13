"""
Real-time market feed backed by yfinance.

Design notes
------------
* No API key required. yfinance fetches data directly from Yahoo Finance.
* A single background worker thread walks the instrument universe one symbol
  at a time with a polite delay, then loops back to keep data fresh.
* Tickers are always rendered in UPPERCASE.
* yfinance.Ticker.fast_info gives price + previous close so we can derive
  the change percentage without extra calls.
"""

import threading
import time
import json

try:
    import yfinance as yf
except ImportError:
    yf = None

CALL_INTERVAL = 2.0       # seconds between fetches (polite, no rate limit)
BACKOFF_INTERVAL = 30.0   # back off on repeated errors


# --- Instrument universe (Yahoo Finance symbols) --------------------------
SYMBOLS = [
    # Equities
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "NFLX",
    "AMD", "INTC", "IBM", "ORCL", "JPM", "BAC", "DIS", "KO",
    # FX (Yahoo format)
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X",
    "USDCHF=X", "USDCAD=X", "NZDUSD=X", "EURGBP=X",
    "EURJPY=X", "XAUUSD=X",
    # Crypto
    "BTC-USD", "ETH-USD", "SOL-USD",
    "XRP-USD", "ADA-USD", "DOGE-USD",
]

# Map Yahoo symbol → display ticker
def _display(sym):
    return (sym
            .replace("=X", "")
            .replace("-", "")
            .upper())


class Quote:
    __slots__ = ("ticker", "price", "change_pct", "ts")

    def __init__(self, ticker, price, change_pct, ts):
        self.ticker = ticker
        self.price = price
        self.change_pct = change_pct
        self.ts = ts


class AlphaVantageFeed:
    """
    Background polling feed using yfinance (no API key needed).

    Public API identical to the original so app.py needs no changes:
        feed.start()
        feed.stop()
        feed.snapshot()    -> dict[ticker] -> Quote
        feed.set_log(fn)   -> optional log sink: fn(msg, type)
    """

    def __init__(self):
        self._symbols = list(SYMBOLS)
        self._quotes = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._log = None

        if yf is None:
            raise RuntimeError(
                "yfinance not installed. Run: pip install yfinance")

    def set_log(self, fn):
        self._log = fn

    def _emit(self, msg, typ="info"):
        if self._log:
            try:
                self._log(msg, typ)
            except Exception:
                pass

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="yf-feed", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def snapshot(self):
        with self._lock:
            return dict(self._quotes)

    def count(self):
        with self._lock:
            return len(self._quotes)

    def _run(self):
        self._emit(
            "FEED worker online · source: finance.yahoo.com · yfinance", "ok")
        idx = 0
        consecutive_errors = 0
        while not self._stop.is_set():
            sym = self._symbols[idx]
            try:
                self._fetch(sym)
                consecutive_errors = 0
            except Exception as e:
                consecutive_errors += 1
                self._emit(f"[{_display(sym)}] feed error: {e}", "err")
            idx = (idx + 1) % len(self._symbols)
            interval = BACKOFF_INTERVAL if consecutive_errors >= 5 else CALL_INTERVAL
            self._stop.wait(interval)

    def _fetch(self, sym):
        ticker_obj = yf.Ticker(sym)
        info = ticker_obj.fast_info

        price = getattr(info, "last_price", None)
        if price is None or price != price:   # NaN check
            self._emit(f"[{_display(sym)}] no price data", "warn")
            return

        prev_close = getattr(info, "previous_close", None)
        change_pct = None
        if prev_close and prev_close > 0:
            change_pct = (price - prev_close) / prev_close * 100.0

        display = _display(sym)
        q = Quote(display, float(price), change_pct, time.time())
        with self._lock:
            self._quotes[display] = q

        self._emit(f"[{display}] quote refreshed @ {fmt_price(q.price)}", "ok")


def fmt_price(p):
    """Human-friendly price formatting based on magnitude."""
    if p is None:
        return "--"
    if p >= 1000:
        return f"{p:,.2f}"
    if p >= 100:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.4f}".rstrip("0").rstrip(".")
    return f"{p:.5f}"