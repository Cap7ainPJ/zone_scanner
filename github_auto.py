from datetime import datetime, date, timedelta
import time
import json
import gzip
import requests
import pandas as pd
import os
from html import escape
from collections import defaultdict
import csv
from io import StringIO

# ======================================================
# USER INPUT TOGGLE / DEFAULTS
# ======================================================
USE_USER_INPUTS = False                # False = use defaults below, True = ask user input
DEFAULT_EXPIRY_MODE = "CURRENT"        # CURRENT or NEXT
DEFAULT_FILTER_ZONES = True
DEFAULT_ZONE_PROXIMITY_PERCENT = 7
DEFAULT_SHOW_CONSOLE_OUTPUT = True
DEFAULT_AUTO_NEXT_BATCH = True         # Used only when USE_USER_INPUTS = False

# ======================================================
# OUTPUT / FILES
# ======================================================
HTML_OUTPUT_FILE = "zones_report_perf.html"

# ======================================================
# GOOGLE DRIVE FILE IDS
# ======================================================
TOKEN_DRIVE_FILE_ID = "1ZQVJE0ZAJPTC9bwPmc3c5IsSEQI3Am7B"
SECTOR_MAP_FILE_ID = "1KlsgVU4Z9rEspQbOj8s-A4UiZZ_0RSm7"

# ======================================================
# TELEGRAM
# ======================================================
SEND_TELEGRAM_HTML = True
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8468300412:AAGQUGlwho6CSdVVoVpa9v1AAr9gpc3vf1o")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "183136633")

# ======================================================
# ANSI colors for terminal output
# ======================================================
GREEN = "\033[92m"
ORANGE = "\033[33m"
RESET = "\033[0m"

# ======================================================
# Zone filtering controls
# ======================================================
FILTER_ZONES = DEFAULT_FILTER_ZONES
ZONE_PROXIMITY_PERCENT = DEFAULT_ZONE_PROXIMITY_PERCENT
MAX_ZONES_TO_SHOW = 5

# ======================================================
# Console output toggle
# ======================================================
SHOW_CONSOLE_OUTPUT = DEFAULT_SHOW_CONSOLE_OUTPUT

# ======================================================
# Sector map output
# ======================================================
SYMBOL_TO_SECTORS = defaultdict(set)
SECTOR_SUMMARY = defaultdict(lambda: {"ce": 0, "pe": 0})

# ======================================================
# Performance caches
# ======================================================
EARLIEST_DATE_CACHE = {}
STITCHED_CACHE = {}

# ======================================================
# HTML HELPERS
# ======================================================
HTML_ROWS = []

# ======================================================
# NSE SESSION
# ======================================================
nse_session = requests.Session()
nse_headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.nseindia.com",
    "Accept": "application/json",
}
try:
    nse_session.get("https://www.nseindia.com", headers=nse_headers, timeout=10)
except:
    pass

# ======================================================
# CONFIG
# ======================================================
BATCH_SIZE = 300
INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
LTP_URL = "https://api.upstox.com/v3/market-quote/ltp"

# ======================================================
# GOOGLE DRIVE HELPERS
# ======================================================
def download_gdrive_text(file_id):
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    txt = r.text.strip()
    if "<html" in txt.lower():
        raise Exception(f"Google Drive file {file_id} is not publicly downloadable.")
    return txt

# ======================================================
# HTML HELPERS
# ======================================================
def option_box_html(side, opt_symbol, opt_price, zones, special_ids):
    side_cls = "ce-box" if side == "CE" else "pe-box"

    zone_lines = []
    for z in zones:
        star = '<span class="special">⭐</span>' if z["id"] in special_ids else ""
        zone_lines.append(
            f'<div class="option-zone-line">'
            f'High={escape(str(z["high"]))} | '
            f'Low={escape(str(z["low"]))} | '
            f'{escape(str(z["time"]))} {star}'
            f'</div>'
        )

    zone_html = "".join(zone_lines) if zone_lines else '<div class="empty">No zones</div>'

    return (
        f'<div class="option-box {side_cls}">'
        f'<div class="option-head"><b>{escape(side)}</b></div>'
        f'<div><b>Symbol:</b> {escape(str(opt_symbol))}</div>'
        f'<div><b>CMP:</b> {escape(str(opt_price))}</div>'
        f'<div class="option-zones">{zone_html}</div>'
        f'</div>'
    )

def zone_line_text(side, z, is_special):
    special = " ⭐ WITHIN PROXIMITY" if is_special else ""
    return f'{side} → ID={z["id"]} High={z["high"]} Low={z["low"]} Time={z["time"]}{special}'

def build_output_table_html():
    if not HTML_ROWS:
        return '<div class="empty">No qualifying rows found.</div>'

    return f"""
<div class="table-card">
    <div class="table-title">Scanner Output</div>
    <div class="table-wrap">
        <table class="main-table">
            <thead>
                <tr>
                    <th>Sr</th>
                    <th class="symbol-col">Symbol</th>
                    <th class="sector-col">Sector</th>
                    <th>CE Details</th>
                    <th>PE Details</th>
                </tr>
            </thead>
            <tbody>
                {''.join(HTML_ROWS)}
            </tbody>
        </table>
    </div>
</div>
"""

def build_sector_summary_html():
    if not SECTOR_SUMMARY:
        return '<div class="empty">No sector summary data available.</div>'

    rows = []
    total_ce = 0
    total_pe = 0

    for sector in sorted(SECTOR_SUMMARY.keys()):
        ce_count = SECTOR_SUMMARY[sector]["ce"]
        pe_count = SECTOR_SUMMARY[sector]["pe"]
        total_ce += ce_count
        total_pe += pe_count

        rows.append(
            f"<tr><td>{escape(sector)}</td><td>{ce_count}</td><td>{pe_count}</td></tr>"
        )

    rows.append(
        f"<tr style='font-weight:bold; background:#f2f2f2; color:#111;'>"
        f"<td>TOTAL</td><td>{total_ce}</td><td>{total_pe}</td></tr>"
    )

    return f"""
<div class="table-card">
    <div class="table-title">Sector Summary</div>
    <div class="table-wrap">
        <table class="summary-table">
            <thead>
                <tr>
                    <th>Sector</th>
                    <th>CE</th>
                    <th>PE</th>
                </tr>
            </thead>
            <tbody>
                {''.join(rows)}
            </tbody>
        </table>
    </div>
</div>
"""

def build_html_report(run_id, generated_at_str, expiry_used):
    return f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Zones Report</title>
<style>
body {{
    font-family: Arial, sans-serif;
    background: #0f1115;
    color: #e8e8e8;
    padding: 16px;
}}

.topbar {{
    background: #171a21;
    padding: 14px 16px;
    border-radius: 12px;
    margin-bottom: 16px;
    border: 1px solid #2a2f3a;
}}

.title {{
    font-size: 22px;
    font-weight: bold;
    margin-bottom: 6px;
}}

.subtitle {{
    color: #b8c0cc;
    font-size: 13px;
    line-height: 1.5;
}}

.table-card {{
    background: #171a21;
    border-radius: 12px;
    padding: 12px;
    margin-bottom: 15px;
    border: 1px solid #2a2f3a;
}}

.table-title {{
    font-size: 18px;
    font-weight: bold;
    margin-bottom: 10px;
}}

.table-wrap {{
    overflow-x: auto;
}}

table {{
    width: 100%;
    border-collapse: collapse;
    background: #11141b;
    border: 1px solid #2a2f3a;
}}

th, td {{
    padding: 8px;
    border-bottom: 1px solid #333;
    vertical-align: top;
    text-align: left;
    font-size: 13px;
}}

th {{
    background: #141922;
    color: #cfd6df;
}}

.symbol-col {{
    width: 140px;
    min-width: 140px;
    max-width: 140px;
    white-space: normal;
    line-height: 1.3;
}}

.symbol-main {{
    font-size: 14px;
    margin-bottom: 4px;
}}

.symbol-sub {{
    font-size: 12px;
    color: #b8c0cc;
    margin-bottom: 2px;
}}

.sector-col {{
    width: 200px;
    min-width: 200px;
    max-width: 200px;
    white-space: normal;
    word-break: break-word;
    line-height: 1.4;
}}

.option-box {{
    padding: 8px;
    border-radius: 8px;
    min-width: 260px;
}}

.ce-box {{
    background: rgba(50,160,80,0.10);
    border: 1px solid rgba(50,160,80,0.25);
}}

.pe-box {{
    background: rgba(210,150,40,0.10);
    border: 1px solid rgba(210,150,40,0.25);
}}

.option-head {{
    margin-bottom: 6px;
    font-size: 14px;
}}

.option-zone-line {{
    margin-top: 4px;
    font-size: 12px;
    line-height: 1.5;
}}

.special {{
    color: gold;
    font-weight: bold;
}}

.empty {{
    color: #9aa4b2;
    font-style: italic;
}}
</style>
</head>

<body>

<div class="topbar">
    <div class="title">Zones Report</div>
    <div class="subtitle">
        Generated at: {escape(generated_at_str)}<br>
        Run ID: {escape(run_id)}<br>
        Expiry: {escape(str(expiry_used))}
    </div>
</div>

{build_output_table_html()}

{build_sector_summary_html()}

</body>
</html>
"""

def write_html_report(run_id, generated_at_str, expiry_used):
    folder = os.path.dirname(HTML_OUTPUT_FILE)
    if folder and not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)

    with open(HTML_OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(build_html_report(run_id, generated_at_str, expiry_used))

    print("HTML saved:", HTML_OUTPUT_FILE)

# ======================================================
# SECTOR MAP HELPERS
# ======================================================
def load_sector_map():
    SYMBOL_TO_SECTORS.clear()

    try:
        text = download_gdrive_text(SECTOR_MAP_FILE_ID)
    except Exception as e:
        print(f"\nWarning: failed to download sector map: {e}")
        return

    loaded_rows = 0

    try:
        f = StringIO(text)
        sample = text[:2048]

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            reader = csv.DictReader(f, delimiter=dialect.delimiter)
        except:
            f.seek(0)
            reader = csv.DictReader(f)

        headers = reader.fieldnames or []
        header_map = {str(h).strip().lower(): h for h in headers}

        symbol_col = header_map.get("symbol")
        sector_col = header_map.get("sector")

        if not symbol_col or not sector_col:
            print("\nWarning: sector map missing symbol/sector columns.")
            return

        for row in reader:
            symbol = str(row.get(symbol_col, "")).strip().upper()
            sector_raw = str(row.get(sector_col, "")).strip()

            if not symbol or not sector_raw:
                continue

            parts = [x.strip() for x in sector_raw.replace("|", ",").replace(";", ",").split(",") if x.strip()]
            for sector in parts:
                SYMBOL_TO_SECTORS[symbol].add(sector)

            loaded_rows += 1

        print(f"\nLoaded sector map rows: {loaded_rows}")
        print(f"Unique symbols in sector map: {len(SYMBOL_TO_SECTORS)}")

    except Exception as e:
        print(f"\nWarning: sector map parse failed: {e}")

def add_to_sector_summary(symbol, ce_special_count, pe_special_count):
    sectors = SYMBOL_TO_SECTORS.get(str(symbol).upper(), set())
    if not sectors:
        return

    for sector in sectors:
        SECTOR_SUMMARY[sector]["ce"] += ce_special_count
        SECTOR_SUMMARY[sector]["pe"] += pe_special_count

def print_sector_summary():
    if not SECTOR_SUMMARY:
        print("\nNo sector summary data available.")
        return

    total_ce = 0
    total_pe = 0

    print("\n" + "=" * 72)
    print("SECTOR SUMMARY")
    print("=" * 72)
    print(f"{'SECTOR':35} {'CE IN PROX':>15} {'PE IN PROX':>15}")
    print("-" * 72)

    for sector in sorted(SECTOR_SUMMARY.keys()):
        ce_count = SECTOR_SUMMARY[sector]["ce"]
        pe_count = SECTOR_SUMMARY[sector]["pe"]
        total_ce += ce_count
        total_pe += pe_count
        print(f"{sector:35} {ce_count:>15} {pe_count:>15}")

    print("-" * 72)
    print(f"{'TOTAL':35} {total_ce:>15} {total_pe:>15}")
    print("=" * 72)

# ======================================================
# TOKEN
# ======================================================
def load_token():
    print("Downloading token...")
    tok = download_gdrive_text(TOKEN_DRIVE_FILE_ID)
    print("Token OK.")
    return tok

ACCESS_TOKEN = load_token()

SESSION = requests.Session()
SESSION.headers.update({
    "Accept": "application/json",
    "Authorization": f"Bearer {ACCESS_TOKEN}"
})

# ======================================================
# TELEGRAM HELPERS
# ======================================================
def send_html_to_telegram(run_id, generated_at_str, expiry_used):
    if not SEND_TELEGRAM_HTML:
        return

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram skipped: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing.")
        return

    if not os.path.exists(HTML_OUTPUT_FILE):
        print("Telegram skipped: HTML file not found.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"

    caption = (
        f"Zones Report\n"
        f"Run ID: {run_id}\n"
        f"Generated: {generated_at_str}\n"
        f"Expiry: {expiry_used}"
    )

    try:
        with open(HTML_OUTPUT_FILE, "rb") as f:
            files = {"document": (os.path.basename(HTML_OUTPUT_FILE), f, "text/html")}
            data = {
                "chat_id": TELEGRAM_CHAT_ID,
                "caption": caption[:1024],
            }
            r = requests.post(url, data=data, files=files, timeout=60)
            r.raise_for_status()
        print("Telegram HTML sent.")
    except Exception as e:
        print("Telegram send failed:", e)

# ======================================================
# BULK LTP
# ======================================================
def bulk_get_ltp(keys):
    keys = list(keys)
    results = {}
    chunk_sizes = [50, 25, 10, 5]

    for cs in chunk_sizes:
        idx = 0
        while idx < len(keys):
            chunk = keys[idx: idx + cs]
            try:
                r = SESSION.get(LTP_URL, params={"instrument_key": ",".join(chunk)}, timeout=12)
                if r.status_code == 429:
                    break

                r.raise_for_status()
                data = r.json().get("data", {})
                for _, info in data.items():
                    token = info.get("instrument_token")
                    if token:
                        results[token] = float(info["last_price"])

                idx += cs
            except:
                break

        if len(results) == len(keys):
            return results

        time.sleep(1)

    idx = 0
    while idx < len(keys):
        chunk = keys[idx: idx + 5]
        try:
            time.sleep(1)
            r = SESSION.get(LTP_URL, params={"instrument_key": ",".join(chunk)}, timeout=12)
            r.raise_for_status()
            data = r.json().get("data", {})
            for _, info in data.items():
                token = info.get("instrument_token")
                if token:
                    results[token] = float(info["last_price"])
        except:
            pass

        idx += 5

    return results

# ======================================================
# INSTRUMENT MASTER
# ======================================================
def download_instruments():
    print("\nDownloading instrument master...")
    r = SESSION.get(INSTRUMENTS_URL, timeout=60, stream=True)
    r.raise_for_status()

    with gzip.GzipFile(fileobj=r.raw) as gz:
        data = json.loads(gz.read().decode("utf-8"))

    print("Loaded", len(data), "instruments.")
    return data

# ======================================================
# STRUCTS
# ======================================================
def build_structs(inst):
    eq = {}
    fo = {}

    for item in inst:
        seg = item.get("segment")
        typ = item.get("instrument_type")

        if seg in ("NSE_EQ", "NSE_INDEX") and typ in ("EQ", "INDEX"):
            eq[item["trading_symbol"]] = item

        if seg == "NSE_FO" and typ in ("CE", "PE"):
            und = item.get("underlying_symbol")
            if und:
                fo.setdefault(und, []).append(item)

    return eq, fo

# ======================================================
# EXPIRY
# ======================================================
def get_all_expiries(fo_list):
    return sorted({
        datetime.fromtimestamp(x["expiry"] / 1000).date()
        for x in fo_list
        if isinstance(x.get("expiry"), (int, float))
    })

def pick_expiry_default(fo_list, mode):
    today = date.today()
    expiries = get_all_expiries(fo_list)

    current = None
    for e in expiries:
        if e >= today:
            current = e
            break
    if current is None:
        current = expiries[-1]

    try:
        next_exp = expiries[expiries.index(current) + 1]
    except:
        next_exp = current

    if str(mode).strip().upper() == "NEXT":
        return next_exp
    return current

def pick_expiry_user_choice(fo_list):
    today = date.today()
    expiries = get_all_expiries(fo_list)

    current = None
    for e in expiries:
        if e >= today:
            current = e
            break
    if current is None:
        current = expiries[-1]

    try:
        next_exp = expiries[expiries.index(current) + 1]
    except:
        next_exp = current

    print("\n================= EXPIRY INFO =================")
    print(f"Current expiry : {current}")
    print(f"Next expiry    : {next_exp}")
    print("================================================")

    print("\nChoose expiry:")
    print("1 = CURRENT")
    print("2 = NEXT")

    while True:
        ch = input("Enter 1 or 2: ").strip()
        if ch == "1":
            return current
        if ch == "2":
            return next_exp
        print("Invalid input.")

# ======================================================
# SPOT KEY
# ======================================================
def get_spot_key(symbol, eq_map):
    if symbol in eq_map:
        return eq_map[symbol]["instrument_key"]

    for v in eq_map.values():
        if v.get("segment") == "NSE_INDEX" and v["trading_symbol"] == symbol:
            return v["instrument_key"]

    raise Exception("Spot key not found for " + symbol)

# ======================================================
# ATM SELECTOR
# ======================================================
def choose_atm(spot, fo_list, expiry):
    ce = [x for x in fo_list if x["instrument_type"] == "CE"
          and datetime.fromtimestamp(x["expiry"] / 1000).date() == expiry]

    pe = [x for x in fo_list if x["instrument_type"] == "PE"
          and datetime.fromtimestamp(x["expiry"] / 1000).date() == expiry]

    ce_strikes = sorted({float(x["strike_price"]) for x in ce})
    pe_strikes = sorted({float(x["strike_price"]) for x in pe})

    if not ce_strikes or not pe_strikes:
        raise Exception("No valid strikes")

    nce = min(ce_strikes, key=lambda s: abs(s - spot))
    npe = min(pe_strikes, key=lambda s: abs(s - spot))

    if nce < spot:
        ups = [s for s in ce_strikes if s >= spot]
        if ups:
            nce = ups[0]

    if npe > spot:
        downs = [s for s in pe_strikes if s <= spot]
        if downs:
            npe = downs[-1]

    ce_row = next(x for x in ce if float(x["strike_price"]) == nce)
    pe_row = next(x for x in pe if float(x["strike_price"]) == npe)

    return ce_row, pe_row

# ======================================================
# RAW Upstox Historical Fetch
# ======================================================
def fetch_option_ohlc_1h_upstox(instrument_key, from_date, to_date):
    url = (
        "https://api.upstox.com/v3/historical-candle/"
        f"{instrument_key}/hours/1/{to_date}/{from_date}"
    )

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }

    try:
        r = SESSION.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        js = r.json()
    except:
        return []

    candles = js.get("data", {}).get("candles", [])
    if not candles:
        return []

    out = []
    for c in candles:
        ts, o, h, l, cl, vol, oi = c
        out.append([
            pd.to_datetime(ts),
            float(o), float(h), float(l), float(cl), float(vol)
        ])

    return out

# ======================================================
# INTRADAY FETCH
# ======================================================
def fetch_intraday_1h(instrument_key):
    url = f"https://api.upstox.com/v3/historical-candle/{instrument_key}/1hour"

    try:
        r = SESSION.get(url, timeout=20)
        r.raise_for_status()
        js = r.json()
    except:
        return []

    raw = js.get("data", {}).get("candles", [])

    out = []
    for c in raw:
        ts, o, h, l, cl, vol, oi = c
        out.append([
            pd.to_datetime(ts),
            float(o), float(h), float(l), float(cl), float(vol)
        ])
    return out

def find_earliest_candle_date(instrument_key):
    if instrument_key in EARLIEST_DATE_CACHE:
        return EARLIEST_DATE_CACHE[instrument_key]

    today = datetime.now().date()
    guess_days = [365, 180, 120, 90, 60, 45, 30, 20, 10, 5, 2]

    earliest_date = None

    for d in guess_days:
        from_dt = (today - timedelta(days=d)).isoformat()
        to_dt = today.isoformat()

        candles = fetch_option_ohlc_1h_upstox(instrument_key, from_dt, to_dt)
        if len(candles) > 0:
            earliest_date = candles[0][0].date()
            break

    if earliest_date is None:
        EARLIEST_DATE_CACHE[instrument_key] = None
        return None

    low = 1
    high = 2000

    while low <= high:
        mid = (low + high) // 2
        test_from = (earliest_date - timedelta(days=mid)).isoformat()

        candles = fetch_option_ohlc_1h_upstox(instrument_key, test_from, today.isoformat())

        if len(candles) > 0:
            earliest_date = candles[-1][0].date()
            low = mid + 1
        else:
            high = mid - 1

    EARLIEST_DATE_CACHE[instrument_key] = earliest_date
    return earliest_date

# ======================================================
# STITCHED 1H
# ======================================================
def fetch_stitched_1h(instrument_key):
    if instrument_key in STITCHED_CACHE:
        return STITCHED_CACHE[instrument_key]

    today = datetime.now().date()
    earliest = find_earliest_candle_date(instrument_key)

    if earliest is None:
        hist = []
    else:
        from_dt = earliest.isoformat()
        to_dt = today.isoformat()
        hist = fetch_option_ohlc_1h_upstox(instrument_key, from_dt, to_dt)

    live = fetch_intraday_1h(instrument_key)

    combined = hist + live

    dedup = {}
    for c in combined:
        dedup[c[0]] = c

    combined = list(dedup.values())
    combined.sort(key=lambda x: x[0])

    STITCHED_CACHE[instrument_key] = combined
    return combined

# ======================================================
# SAFE STITCH WRAPPER
# ======================================================
def safe_stitched_fetch(instrument_key, retries=1, delay=0.5):
    data = []
    for attempt in range(1, retries + 1):
        data = fetch_stitched_1h(instrument_key)

        if isinstance(data, list) and len(data) >= 10:
            return data

        print(f"⚠️ Insufficient stitched candles ({instrument_key}), retry {attempt}/{retries}")
        time.sleep(delay)

    print("❌ Could not fetch stitched candles:", instrument_key)
    return data if data else []

# ======================================================
# ORDERBLOCK ENGINE
# ======================================================
ZONE_CANDLES = 3

def get_bullish_zones(raw, expiry, live_cmp=None):
    n = len(raw)
    if n < 10:
        return [], []

    opens = [c[1] for c in raw]
    highs = [c[2] for c in raw]
    lows = [c[3] for c in raw]
    closes = [c[4] for c in raw]

    lastSignal = 0
    runningLowestHigh = None
    bullishBoxes = []

    for i in range(1, n):
        starterBull = (
            closes[i - 1] < opens[i - 1] and
            closes[i] > opens[i] and
            closes[i] > highs[i - 1]
        )

        if lastSignal == 0 and starterBull:
            lastSignal = 1
            runningLowestHigh = None

        if lastSignal == -1:
            if runningLowestHigh is None:
                runningLowestHigh = highs[i]
            else:
                runningLowestHigh = min(runningLowestHigh, highs[i])

        newBull = (
            lastSignal == -1 and
            runningLowestHigh is not None and
            closes[i] > runningLowestHigh
        )

        if newBull:
            left = i - 1 - (ZONE_CANDLES - 1)
            if left < 0:
                left = 0

            zoneHigh = max(highs[left:i])
            zoneLow = min(lows[left:i])

            start_idx = max(0, i - (ZONE_CANDLES + 1))

            bullishBoxes.append({
                "id": f"Z{start_idx}",
                "high": round(zoneHigh, 2),
                "low": round(zoneLow, 2),
                "cmp": round(closes[i], 2),
                "index": start_idx,
                "created_bar": i,
                "time": raw[start_idx][0] + timedelta(hours=1)
            })

            lastSignal = 1
            runningLowestHigh = None

        if closes[i] < opens[i] and closes[i] < lows[i - 1]:
            lastSignal = -1

    alive = []
    latest_cmp = live_cmp if live_cmp is not None else closes[-1]

    for z in bullishBoxes:
        zone_low = z["low"]
        created_bar = z["created_bar"]

        cmp_ok = latest_cmp >= zone_low
        hist_ok = not any(closes[j] < zone_low for j in range(created_bar + 1, n))

        if cmp_ok and hist_ok:
            alive.append(z)

    alive = sorted(alive, key=lambda z: z["time"], reverse=True)

    expiry_month_start = date(expiry.year, expiry.month, 1)
    expiry_start_limit = expiry_month_start - timedelta(days=10)

    recent_zones = [z for z in alive if z["time"].date() >= expiry_start_limit]
    recent_zones = recent_zones[:MAX_ZONES_TO_SHOW]

    if not FILTER_ZONES:
        return recent_zones, []

    qualifying = []

    for z in recent_zones:
        zone_low = z["low"]
        if zone_low <= 0:
            continue

        distance_pct = abs(latest_cmp - zone_low) / zone_low * 100

        if distance_pct <= ZONE_PROXIMITY_PERCENT:
            qualifying.append(z)

    return recent_zones, qualifying

# ======================================================
# PRINT BLOCK
# ======================================================
def print_block(rows, sr, ce_rows, pe_rows):
    for r in rows:
        sym, spot, expiry, ce_sym, ce_ltp, pe_sym, pe_ltp = r

        try:
            ce_info = ce_rows[sym]
            pe_info = pe_rows[sym]

            ce_raw = safe_stitched_fetch(ce_info["instrument_key"])
            pe_raw = safe_stitched_fetch(pe_info["instrument_key"])

            expiry_dt = datetime.fromisoformat(expiry).date()

            ce_live_cmp = ce_ltp if isinstance(ce_ltp, (int, float)) else None
            pe_live_cmp = pe_ltp if isinstance(pe_ltp, (int, float)) else None

            ce_all, ce_special = get_bullish_zones(ce_raw, expiry_dt, ce_live_cmp)
            pe_all, pe_special = get_bullish_zones(pe_raw, expiry_dt, pe_live_cmp)

            if FILTER_ZONES:
                if not ce_special and not pe_special:
                    continue
            else:
                if not ce_all and not pe_all:
                    continue

            ce_special_ids = {z["id"] for z in ce_special}
            pe_special_ids = {z["id"] for z in pe_special}

            sectors = sorted(SYMBOL_TO_SECTORS.get(str(sym).upper(), set()))
            sector_text = "<br>".join(escape(s) for s in sectors) if sectors else "-"
            sector_title = ", ".join(sectors) if sectors else "-"

            symbol_cell = (
                f"<div class='symbol-main'><b>{escape(str(sym))}</b></div>"
                f"<div class='symbol-sub'>Spot: {escape(str(spot))}</div>"
                f"<div class='symbol-sub'>Expiry: {escape(str(expiry))}</div>"
            )

            add_to_sector_summary(sym, len(ce_special), len(pe_special))

            if SHOW_CONSOLE_OUTPUT:
                print("\n" + "═" * 60)
                print(f"  🔵 SERIAL NO : {sr}")
                print("═" * 60)
                print(f"  SYMBOL       : {sym}")
                print(f"  SECTOR       : {', '.join(sectors) if sectors else '-'}")
                print(f"  SPOT         : {spot}")
                print(f"  EXPIRY       : {expiry}")
                print("─" * 60)
                print(f"  🟢 CE SYMBOL  : {ce_sym}")
                print(f"  💹 CE PRICE   : {ce_ltp}")
                print()
                print(f"  🟣 PE SYMBOL  : {pe_sym}")
                print(f"  📉 PE PRICE   : {pe_ltp}")
                print()
                print("      --- BULLISH ZONES ---")

                for z in ce_all:
                    print(GREEN + zone_line_text("CE", z, z["id"] in ce_special_ids) + RESET)

                for z in pe_all:
                    print(ORANGE + zone_line_text("PE", z, z["id"] in pe_special_ids) + RESET)

                print("═" * 60)

            ce_box = option_box_html("CE", ce_sym, ce_ltp, ce_all, ce_special_ids)
            pe_box = option_box_html("PE", pe_sym, pe_ltp, pe_all, pe_special_ids)

            HTML_ROWS.append(
                f"<tr>"
                f"<td>{sr}</td>"
                f"<td class='symbol-col'>{symbol_cell}</td>"
                f"<td class='sector-col' title='{escape(sector_title)}'>{sector_text}</td>"
                f"<td>{ce_box}</td>"
                f"<td>{pe_box}</td>"
                f"</tr>"
            )

            sr += 1

        except Exception as e:
            print("Error:", e)

    return sr

# ======================================================
# MAIN
# ======================================================
def main():
    global FILTER_ZONES, ZONE_PROXIMITY_PERCENT, SHOW_CONSOLE_OUTPUT

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    generated_at_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    HTML_ROWS.clear()
    SECTOR_SUMMARY.clear()
    EARLIEST_DATE_CACHE.clear()
    STITCHED_CACHE.clear()

    instruments = download_instruments()
    eq_map, fo_map = build_structs(instruments)
    load_sector_map()

    symbols = sorted(list(fo_map.keys()), key=lambda x: x.upper())
    total = len(symbols)

    print(f"\nTotal F&O underlyings: {total}")

    if total == 0:
        print("No F&O underlyings found.")
        return

    first_symbol = symbols[0]

    if USE_USER_INPUTS:
        expiry = pick_expiry_user_choice(fo_map[first_symbol])
        print(f"\nUsing expiry: {expiry}\n")

        try:
            ans = input("\nApply zone proximity filter? (Y/n): ").strip().lower()
            FILTER_ZONES = False if ans == "n" else True

            pct = input(f"Enter proximity % (default {DEFAULT_ZONE_PROXIMITY_PERCENT}): ").strip()
            if pct:
                ZONE_PROXIMITY_PERCENT = float(pct)
            else:
                ZONE_PROXIMITY_PERCENT = DEFAULT_ZONE_PROXIMITY_PERCENT

            show_console = input("Show console block output? (Y/n): ").strip().lower()
            SHOW_CONSOLE_OUTPUT = False if show_console == "n" else True
        except:
            FILTER_ZONES = DEFAULT_FILTER_ZONES
            ZONE_PROXIMITY_PERCENT = DEFAULT_ZONE_PROXIMITY_PERCENT
            SHOW_CONSOLE_OUTPUT = DEFAULT_SHOW_CONSOLE_OUTPUT
    else:
        expiry = pick_expiry_default(fo_map[first_symbol], DEFAULT_EXPIRY_MODE)
        FILTER_ZONES = DEFAULT_FILTER_ZONES
        ZONE_PROXIMITY_PERCENT = DEFAULT_ZONE_PROXIMITY_PERCENT
        SHOW_CONSOLE_OUTPUT = DEFAULT_SHOW_CONSOLE_OUTPUT

        print(f"\nUsing expiry: {expiry}")
        print(f"Using defaults → FILTER_ZONES={FILTER_ZONES}, "
              f"ZONE_PROXIMITY_PERCENT={ZONE_PROXIMITY_PERCENT}, "
              f"SHOW_CONSOLE_OUTPUT={SHOW_CONSOLE_OUTPUT}, "
              f"AUTO_NEXT_BATCH={DEFAULT_AUTO_NEXT_BATCH}\n")

    idx = 0
    sr = 1

    while idx < total:
        batch = symbols[idx: idx + BATCH_SIZE]
        print(f"\n=== Batch {idx // BATCH_SIZE + 1} ({len(batch)} symbols) ===")

        spot_keys = []
        ce_rows = {}
        pe_rows = {}

        for sym in batch:
            try:
                spot_key = get_spot_key(sym, eq_map)
                spot_keys.append(spot_key)
            except:
                pass

        ltp_spot_map = bulk_get_ltp(spot_keys)

        rows = []
        option_ltp_keys = set()
        symbol_prepared = []

        for sym in batch:
            try:
                spot_key = get_spot_key(sym, eq_map)
                spot = ltp_spot_map.get(spot_key)
                if spot is None:
                    continue

                fo_list = fo_map[sym]
                ce_row, pe_row = choose_atm(spot, fo_list, expiry)
                ce_rows[sym] = ce_row
                pe_rows[sym] = pe_row

                option_ltp_keys.add(ce_row["instrument_key"])
                option_ltp_keys.add(pe_row["instrument_key"])

                symbol_prepared.append((sym, spot, ce_row, pe_row))
            except Exception as e:
                print("Error:", e)

        option_ltp_map = bulk_get_ltp(option_ltp_keys)

        for sym, spot, ce_row, pe_row in symbol_prepared:
            try:
                ce_ltp = option_ltp_map.get(ce_row["instrument_key"])
                pe_ltp = option_ltp_map.get(pe_row["instrument_key"])

                rows.append([
                    sym,
                    round(spot, 2),
                    expiry.isoformat(),
                    ce_row["trading_symbol"],
                    round(ce_ltp, 2) if ce_ltp is not None else "N/A",
                    pe_row["trading_symbol"],
                    round(pe_ltp, 2) if pe_ltp is not None else "N/A",
                ])
            except Exception as e:
                print("Error:", e)

        sr = print_block(rows, sr, ce_rows, pe_rows)

        idx += BATCH_SIZE
        if idx >= total:
            print("\nAll batches complete.")
            break

        if USE_USER_INPUTS:
            cmd = input("\nType NEXT for next batch or EXIT: ").strip().upper()
            if cmd != "NEXT":
                break
        else:
            if not DEFAULT_AUTO_NEXT_BATCH:
                break

    if SHOW_CONSOLE_OUTPUT:
        print_sector_summary()

    write_html_report(run_id, generated_at_str, expiry)

    if SEND_TELEGRAM_HTML:
        send_html_to_telegram(run_id, generated_at_str, expiry)

# ======================================================
# RUN
# ======================================================
if __name__ == "__main__":
    main()