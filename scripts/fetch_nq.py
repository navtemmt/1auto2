import os
import pandas as pd
from datetime import datetime
from tvDatafeed import TvDatafeed, Interval

# Init TradingView datafeed
tv = TvDatafeed()

# Folder structure
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

SYMBOLS = {
    "NQ1!": {"exchange": "CME_MINI", "intervals": [Interval.in_1_minute, Interval.in_5_minute]},
    "ES1!": {"exchange": "CME_MINI", "intervals": [Interval.in_1_minute]},
}

def fetch_and_update(symbol, exchange, interval):
    """Fetch new bars and append them into monthly CSV files."""
    print(f"=== Fetching {symbol} {interval} ===")
    df = tv.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=500)
    if df is None or df.empty:
        print(f"⚠️ No data for {symbol} {interval}")
        return

    df.reset_index(inplace=True)
    df["year_month"] = df["datetime"].dt.strftime("%Y-%m")

    # Save by month
    base_dir = os.path.join(DATA_DIR, f"{symbol}_{interval.name}")
    os.makedirs(base_dir, exist_ok=True)

    for month, chunk in df.groupby("year_month"):
        file_path = os.path.join(base_dir, f"{month}.csv")

        if os.path.exists(file_path):
            old = pd.read_csv(file_path, parse_dates=["datetime"])
            combined = pd.concat([old, chunk]).drop_duplicates(subset="datetime").sort_values("datetime")
        else:
            combined = chunk

        combined.to_csv(file_path, index=False)
        print(f"✅ Updated {file_path} ({len(chunk)} new rows)")

def main():
    for symbol, cfg in SYMBOLS.items():
        for interval in cfg["intervals"]:
            fetch_and_update(symbol, cfg["exchange"], interval)

    # Log run
    with open("run_log.txt", "a") as f:
        f.write(f"=== Run at {datetime.utcnow()} UTC ===\n")

if __name__ == "__main__":
    main()
