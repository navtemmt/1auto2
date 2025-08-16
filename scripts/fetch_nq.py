import os
import pandas as pd
from datetime import datetime

# Ensure tradingview-datafeed is installed
try:
    from tvDatafeed import TvDatafeed, Interval
except ModuleNotFoundError:
    os.system("pip install tradingview-datafeed")
    from tvDatafeed import TvDatafeed, Interval


def log_run(message):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    with open("run_log.txt", "a") as f:   # append mode
        f.write(f"[{ts}] {message}\n")


def fetch_and_update(symbol="NQ1!", exchange="CME_MINI", 
                     interval=Interval.in_1_minute, 
                     n_bars=500,
                     folder="data/nq_1m",
                     username=None, password=None):
    """
    Fetches OHLC data from TradingView and stores it in monthly CSV files.
    """

    # Connect to TradingView
    tv = TvDatafeed(username=username, password=password)

    # Fetch latest bars
    df_new = tv.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars)

    if df_new is None or df_new.empty:
        log_run(f"⚠️ No new data fetched for {folder}")
        return

    df_new.reset_index(inplace=True)  # move datetime to column
    df_new["month"] = df_new["datetime"].dt.to_period("M")

    # Ensure folder exists
    os.makedirs(folder, exist_ok=True)

    # Split by month and save
    for month, df_month in df_new.groupby("month"):
        month_str = str(month)  # e.g., "2025-08"
        filename = os.path.join(folder, f"{month_str}.csv")

        if os.path.exists(filename):
            df_old = pd.read_csv(filename, parse_dates=["datetime"])
            df_combined = pd.concat([df_old, df_month], ignore_index=True)
            df_combined.drop_duplicates(subset=["datetime"], inplace=True)
            df_combined.sort_values("datetime", inplace=True)
        else:
            df_combined = df_month

        df_combined.to_csv(filename, index=False)
        log_run(f"✅ Updated {filename} (rows: {len(df_combined)})")


if __name__ == "__main__":
    try:
        # 🔹 NQ 1-minute
        fetch_and_update(symbol="NQ1!", exchange="CME_MINI", 
                         interval=Interval.in_1_minute, 
                         folder="data/nq_1m")

        # 🔹 NQ 5-minute
        fetch_and_update(symbol="NQ1!", exchange="CME_MINI", 
                         interval=Interval.in_5_minute, 
                         folder="data/nq_5m")

        log_run("✅ Data fetch successful")
    except Exception as e:
        log_run(f"❌ Error: {e}")
