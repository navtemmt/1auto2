import os
import pandas as pd

# Ensure pandas is installed
try:
    import pandas as pd
except ModuleNotFoundError:
    os.system("pip install pandas")
    import pandas as pd

# Ensure tradingview-datafeed is installed
try:
    from tvDatafeed import TvDatafeed, Interval
except ModuleNotFoundError:
    os.system("pip install tradingview-datafeed")
    from tvDatafeed import TvDatafeed, Interval


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
        print(f"⚠️ No new data fetched for {folder}.")
        return

    df_new.reset_index(inplace=True)  # move datetime to column
    df_new["year_month"] = df_new["datetime"].dt.strftime("%Y-%m")

    # Save per month
    for ym, df_month in df_new.groupby("year_month"):
        filename = os.path.join(folder, f"{ym}.csv")
        os.makedirs(folder, exist_ok=True)

        if os.path.exists(filename):
            df_old = pd.read_csv(filename, parse_dates=["datetime"])
            df_combined = pd.concat([df_old, df_month], ignore_index=True)
            df_combined.drop_duplicates(subset=["datetime"], inplace=True)
            df_combined.sort_values("datetime", inplace=True)
        else:
            df_combined = df_month

        df_combined.to_csv(filename, index=False)
        print(f"✅ Updated {filename} (rows: {len(df_combined)})")


if __name__ == "__main__":
    # 🔹 Fetch NQ 1-minute
    fetch_and_update(symbol="NQ1!", exchange="CME_MINI", interval=Interval.in_1_minute, folder="data/nq_1m")

    # 🔹 Fetch NQ 5-minute
    fetch_and_update(symbol="NQ1!", exchange="CME_MINI", interval=Interval.in_5_minute, folder="data/nq_5m")
