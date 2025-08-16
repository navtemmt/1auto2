import os
import pandas as pd

try:
    from tvDatafeed import TvDatafeed, Interval
except ModuleNotFoundError:
    os.system("pip install tradingview-datafeed pandas")
    from tvDatafeed import TvDatafeed, Interval


def fetch_and_update(symbol="NQ1!", exchange="CME_MINI",
                     interval=Interval.in_1_minute,
                     n_bars=500,
                     folder="data/nq_1m",
                     username=None, password=None):
    """
    Fetches OHLC data from TradingView and splits it into monthly CSVs.
    """

    # Connect
    tv = TvDatafeed(username=username, password=password)

    # Fetch latest bars
    df_new = tv.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars)
    if df_new is None or df_new.empty:
        print(f"⚠️ No new data fetched for {folder}.")
        return

    df_new.reset_index(inplace=True)  # datetime as column

    # Ensure folder exists
    os.makedirs(folder, exist_ok=True)

    # Process each month separately
    df_new["year_month"] = df_new["datetime"].dt.to_period("M")

    for ym, df_month in df_new.groupby("year_month"):
        file_path = os.path.join(folder, f"{ym}.csv")

        if os.path.exists(file_path):
            df_old = pd.read_csv(file_path, parse_dates=["datetime"])
            df_combined = pd.concat([df_old, df_month], ignore_index=True)
            df_combined.drop_duplicates(subset=["datetime"], inplace=True)
            df_combined.sort_values("datetime", inplace=True)
        else:
            df_combined = df_month

        df_combined.to_csv(file_path, index=False)
        print(f"✅ Updated {file_path} (rows: {len(df_combined)})")


if __name__ == "__main__":
    # 1m
    fetch_and_update(symbol="NQ1!", exchange="CME_MINI", interval=Interval.in_1_minute, folder="data/nq_1m")
    # 5m
    fetch_and_update(symbol="NQ1!", exchange="CME_MINI", interval=Interval.in_5_minute, folder="data/nq_5m")
