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


def fetch_and_update(symbol, exchange, interval, filename, username=None, password=None):
    """
    Fetches OHLC data from TradingView and appends it to a CSV file.
    """
    tv = TvDatafeed(username=username, password=password)
    df_new = tv.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=500)

    if df_new is None or df_new.empty:
        print(f"⚠️ No new data fetched for {symbol} -> {filename}.")
        return

    df_new.reset_index(inplace=True)  # move datetime to column

    if os.path.exists(filename):
        df_old = pd.read_csv(filename, parse_dates=["datetime"])
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
        df_combined.drop_duplicates(subset=["datetime"], inplace=True)
        df_combined.sort_values("datetime", inplace=True)
    else:
        df_combined = df_new

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    df_combined.to_csv(filename, index=False)

    print(f"✅ Updated {symbol} {interval} -> {filename} (rows: {len(df_combined)})")


if __name__ == "__main__":
    tasks = [
        # NQ1!
        ("NQ1!", "CME_MINI", Interval.in_1_minute, "data/nq_1m.csv"),
        ("NQ1!", "CME_MINI", Interval.in_5_minute, "data/nq_5m.csv"),

        # ES1! (only 1m)
        ("ES1!", "CME_MINI", Interval.in_1_minute, "data/es_1m.csv"),
    ]

    for symbol, exchange, interval, filename in tasks:
        fetch_and_update(symbol, exchange, interval, filename)
