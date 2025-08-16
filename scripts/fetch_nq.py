def check_data_quality(filename, interval="1min", log_file="run_log.txt"):
    import pandas as pd
    from pandas.tseries.frequencies import to_offset
    from datetime import datetime

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    with open(log_file, "a") as f:
        f.write(f"\n=== Run at {now} ===\n")
        f.write(f"=== Checking {filename} ===\n")

    if not os.path.exists(filename):
        with open(log_file, "a") as f:
            f.write("No data file found.\n")
        return

    df = pd.read_csv(filename, parse_dates=["datetime"])
    if df.empty:
        with open(log_file, "a") as f:
            f.write("File is empty.\n")
        return

    # Sort by datetime
    df.sort_values("datetime", inplace=True)

    # Build expected range of timestamps
    freq = "1min" if interval == "1min" else "5min"
    expected_range = pd.date_range(df["datetime"].min(), df["datetime"].max(), freq=to_offset(freq))
    actual_range = pd.to_datetime(df["datetime"])

    # Find missing
    missing = expected_range.difference(actual_range)

    # Duplicates
    duplicates = df.duplicated(subset=["datetime"]).sum()

    # Out-of-range (example: negative prices)
    out_of_range = ((df["open"] <= 0) | (df["high"] <= 0) | (df["low"] <= 0) | (df["close"] <= 0)).sum()

    # Log results
    with open(log_file, "a") as f:
        f.write(f"Missing bars: {len(missing)}\n")
        if len(missing) > 0:
            f.write(f"Missing timestamps:\n")
            for ts in missing[:20]:  # limit to first 20 so log doesn’t explode
                f.write(f"  {ts}\n")
            if len(missing) > 20:
                f.write(f"  ... ({len(missing)-20} more)\n")
        f.write(f"Duplicate rows: {duplicates}\n")
        f.write(f"Out-of-range prices: {out_of_range}\n")
        f.write(f"First row: {df.iloc[0].to_dict()}\n")
        f.write(f"Last row: {df.iloc[-1].to_dict()}\n")
