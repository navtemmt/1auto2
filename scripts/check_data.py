import os
import pandas as pd

def check_file(filename, freq="1min", price_range=(1000, 30000)):
    """
    Check data quality for OHLC file.
    """
    if not os.path.exists(filename):
        return f"❌ {filename} not found.\n"

    df = pd.read_csv(filename, parse_dates=["datetime"])
    df = df.sort_values("datetime")

    report = []
    report.append(f"=== Checking {filename} ===")

    # 1. Missing bars
    expected = pd.date_range(df["datetime"].min(), df["datetime"].max(), freq=freq)
    missing = expected.difference(df["datetime"])
    report.append(f"Missing bars: {len(missing)}")

    # 2. Duplicates
    dupes = df[df.duplicated("datetime")]
    report.append(f"Duplicate rows: {len(dupes)}")

    # 3. Outliers
    min_price, max_price = df[["open","high","low","close"]].min().min(), df[["open","high","low","close"]].max().max()
    outliers = df[(df["close"] < price_range[0]) | (df["close"] > price_range[1])]
    report.append(f"Out-of-range prices: {len(outliers)} "
                  f"(min={min_price}, max={max_price})")

    # 4. Sample preview
    report.append(f"First row: {df.head(1).to_dict('records')[0]}")
    report.append(f"Last row: {df.tail(1).to_dict('records')[0]}")

    return "\n".join(report) + "\n\n"


if __name__ == "__main__":
    report = ""
    report += check_file("data/nq_1m/2025-08.csv", freq="1min")
    report += check_file("data/nq_5m/2025-08.csv", freq="5min")

    with open("quality_report.txt", "a") as f:
        f.write(report)

    print("✅ Quality check done. Results saved to quality_report.txt")
