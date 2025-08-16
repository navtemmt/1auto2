import os
import pandas as pd
from datetime import datetime

def check_file(path):
    if not os.path.exists(path):
        return f"{path} not found.\n"

    df = pd.read_csv(path, parse_dates=["datetime"])

    report = [f"=== Checking {path} ==="]

    # Check for missing timestamps (only if datetime sorted)
    df = df.sort_values("datetime")
    if len(df) > 1:
        diffs = df["datetime"].diff().dropna().value_counts()
        if not diffs.empty:
            most_common = diffs.idxmax()
            missing = (df["datetime"].diff() > most_common).sum()
            report.append(f"Missing bars: {missing}")
        else:
            report.append("Missing bars: 0")
    else:
        report.append("Missing bars: (not enough data)")

    # Duplicates
    dup = df.duplicated(subset=["datetime"]).sum()
    report.append(f"Duplicate rows: {dup}")

    # Out-of-range checks
    if "high" in df and "low" in df and "close" in df:
        outliers = df[(df["high"] < df["low"]) | (df["close"] > df["high"]) | (df["close"] < df["low"])]
        report.append(f"Out-of-range prices: {len(outliers)}")

    # Sample rows
    if not df.empty:
        report.append(f"First row: {df.iloc[0].to_dict()}")
        report.append(f"Last row: {df.iloc[-1].to_dict()}")

    return "\n".join(report) + "\n\n"


if __name__ == "__main__":
    report_path = "quality_report.txt"
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    with open(report_path, "a") as f:   # <-- append mode
        f.write(f"=== Run at {timestamp} ===\n")
        f.write(check_file("data/nq_1m/2025-08.csv"))
        f.write(check_file("data/nq_5m/2025-08.csv"))
        f.write("\n")
