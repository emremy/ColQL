import json
import sys
from pathlib import Path

REGRESSION_WARN = 3.0
REGRESSION_BLOCK = 7.0

def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def pct_delta(before: float, after: float) -> float:
    if before == 0:
        return 0.0
    return ((after - before) / before) * 100

def compare(before: dict, after: dict) -> list[dict]:
    rows = []
    for metric, before_value in before.items():
        if metric not in after:
            continue

        after_value = after[metric]
        delta = pct_delta(float(before_value), float(after_value))

        if delta >= REGRESSION_BLOCK:
            status = "block"
        elif delta >= REGRESSION_WARN:
            status = "warn"
        else:
            status = "ok"

        rows.append({
            "metric": metric,
            "before": before_value,
            "after": after_value,
            "delta_percent": round(delta, 2),
            "status": status,
        })
    return rows

def main():
    if len(sys.argv) != 3:
        print("usage: python compare_benchmarks.py before.json after.json")
        sys.exit(1)

    before = load_json(sys.argv[1])
    after = load_json(sys.argv[2])

    rows = compare(before, after)

    for row in rows:
        print(
            f"{row['status'].upper():5} "
            f"{row['metric']}: "
            f"{row['before']} -> {row['after']} "
            f"({row['delta_percent']}%)"
        )

    if any(row["status"] == "block" for row in rows):
        sys.exit(2)

if __name__ == "__main__":
    main()
