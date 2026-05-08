import json
import sys

FIELDS = ["heapUsed", "heapTotal", "rss", "external", "arrayBuffers"]

def load(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def mb(value):
    return round(value / 1024 / 1024, 2)

def main():
    if len(sys.argv) != 3:
        print("usage: python memory_snapshot_compare.py before.json after.json")
        sys.exit(1)

    before = load(sys.argv[1])
    after = load(sys.argv[2])

    for field in FIELDS:
        if field not in before or field not in after:
            continue

        b = before[field]
        a = after[field]
        delta = a - b
        pct = ((a - b) / b * 100) if b else 0

        print(
            f"{field}: "
            f"{mb(b)} MB -> {mb(a)} MB "
            f"delta={mb(delta)} MB ({pct:.2f}%)"
        )

if __name__ == "__main__":
    main()
