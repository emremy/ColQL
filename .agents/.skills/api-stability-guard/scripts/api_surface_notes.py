import re
from pathlib import Path

EXPORT_RE = re.compile(r"export\s+(?:class|function|type|interface|const|\{)")

def find_exports(src_dir: str) -> None:
    entrypoint = Path(src_dir) / "index.ts"
    if entrypoint.exists():
        print("# package entrypoint exports")
        for line_no, line in enumerate(entrypoint.read_text(encoding="utf-8", errors="ignore").splitlines(), start=1):
            if EXPORT_RE.search(line):
                print(f"{entrypoint}:{line_no}: {line.strip()}")
        print()

    print("# exported declarations in src")
    for path in Path(src_dir).rglob("*.ts"):
        if path == entrypoint:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for line_no, line in enumerate(text.splitlines(), start=1):
            if EXPORT_RE.search(line):
                print(f"{path}:{line_no}: {line.strip()}")

if __name__ == "__main__":
    find_exports("src")
