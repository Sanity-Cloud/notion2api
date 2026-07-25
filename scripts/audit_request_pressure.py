from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.request_pressure import analyze_lines


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit Notion2API inference request pressure.")
    parser.add_argument("logs", nargs="+", type=Path)
    args = parser.parse_args()
    lines: list[str] = []
    for path in args.logs:
        lines.extend(path.read_text(encoding="utf-8", errors="replace").splitlines())
    print(json.dumps(analyze_lines(lines), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
