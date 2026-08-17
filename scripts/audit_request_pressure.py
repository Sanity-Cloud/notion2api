from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.request_pressure import analyze_lines  # noqa: E402


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
