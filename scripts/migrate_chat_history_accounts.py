#!/usr/bin/env python3
"""Migrate the shared chat_history.db into per-account shards."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from app.chat_history.migrate import (  # noqa: E402
    migrate_shared_chat_history,
    resolve_migration_source,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Partition the monolithic Notion chat_history.db into "
            "data/chat_history/<safe_account_key>.db shards. Unattributed "
            "threads are quarantined into the legacy shard."
        )
    )
    parser.add_argument(
        "--source",
        default="",
        help="Optional path to the shared chat_history.db (default: env/legacy path)",
    )
    parser.add_argument(
        "--known-map",
        default="",
        help="Optional JSON object mapping thread_id -> account_key",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Attribute threads without writing shards",
    )
    parser.add_argument(
        "--move-source",
        action="store_true",
        help="Move the shared db to *.pre-account-migration instead of copying",
    )
    args = parser.parse_args()

    known: dict[str, str] = {}
    if args.known_map:
        payload = json.loads(Path(args.known_map).read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise SystemExit("--known-map must be a JSON object")
        known = {str(k): str(v) for k, v in payload.items()}

    source = args.source.strip() or str(resolve_migration_source())
    result = migrate_shared_chat_history(
        source_db=source,
        known_thread_accounts=known,
        dry_run=bool(args.dry_run),
        keep_source=not bool(args.move_source),
    )
    print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
