"""Activate the additive chat-history schema for current governed account shards."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.account_scope import canonical_account_key  # noqa: E402
from app.chat_history.schema_activation import (  # noqa: E402
    activate_shard,
    write_activation_receipt,
)
from app.chat_history.store import get_account_chat_history_db_path  # noqa: E402
from app.config import get_governed_accounts  # noqa: E402


def _governed_shards() -> list[Path]:
    paths: set[Path] = set()
    for account in get_governed_accounts():
        workspace_id = str(account.get("space_id") or account.get("workspace_id") or "")
        notion_user_id = str(account.get("user_id") or account.get("notion_user_id") or "")
        account_key = canonical_account_key(workspace_id, notion_user_id)
        paths.add(Path(get_account_chat_history_db_path(account_key)).resolve())
    return sorted(paths)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup-dir", required=True)
    parser.add_argument("--receipt", required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument(
        "--chat-history-db-dir",
        help="Explicit runtime shard root; overrides CHAT_HISTORY_DB_DIR for target resolution.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    args = parser.parse_args()

    current_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True
    ).strip()
    if args.expected_commit != current_commit:
        raise RuntimeError("expected commit does not match the checked-out repository HEAD")
    worktree_status = subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=REPO_ROOT,
        text=True,
    )
    if worktree_status.strip():
        raise RuntimeError("schema activation requires a clean repository worktree")
    if args.chat_history_db_dir:
        os.environ["CHAT_HISTORY_DB_DIR"] = str(Path(args.chat_history_db_dir).resolve())

    targets = _governed_shards()
    if not targets:
        raise RuntimeError("no governed account shards resolved")
    receipts = []
    receipt_args = {
        "receipt_path": args.receipt,
        "expected_commit": current_commit,
        "expected_shard_count": len(targets),
    }
    write_activation_receipt(receipts, status="in_progress", **receipt_args)
    try:
        for target in targets:
            receipts.append(
                activate_shard(
                    target,
                    backup_dir=args.backup_dir,
                    timeout_seconds=args.timeout_seconds,
                )
            )
            write_activation_receipt(receipts, status="in_progress", **receipt_args)
    except Exception as exc:
        write_activation_receipt(
            receipts,
            status="failed",
            error_type=type(exc).__name__,
            **receipt_args,
        )
        raise
    receipt_path = write_activation_receipt(
        receipts,
        status="completed",
        **receipt_args,
    )
    print(receipt_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
