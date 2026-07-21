from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from app.chat_history.store import ChatHistoryStore, MODEL_METADATA_COLUMNS  # noqa: E402

ENV_FILE = REPO / ".env"
DEFAULT_DB = REPO / "data" / "chat_history.db"


def parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


def resolve_chat_history_db(env: dict[str, str]) -> Path:
    explicit = env.get("CHAT_HISTORY_DB_PATH")
    if explicit:
        return Path(explicit)
    main_db = env.get("DB_PATH")
    if main_db:
        return Path(main_db).expanduser().resolve().parent / "chat_history.db"
    return DEFAULT_DB


def read_chat_message_columns(db_path: Path) -> set[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        return {str(row[1]) for row in conn.execute("PRAGMA table_info(chat_messages)").fetchall()}
    finally:
        conn.close()


def main() -> int:
    env = parse_env_file(ENV_FILE)
    db_path = resolve_chat_history_db(env)
    print(f"chat_history_db={db_path}")

    if not db_path.exists():
        print("chat_history.db does not exist yet; no migration needed until history is imported.")
        return 0

    before = read_chat_message_columns(db_path)

    ChatHistoryStore(str(db_path))

    final_cols = read_chat_message_columns(db_path)

    added = sorted(set(MODEL_METADATA_COLUMNS) - before)
    print("added=" + (",".join(added) if added else "<none>"))
    print("model_columns=" + ",".join(sorted(set(MODEL_METADATA_COLUMNS) & final_cols)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
