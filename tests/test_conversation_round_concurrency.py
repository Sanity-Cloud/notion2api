from concurrent.futures import ThreadPoolExecutor


def test_persist_round_allocates_unique_indices_across_connections(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "conversations.db"))

    from app.conversation import ConversationManager

    manager = ConversationManager()
    conversation_id = manager.new_conversation(
        title="Concurrent round allocation",
        conversation_id="conversation-concurrent-rounds",
    )

    def persist(index: int) -> int:
        return manager.persist_round(
            conversation_id,
            f"user-{index}",
            f"assistant-{index}",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        round_indices = list(pool.map(persist, [1, 2]))

    assert sorted(round_indices) == [0, 1]
    with manager._get_conn() as conn:
        next_round = conn.execute(
            "SELECT next_round_index FROM conversations WHERE id = ?",
            (conversation_id,),
        ).fetchone()["next_round_index"]
        stored_rounds = [
            row["round_number"]
            for row in conn.execute(
                "SELECT round_number FROM sliding_window "
                "WHERE conversation_id = ? ORDER BY round_number",
                (conversation_id,),
            ).fetchall()
        ]
        message_count = conn.execute(
            "SELECT COUNT(*) AS count FROM messages WHERE conversation_id = ?",
            (conversation_id,),
        ).fetchone()["count"]

    assert next_round == 2
    assert stored_rounds == [0, 1]
    assert message_count == 4
