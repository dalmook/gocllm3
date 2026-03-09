import tempfile
from pathlib import Path

import store


def test_query_log_and_feedback_linkage():
    with tempfile.TemporaryDirectory() as tmpdir:
        store.DB_PATH = str(Path(tmpdir) / "test.db")
        store.init_db()

        store.log_query_event(
            request_id="req-1",
            sender_knox="user1",
            sender_name="tester",
            chatroom_id="100",
            chat_type="SINGLE",
            raw_question="이번주 모바일 이슈",
            effective_question="이번주 모바일 주요 이슈",
            normalized_query="이번주 모바일 주요 이슈",
            detected_intent="rag_only",
            rag_used=1,
            success_flag=1,
            answer_preview="preview",
            debug_json={"weekly_issue_query": True},
        )

        row = store.get_query_log("req-1")
        assert row is not None
        assert row["request_id"] == "req-1"
        assert row["rag_used"] == 1

        fb_id = store.add_query_feedback(
            request_id="req-1",
            chatroom_id="100",
            sender_knox="user1",
            feedback_type="dislike",
            reason_code="stale_doc",
            memo="old doc",
            detected_intent="rag_only",
        )
        assert fb_id > 0

        summary = store.list_feedback_summary(days=30)
        assert any(item["request_id"] == "req-1" for item in summary)
