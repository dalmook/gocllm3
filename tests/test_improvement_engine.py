import tempfile
from pathlib import Path

import store
from app.improvement_engine import build_improvement_report


def test_build_improvement_report_generates_candidates():
    with tempfile.TemporaryDirectory() as tmpdir:
        store.DB_PATH = str(Path(tmpdir) / "test.db")
        store.init_db()

        for idx in range(4):
            req_id = f"req-{idx}"
            store.log_query_event(
                request_id=req_id,
                sender_knox="u1",
                chatroom_id="1",
                chat_type="SINGLE",
                raw_question="이번주 모바일 이슈 요약",
                effective_question="이번주 모바일 이슈 요약",
                normalized_query="이번주 모바일 이슈 요약",
                detected_intent="rag_only",
                sql_used=0,
                rag_used=1,
                rag_selected_domain="mail",
                fallback_reason="",
                answer_preview="preview",
                success_flag=1,
                debug_json={"weekly_issue_query": True, "exact_week_match_count": 0, "detected_topic": "MOBILE"},
            )
            store.add_query_feedback(
                request_id=req_id,
                chatroom_id="1",
                sender_knox="u1",
                feedback_type="dislike",
                reason_code="stale_doc",
                memo="",
                detected_intent="rag_only",
            )

        report = build_improvement_report(days=30, insert_candidates=True)
        assert report["summary"]["candidate_count"] >= 1

        candidates = store.list_improvement_candidates(status="new")
        assert len(candidates) >= 1
