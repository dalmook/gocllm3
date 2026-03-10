from datetime import datetime

from app.search_improvement import (
    build_weekly_search_query_variants,
    compute_target_week_label,
    detect_weekly_issue_query,
    rerank_weekly_issue_docs,
)


def test_detect_weekly_issue_query():
    assert detect_weekly_issue_query("이번주 모바일 주요이슈 정리") is True
    assert detect_weekly_issue_query("지난주 주간 보고에서 1Q 생산 차질 Risk 정리해줘") is True
    assert detect_weekly_issue_query("모바일 담당자 알려줘") is False


def test_compute_target_week_label():
    now = datetime(2026, 3, 9)
    assert compute_target_week_label(now, "이번주 주요이슈") == "W11"
    assert compute_target_week_label(now, "지난주 주요이슈") == "W10"


def test_weekly_rerank_prefers_exact_week():
    docs = [
        {"title": "W09 Mobile 주요 이슈", "_combined_score": 0.8},
        {"title": "W11 Mobile 주요 이슈", "_combined_score": 0.6},
    ]
    reranked = rerank_weekly_issue_docs("이번주 모바일 주요이슈", docs, "W11", "MOBILE")
    assert reranked[0]["title"].startswith("W11")


def test_build_weekly_query_variants():
    q = build_weekly_search_query_variants("이번주 모바일 주요이슈", "W11", "MOBILE")
    assert any("W11" in item for item in q)
