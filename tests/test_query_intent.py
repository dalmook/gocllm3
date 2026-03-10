from app.query_intent import classify_query_intent


def _classify(q: str, *, prefer_general: bool = False, issue_summary_intent: bool = False, glossary_intent: bool = False) -> str:
    return classify_query_intent(
        q,
        q,
        sql_match=None,
        prefer_general=prefer_general,
        issue_summary_intent=issue_summary_intent,
        glossary_intent=glossary_intent,
    )


def test_doc_nav_examples():
    assert _classify("최근 문서 제목 알려줘") == "doc_nav"
    assert _classify("금주에 학습한 문서 알려줘") == "doc_nav"


def test_doc_summary_examples():
    assert _classify("지난주 주간 보고에서 1Q 생산 차질 Risk에 대해 정리해줘") == "doc_summary"
    assert _classify("이번주 주간 보고 기준 1Q 생산 차질 Risk 정리해줘") == "doc_summary"


def test_general_llm_kept_for_chitchat_prefer_general():
    assert _classify("자냐?", prefer_general=True) == "general_llm"


def test_existing_fallback_kept_for_generic_doc_need():
    assert _classify("문서 배경 알려줘") == "rag_only"
