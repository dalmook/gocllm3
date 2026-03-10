import re
from typing import Optional

from app.sql_registry import SQLRegistryMatch


DOC_HINT_WORDS = [
    "이슈", "요약", "정리", "배경", "원인", "변경", "회의", "메일", "문서", "결정", "왜", "어떻게"
]

DOC_NAV_HINTS = [
    "문서 제목", "제목 알려", "문서 목록", "목록 알려", "무슨 문서", "어떤 문서",
    "최근 문서", "최신 문서", "금주 문서", "금주에 학습", "학습한 문서",
]

DOC_SUMMARY_TIME_HINTS = ["지난주", "이번주", "금주"]
DOC_SUMMARY_DOC_HINTS = ["주간 보고", "주간보고", "보고서", "문서"]
DOC_SUMMARY_ACTION_HINTS = ["정리해줘", "정리", "요약해줘", "요약"]


def normalize_question(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _contains_any(text: str, words: list[str]) -> bool:
    return any(w in text for w in words)


def _classify_doc_intent(question_lower: str, issue_summary_intent: bool) -> tuple[str, str]:
    compact = question_lower.replace(" ", "")

    nav_direct = _contains_any(question_lower, DOC_NAV_HINTS) or any(
        tok in compact for tok in ["문서제목", "문서목록", "최신문서", "최근문서", "무슨문서", "학습한문서"]
    )
    if nav_direct:
        return "doc_nav", "matched doc_nav keywords"

    has_time = _contains_any(question_lower, DOC_SUMMARY_TIME_HINTS)
    has_doc = _contains_any(question_lower, DOC_SUMMARY_DOC_HINTS)
    has_action = _contains_any(question_lower, DOC_SUMMARY_ACTION_HINTS)
    if (has_time and has_doc and has_action) or issue_summary_intent:
        reason = "matched weekly-summary pattern" if (has_time and has_doc and has_action) else "issue_summary_intent=true"
        return "doc_summary", reason

    return "", ""


def classify_query_intent(
    question: str,
    effective_question: str,
    sql_match: Optional[SQLRegistryMatch],
    *,
    prefer_general: bool,
    issue_summary_intent: bool = False,
    glossary_intent: bool = False,
) -> str:
    if prefer_general:
        print("[INTENT_RULE] general_llm reason=prefer_general")
        return "general_llm"

    q = normalize_question(effective_question or question).lower()
    doc_intent, doc_reason = _classify_doc_intent(q, issue_summary_intent)
    if doc_intent:
        print(f"[INTENT_RULE] {doc_intent} reason={doc_reason} q={q!r}")
        return doc_intent

    has_doc_need = issue_summary_intent or glossary_intent or any(word in q for word in DOC_HINT_WORDS)
    has_sql = bool(sql_match and sql_match.score >= 1.5)

    if has_sql and has_doc_need:
        print("[INTENT_RULE] hybrid reason=sql+doc_need")
        return "hybrid"
    if has_sql and not has_doc_need:
        print("[INTENT_RULE] data_only reason=sql_only")
        return "data_only"
    if has_doc_need:
        print("[INTENT_RULE] rag_only reason=doc_need")
        return "rag_only"
    print("[INTENT_RULE] rag_only reason=default")
    return "rag_only"
