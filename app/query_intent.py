import re
from typing import Any, Dict, Optional

from app.sql_registry import SQLRegistryMatch


DOC_HINT_WORDS = [
    "이슈", "요약", "정리", "배경", "원인", "이유", "변경", "회의", "메일", "문서", "결정", "왜", "어떻게",
]

DOC_STRONG_HINT_WORDS = [
    "관련", "같이", "보고", "공유", "영향", "리스크", "원인", "이유", "배경",
]

SQL_VALUE_HINT_WORDS = [
    "몇개", "얼마", "판매", "실적", "순생산", "순입고", "추이", "비교", "월별", "버전별", "기준",
]


def normalize_question(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _sql_signal_score(question: str, sql_match: Optional[SQLRegistryMatch], sql_trace: Optional[Dict[str, Any]]) -> float:
    q = normalize_question(question).lower()
    planner = dict((sql_trace or {}).get("planner_plan") or {})
    slots = dict((sql_trace or {}).get("final_slots") or (sql_trace or {}).get("slots") or {})
    versions = [str(v).strip() for v in (planner.get("versions") or slots.get("versions") or []) if str(v).strip()]
    filters = dict(planner.get("filters") or slots.get("filters") or {})
    periods = planner.get("periods") or slots.get("periods") or []
    period_groups = planner.get("period_groups") or slots.get("period_groups") or []
    analysis_type = str(planner.get("analysis_type") or slots.get("analysis_type") or "").strip().lower()

    score = 0.0
    if sql_match and sql_match.score >= 1.5:
        score += 2.0
    if slots.get("metric") or planner.get("metric"):
        score += 1.4
    if versions:
        score += 1.0
    if filters:
        score += 0.8
    if periods or period_groups:
        score += 0.9
    if analysis_type in {"trend", "compare", "grouped"}:
        score += 0.9
    if any(word in q for word in SQL_VALUE_HINT_WORDS):
        score += 0.7
    return score


def _doc_signal_score(question: str, *, issue_summary_intent: bool, glossary_intent: bool) -> float:
    q = normalize_question(question).lower()
    score = 0.0
    if issue_summary_intent or glossary_intent:
        score += 2.0
    score += 0.9 * sum(1 for word in DOC_HINT_WORDS if word in q)
    score += 0.5 * sum(1 for word in DOC_STRONG_HINT_WORDS if word in q)
    if "이번주" in q or "최근" in q:
        score += 0.4
    return score


def classify_query_intent(
    question: str,
    effective_question: str,
    sql_match: Optional[SQLRegistryMatch],
    *,
    prefer_general: bool,
    issue_summary_intent: bool = False,
    glossary_intent: bool = False,
    sql_trace: Optional[Dict[str, Any]] = None,
) -> str:
    q = normalize_question(effective_question or question).lower()
    sql_score = _sql_signal_score(q, sql_match, sql_trace)
    doc_score = _doc_signal_score(q, issue_summary_intent=issue_summary_intent, glossary_intent=glossary_intent)

    if prefer_general and sql_score < 1.5 and doc_score < 1.5:
        return "general_llm"
    if sql_score >= 2.2 and doc_score >= 1.2:
        return "hybrid"
    if sql_score >= 2.2 and doc_score < 1.4:
        return "data_only"
    if doc_score >= 1.2:
        return "rag_only"
    if sql_score >= 1.6:
        return "data_only"
    return "rag_only"
