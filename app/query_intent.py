import re
from typing import Optional

from app.sql_registry import SQLRegistryMatch


DOC_HINT_WORDS = [
    "이슈", "요약", "정리", "배경", "원인", "변경", "회의", "메일", "문서", "결정", "왜", "어떻게"
]



def normalize_question(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())



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
        return "general_llm"

    q = normalize_question(effective_question or question).lower()
    has_doc_need = issue_summary_intent or glossary_intent or any(word in q for word in DOC_HINT_WORDS)
    has_sql = bool(sql_match and sql_match.score >= 1.5)

    if has_sql and has_doc_need:
        return "hybrid"
    if has_sql and not has_doc_need:
        return "data_only"
    if has_doc_need:
        return "rag_only"
    return "rag_only"
