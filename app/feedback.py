from __future__ import annotations

from typing import Dict

FEEDBACK_TYPE_LIKE = "like"
FEEDBACK_TYPE_DISLIKE = "dislike"

REASON_LABELS: Dict[str, str] = {
    "stale_doc": "최신 문서 아님",
    "wrong_doc": "엉뚱한 문서 찾음",
    "should_use_sql": "SQL로 조회해야 했음",
    "vague_answer": "답이 모호함",
    "other": "기타",
}

VALID_REASON_CODES = set(REASON_LABELS.keys())


def normalize_feedback_type(raw: str) -> str:
    value = (raw or "").strip().lower()
    if value in (FEEDBACK_TYPE_LIKE, FEEDBACK_TYPE_DISLIKE):
        return value
    return FEEDBACK_TYPE_DISLIKE


def normalize_reason_code(raw: str) -> str:
    code = (raw or "").strip().lower()
    if code in VALID_REASON_CODES:
        return code
    return "other"
