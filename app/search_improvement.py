from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any, Dict, List

WEEKLY_KEYWORDS = ("이번주", "금주", "지난주", "저번주", "전주", "주간", "W")
ISSUE_KEYWORDS = ("주요이슈", "이슈정리", "현황", "요약", "이슈", "주간보고", "주간 보고", "보고서")
TOPIC_KEYWORDS = ("mobile", "gfx", "flash", "hbm", "dram", "nand")


def _compact(text: str) -> str:
    return re.sub(r"\s+", "", (text or "").lower())


def detect_weekly_issue_query(question: str) -> bool:
    q = _compact(question)
    has_week = any(k in q for k in ("이번주", "금주", "지난주", "저번주", "전주")) or bool(re.search(r"w\s?\d{1,2}", q))
    has_issue = any(k in q for k in ISSUE_KEYWORDS)
    return has_week and has_issue


def detect_topic(question: str) -> str:
    q = (question or "").lower()
    for keyword in TOPIC_KEYWORDS:
        if keyword in q:
            return keyword.upper()
    return ""


def _week_label_from_datetime(dt: datetime) -> str:
    week = dt.isocalendar().week
    return f"W{week:02d}"


def _shift_week(dt: datetime, delta_week: int) -> datetime:
    return dt + timedelta(days=7 * delta_week)


def compute_target_week_label(now: datetime, question: str) -> str:
    q = _compact(question)
    explicit = re.search(r"w\s?(\d{1,2})", q)
    if explicit:
        return f"W{int(explicit.group(1)):02d}"
    if "지난주" in q or "저번주" in q or "전주" in q:
        return _week_label_from_datetime(_shift_week(now, -1))
    return _week_label_from_datetime(now)


def build_weekly_search_query_variants(question: str, week_label: str, topic: str) -> List[str]:
    variants: List[str] = []
    q = (question or "").strip()
    if not q:
        return variants

    variants.append(q)
    if week_label and week_label not in q:
        variants.append(f"{week_label} {q}")
    if topic:
        variants.append(f"{week_label} {topic} 주요 이슈")
        variants.append(f"{topic} 주간 이슈 {week_label}")
    variants.append(f"주간 이슈 {week_label}")

    out: List[str] = []
    for item in variants:
        item = re.sub(r"\s+", " ", item).strip()
        if item and item not in out:
            out.append(item)
    return out


def _text_of_doc(doc: Dict[str, Any]) -> str:
    parts = [
        str(doc.get("title") or ""),
        str(doc.get("_source", {}).get("title") or ""),
        str(doc.get("content") or ""),
        str(doc.get("_source", {}).get("content") or ""),
        str(doc.get("summary") or ""),
    ]
    return " ".join(parts).lower()


def _extract_week_tokens(text: str) -> List[str]:
    found = re.findall(r"w\s?(\d{1,2})", text.lower())
    return [f"W{int(x):02d}" for x in found]


def _week_num(label: str) -> int:
    try:
        return int((label or "").upper().replace("W", ""))
    except Exception:
        return 0


def rerank_weekly_issue_docs(question: str, docs: List[Dict[str, Any]], week_label: str, topic: str) -> List[Dict[str, Any]]:
    if not docs:
        return []

    target_num = _week_num(week_label)
    reranked: List[Dict[str, Any]] = []

    for doc in docs:
        item = dict(doc)
        title = f"{doc.get('title') or ''} {doc.get('_source', {}).get('title') or ''}".lower()
        text = _text_of_doc(doc)
        tokens = _extract_week_tokens(text)

        base = float(doc.get("_combined_score") or doc.get("_score") or 0.0)
        bonus = 0.0
        reasons: List[str] = []

        if week_label and week_label.lower() in title:
            bonus += 0.35
            reasons.append("title_week_exact")

        if topic and topic.lower() in title:
            bonus += 0.15
            reasons.append("title_topic_exact")

        other_weeks = [tok for tok in tokens if tok != week_label]
        if other_weeks:
            bonus -= 0.20
            reasons.append("other_week_penalty")

        if target_num and tokens:
            diffs = [abs(_week_num(tok) - target_num) for tok in tokens if _week_num(tok) > 0]
            if diffs:
                min_diff = min(diffs)
                if min_diff == 0:
                    bonus += 0.15
                    reasons.append("week_distance_0")
                elif min_diff <= 2:
                    bonus += 0.05
                    reasons.append("week_distance_near")
                else:
                    bonus -= 0.15
                    reasons.append("week_distance_far")

        item["_weekly_score"] = round(base + bonus, 4)
        item["_weekly_reason"] = ",".join(reasons) if reasons else "base"
        item["_weekly_exact_week_match"] = int(week_label in tokens or week_label.lower() in title)
        reranked.append(item)

    reranked.sort(
        key=lambda x: (
            float(x.get("_weekly_score") or 0.0),
            float(x.get("_combined_score") or x.get("_score") or 0.0),
        ),
        reverse=True,
    )
    return reranked


def summarize_rerank_reason(doc: Dict[str, Any]) -> str:
    reason = str(doc.get("_weekly_reason") or "base")
    return reason[:200]


def extract_week_tokens_from_doc(doc: Dict[str, Any]) -> List[str]:
    return _extract_week_tokens(_text_of_doc(doc))
