from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List

import store


def _tokenize(question: str) -> List[str]:
    text = (question or "").strip().lower()
    for ch in [",", ".", "?", "!", "(", ")", "[", "]", ":", ";", "\n", "\t", "-"]:
        text = text.replace(ch, " ")
    return [tok for tok in text.split() if len(tok) >= 2]


def _question_key(question: str) -> str:
    tokens = _tokenize(question)
    if not tokens:
        return ""
    return " ".join(tokens[:6])


def analyze_failed_queries(days: int = 7) -> Dict[str, Any]:
    logs = store.list_query_logs_recent(days=days, limit=5000)
    feedback = store.list_feedback_recent(days=days, limit=5000)

    fb_by_request: Dict[str, List[dict]] = defaultdict(list)
    for fb in feedback:
        rid = str(fb.get("request_id") or "")
        if rid:
            fb_by_request[rid].append(fb)

    analyzed: List[dict] = []
    for log in logs:
        rid = str(log.get("request_id") or "")
        feedback_rows = fb_by_request.get(rid, [])
        dislikes = sum(1 for f in feedback_rows if (f.get("feedback_type") or "") == "dislike")
        likes = sum(1 for f in feedback_rows if (f.get("feedback_type") or "") == "like")
        analyzed.append(
            {
                "log": log,
                "likes": likes,
                "dislikes": dislikes,
                "feedback_rows": feedback_rows,
                "failed": bool(log.get("success_flag") == 0 or dislikes > likes or log.get("fallback_reason")),
            }
        )

    return {"logs": logs, "feedback": feedback, "analyzed": analyzed}


def detect_alias_candidates(analyzed_rows: List[dict]) -> List[dict]:
    groups: Dict[str, List[dict]] = defaultdict(list)
    for row in analyzed_rows:
        q = str(row["log"].get("effective_question") or row["log"].get("raw_question") or "")
        key = _question_key(q)
        if key:
            groups[key].append(row)

    out: List[dict] = []
    for key, rows in groups.items():
        if len(rows) < 3:
            continue
        dislikes = sum(r.get("dislikes", 0) for r in rows)
        if dislikes < 2:
            continue
        sample = str(rows[0]["log"].get("effective_question") or rows[0]["log"].get("raw_question") or "")
        out.append(
            {
                "candidate_type": "alias",
                "source_pattern": key,
                "suggested_change": f"alias 후보: '{sample}' 표현을 registry alias로 등록 검토",
                "evidence_count": len(rows),
                "confidence_score": min(0.95, 0.45 + (len(rows) * 0.05) + (dislikes * 0.03)),
                "notes": f"dislikes={dislikes}",
            }
        )
    return out


def detect_sql_registry_candidates(analyzed_rows: List[dict]) -> List[dict]:
    quantity_words = ("몇개", "수량", "판매", "출하", "실적")
    grouped: Dict[str, List[dict]] = defaultdict(list)
    for row in analyzed_rows:
        log = row["log"]
        q = str(log.get("effective_question") or "")
        if not any(w in q for w in quantity_words):
            continue
        if log.get("sql_used"):
            continue
        if row.get("dislikes", 0) <= 0:
            continue
        grouped[_question_key(q)].append(row)

    out: List[dict] = []
    for key, rows in grouped.items():
        if not key or len(rows) < 2:
            continue
        out.append(
            {
                "candidate_type": "sql_registry",
                "source_pattern": key,
                "suggested_change": "SQL registry 신규 query 또는 synonym slot 추가 검토",
                "evidence_count": len(rows),
                "confidence_score": min(0.95, 0.5 + len(rows) * 0.08),
                "notes": "sql_match miss + dislike",
            }
        )
    return out


def detect_weekly_rag_candidates(analyzed_rows: List[dict]) -> List[dict]:
    bucket: Dict[str, List[dict]] = defaultdict(list)
    for row in analyzed_rows:
        log = row["log"]
        try:
            debug = json.loads(log.get("debug_json") or "{}")
        except Exception:
            debug = {}
        if not debug.get("weekly_issue_query"):
            continue
        dislike_reasons = [
            str(f.get("reason_code") or "") for f in row.get("feedback_rows") or [] if (f.get("feedback_type") or "") == "dislike"
        ]
        if "stale_doc" not in dislike_reasons and int(debug.get("exact_week_match_count") or 0) > 0:
            continue
        key = str(debug.get("detected_topic") or "WEEKLY")
        bucket[key].append(row)

    out: List[dict] = []
    for key, rows in bucket.items():
        if len(rows) < 2:
            continue
        out.append(
            {
                "candidate_type": "weekly_rule",
                "source_pattern": key,
                "suggested_change": f"{key} 주간 문서 week exact-match 가중치 상향 및 fallback 규칙 조정 검토",
                "evidence_count": len(rows),
                "confidence_score": min(0.95, 0.55 + len(rows) * 0.06),
                "notes": "weekly miss / stale_doc 반복",
            }
        )
    return out


def detect_glossary_candidates(analyzed_rows: List[dict]) -> List[dict]:
    terms = Counter()
    for row in analyzed_rows:
        log = row["log"]
        if str(log.get("rag_selected_domain") or "") == "glossary":
            continue
        if not str(log.get("fallback_reason") or ""):
            continue
        q = str(log.get("effective_question") or "")
        for tok in _tokenize(q):
            if len(tok) >= 3:
                terms[tok] += 1

    out: List[dict] = []
    for term, count in terms.most_common(10):
        if count < 3:
            continue
        out.append(
            {
                "candidate_type": "glossary",
                "source_pattern": term,
                "suggested_change": f"용어사전 후보: '{term}' 정의 문서 추가 검토",
                "evidence_count": count,
                "confidence_score": min(0.9, 0.4 + count * 0.05),
                "notes": "fallback 반복 질의",
            }
        )
    return out


def detect_rerank_candidates(analyzed_rows: List[dict]) -> List[dict]:
    domain_miss = Counter()
    for row in analyzed_rows:
        log = row["log"]
        if not row.get("dislikes", 0):
            continue
        domain = str(log.get("rag_selected_domain") or "none")
        if domain == "none":
            continue
        domain_miss[domain] += 1

    out: List[dict] = []
    for domain, count in domain_miss.items():
        if count < 3:
            continue
        out.append(
            {
                "candidate_type": "rerank_rule",
                "source_pattern": domain,
                "suggested_change": f"{domain} 도메인 제목 exact-match 가중치/패널티 규칙 조정 검토",
                "evidence_count": count,
                "confidence_score": min(0.9, 0.45 + count * 0.06),
                "notes": "domain dislike 집중",
            }
        )
    return out


def _insert_candidates(candidates: List[dict]) -> List[int]:
    inserted: List[int] = []
    for candidate in candidates:
        cid = store.add_improvement_candidate(
            candidate_type=str(candidate["candidate_type"]),
            source_pattern=str(candidate["source_pattern"]),
            suggested_change=str(candidate["suggested_change"]),
            evidence_count=int(candidate.get("evidence_count") or 0),
            confidence_score=float(candidate.get("confidence_score") or 0.0),
            status="new",
            notes=str(candidate.get("notes") or ""),
        )
        inserted.append(cid)
        print(
            f"[IMPROVEMENT] candidate_type={candidate['candidate_type']} "
            f"evidence_count={candidate.get('evidence_count', 0)} id={cid}"
        )
    return inserted


def build_improvement_report(days: int = 7, insert_candidates: bool = True) -> Dict[str, Any]:
    analyzed = analyze_failed_queries(days=days)
    rows = analyzed["analyzed"]
    logs = analyzed["logs"]
    feedback = analyzed["feedback"]

    alias_candidates = detect_alias_candidates(rows)
    sql_candidates = detect_sql_registry_candidates(rows)
    weekly_candidates = detect_weekly_rag_candidates(rows)
    glossary_candidates = detect_glossary_candidates(rows)
    rerank_candidates = detect_rerank_candidates(rows)

    all_candidates = alias_candidates + sql_candidates + weekly_candidates + glossary_candidates + rerank_candidates
    inserted_ids: List[int] = []
    if insert_candidates and all_candidates:
        inserted_ids = _insert_candidates(all_candidates)

    fallback_counter = Counter()
    weekly_fail_counter = Counter()
    sql_miss_counter = Counter()
    glossary_miss_counter = Counter()
    dislike_counter = Counter()

    for row in rows:
        log = row["log"]
        q = str(log.get("effective_question") or log.get("raw_question") or "")
        if row.get("dislikes", 0):
            dislike_counter[q] += row["dislikes"]
        if str(log.get("fallback_reason") or ""):
            fallback_counter[q] += 1
        if str(log.get("sql_used") or "0") == "0" and any(w in q for w in ("수량", "판매", "출하", "실적", "몇개")):
            sql_miss_counter[q] += 1
        try:
            dbg = json.loads(log.get("debug_json") or "{}")
        except Exception:
            dbg = {}
        if dbg.get("weekly_issue_query") and int(dbg.get("exact_week_match_count") or 0) == 0:
            weekly_fail_counter[q] += 1
        if str(log.get("rag_selected_domain") or "") == "none" and str(log.get("fallback_reason") or ""):
            glossary_miss_counter[q] += 1

    like_count = sum(1 for f in feedback if (f.get("feedback_type") or "") == "like")
    dislike_count = sum(1 for f in feedback if (f.get("feedback_type") or "") == "dislike")

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "days": days,
        "summary": {
            "query_count": len(logs),
            "feedback_count": len(feedback),
            "like_count": like_count,
            "dislike_count": dislike_count,
            "candidate_count": len(all_candidates),
            "inserted_ids": inserted_ids,
        },
        "top_dislike_questions": dislike_counter.most_common(10),
        "top_fallback_questions": fallback_counter.most_common(10),
        "top_weekly_fail_questions": weekly_fail_counter.most_common(10),
        "top_sql_intent_miss_questions": sql_miss_counter.most_common(10),
        "top_glossary_miss_questions": glossary_miss_counter.most_common(10),
        "alias_candidates": alias_candidates,
        "sql_registry_candidates": sql_candidates,
        "weekly_rag_candidates": weekly_candidates,
        "glossary_candidates": glossary_candidates,
        "rerank_rule_candidates": rerank_candidates,
    }
