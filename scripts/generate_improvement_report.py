#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import store
from app.improvement_engine import build_improvement_report


def _render_top_section(title: str, rows: List[Tuple[str, int]]) -> str:
    out = [f"## {title}"]
    if not rows:
        out.append("- 없음")
        return "\n".join(out)
    for q, cnt in rows:
        out.append(f"- ({cnt}) {q}")
    return "\n".join(out)


def _render_candidates(title: str, rows: List[Dict[str, Any]]) -> str:
    out = [f"## {title}"]
    if not rows:
        out.append("- 없음")
        return "\n".join(out)
    for c in rows:
        out.append(
            "- "
            f"[{c.get('candidate_type')}] pattern={c.get('source_pattern')} | "
            f"evidence={c.get('evidence_count')} | confidence={c.get('confidence_score')} | "
            f"suggestion={c.get('suggested_change')}"
        )
    return "\n".join(out)


def render_markdown(report: Dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    sections = [
        f"# Improvement Report ({report.get('generated_at')})",
        "## 1. 질문량 / like / dislike 요약",
        f"- 기간: 최근 {report.get('days')}일",
        f"- 질문 수: {summary.get('query_count', 0)}",
        f"- 피드백 수: {summary.get('feedback_count', 0)}",
        f"- like: {summary.get('like_count', 0)}",
        f"- dislike: {summary.get('dislike_count', 0)}",
        f"- 생성 후보 수: {summary.get('candidate_count', 0)}",
        _render_top_section("2. fallback 많은 질문 TOP 10", report.get("top_fallback_questions") or []),
        _render_top_section("3. weekly issue 실패 TOP 10", report.get("top_weekly_fail_questions") or []),
        _render_top_section("4. SQL intent miss TOP 10", report.get("top_sql_intent_miss_questions") or []),
        _render_top_section("5. glossary miss TOP 10", report.get("top_glossary_miss_questions") or []),
        _render_candidates("6. 신규 alias 후보", report.get("alias_candidates") or []),
        _render_candidates("7. 신규 SQL registry 후보", report.get("sql_registry_candidates") or []),
        _render_candidates("8. 신규 rerank rule 후보", report.get("rerank_rule_candidates") or []),
    ]
    return "\n\n".join(sections) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate improvement report from query logs/feedback")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--out-dir", type=str, default="reports")
    parser.add_argument("--no-insert", action="store_true", help="Do not insert improvement candidates")
    args = parser.parse_args()

    store.init_db()
    report = build_improvement_report(days=args.days, insert_candidates=not args.no_insert)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    json_path = out_dir / f"improvement_report_{stamp}.json"
    md_path = out_dir / f"improvement_report_{stamp}.md"

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    with md_path.open("w", encoding="utf-8") as f:
        f.write(render_markdown(report))

    store.add_improvement_run(
        period_days=args.days,
        total_logs=int((report.get("summary") or {}).get("query_count") or 0),
        total_feedback=int((report.get("summary") or {}).get("feedback_count") or 0),
        generated_count=int((report.get("summary") or {}).get("candidate_count") or 0),
        report_json_path=str(json_path),
        report_md_path=str(md_path),
    )

    push_enabled = os.getenv("ENABLE_IMPROVEMENT_REPORT_PUSH", "false").lower() == "true"
    if push_enabled:
        print("[IMPROVEMENT] ENABLE_IMPROVEMENT_REPORT_PUSH=true 이지만 스크립트 모드는 파일 생성만 수행합니다.")

    print(f"[IMPROVEMENT] report_json={json_path}")
    print(f"[IMPROVEMENT] report_md={md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
