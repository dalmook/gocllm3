from typing import List

import pandas as pd



def _to_scalar(v) -> str:
    if v is None:
        return "-"
    return str(v)



def summarize_sql_result(df: pd.DataFrame, *, max_rows: int = 5) -> dict:
    if df is None or df.empty:
        return {
            "summary": "조회 결과가 없습니다.",
            "bullets": ["조회 결과가 없습니다."],
            "rows": 0,
        }

    bullets: List[str] = []
    head = df.head(max_rows)
    cols = list(head.columns)
    for _, row in head.iterrows():
        pairs = [f"{c}={_to_scalar(row.get(c))}" for c in cols[:4]]
        bullets.append("- " + ", ".join(pairs))

    summary = f"총 {len(df)}건 조회되었습니다."
    return {"summary": summary, "bullets": bullets, "rows": len(df)}



def build_data_only_answer(sql_summary: dict) -> str:
    lines = [
        "📌 한줄 요약",
        f"- {sql_summary.get('summary', '조회 결과를 확인했습니다.')}",
        "",
        "📊 데이터 기반 답변",
    ]
    bullets = sql_summary.get("bullets") or ["- 조회 결과가 없습니다."]
    lines.extend(bullets)
    lines.extend(
        [
            "",
            "💡 참고",
            "- SQL 기준 시점과 다른 시스템 반영 시점은 차이가 날 수 있습니다.",
            "",
            "🔗 이슈지 바로가기 👉 https://go/issueG",
        ]
    )
    return "\n".join(lines)



def build_hybrid_prompt(question: str, sql_summary_text: str, rag_context: str) -> str:
    return f"""
당신은 GOC 업무 지원 챗봇입니다.
질문에 대해 SQL 결과와 문서 근거를 함께 반영해 답하세요.

[질문]
{question}

[SQL 요약]
{sql_summary_text}

[RAG 문서]
{rag_context}

출력 형식
📌 한줄 요약
- 핵심 결론 1문장

📊 데이터 기반 답변
- SQL 결과 기반 핵심 수치/건수/기간 2~5개

📂 문서 기반 보강
- 관련 이슈/배경/변경사항 1~4개
- 문서 없으면 \"관련 문서를 찾지 못했습니다.\"

💡 참고
- SQL과 문서의 기준 시점이 다를 수 있으면 명시

🔗 이슈지 바로가기 👉 https://go/issueG
""".strip()



def build_hybrid_fallback_answer(sql_summary: dict, *, rag_found: bool) -> str:
    lines = [
        "📌 한줄 요약",
        f"- {sql_summary.get('summary', '조회 결과를 확인했습니다.')}",
        "",
        "📊 데이터 기반 답변",
    ]
    lines.extend(sql_summary.get("bullets") or ["- 조회 결과가 없습니다."])
    lines.extend(["", "📂 문서 기반 보강"])
    if rag_found:
        lines.append("- 문서가 있었지만 질문과의 관련성이 낮았습니다.")
    else:
        lines.append("- 관련 문서를 찾지 못했습니다.")
    lines.extend(
        [
            "",
            "💡 참고",
            "- SQL과 문서의 기준 시점이 다를 수 있습니다.",
            "",
            "🔗 이슈지 바로가기 👉 https://go/issueG",
        ]
    )
    return "\n".join(lines)
