from typing import Any, Dict, List, Optional

try:
    import pandas as pd
except Exception:  # pragma: no cover - local test shim
    class _PandasShim:
        class DataFrame:  # type: ignore[override]
            pass

        @staticmethod
        def isna(_value) -> bool:
            return False

    pd = _PandasShim()  # type: ignore[assignment]


def _resolve_field_name(df: pd.DataFrame, field: str) -> str:
    if not field:
        return ""
    if field in df.columns:
        return field
    wanted = str(field).strip().lower()
    for col in df.columns:
        if str(col).strip().lower() == wanted:
            return str(col)
    return ""


def _metric_label(metric: str) -> str:
    return {
        "sales": "판매",
        "net_prod": "순생산",
        "net_ipgo": "순입고",
    }.get(str(metric or "").strip(), str(metric or "").strip())


def _version_hint(context: Optional[dict]) -> str:
    slots = (context or {}).get("slots") or {}
    versions = [str(v).strip().upper() for v in (slots.get("versions") or []) if str(v).strip()]
    if versions:
        return ", ".join(versions)
    version = str(slots.get("version") or "").strip().upper()
    return version or "전체"



def _to_scalar(v) -> str:
    if v is None:
        return "-"
    try:
        if pd.isna(v):
            return "-"
    except Exception:
        pass
    return str(v)



def _is_missing(v) -> bool:
    if v is None:
        return True
    try:
        if pd.isna(v):
            return True
    except Exception:
        pass
    if isinstance(v, str) and not v.strip():
        return True
    return False


def summarize_sql_result(
    df: pd.DataFrame,
    *,
    max_rows: int = 5,
    result_mode: str = "table",
    result_field: str = "",
    empty_message: str = "조회 결과가 없습니다.",
    context: Optional[Dict[str, Any]] = None,
) -> dict:
    if df is None or df.empty:
        return {
            "summary": empty_message,
            "bullets": [empty_message],
            "rows": 0,
        }

    if (result_mode or "").lower() == "scalar":
        raw_field = result_field or (df.columns[0] if len(df.columns) > 0 else "")
        field = _resolve_field_name(df, raw_field)
        if not field:
            return {
                "summary": empty_message,
                "bullets": [empty_message],
                "rows": 0,
            }
        value = df.iloc[0][field]
        if _is_missing(value):
            return {
                "summary": empty_message,
                "bullets": [empty_message],
                "rows": 0,
            }
        if context:
            period = (context or {}).get("period") or {}
            period_label = str(period.get("label") or period.get("start_yyyymm") or "지정 기간")
            metric = _metric_label(str(((context or {}).get("slots") or {}).get("metric") or ""))
            version = _version_hint(context)
            unit = str(((context or {}).get("slots") or {}).get("metric_unit") or "").strip()
            summary_text = f"{period_label} {version} {metric}은 {_to_scalar(value)}{unit}".strip()
            return {
                "summary": summary_text,
                "bullets": [f"- {summary_text}"],
                "rows": 1,
            }
        return {
            "summary": f"{field}={_to_scalar(value)}",
            "bullets": [f"- {field}={_to_scalar(value)}"],
            "rows": 1,
        }

    bullets: List[str] = []
    head = df.head(max_rows)
    cols = list(head.columns)
    non_missing_cells = 0
    for _, row in head.iterrows():
        vals = [row.get(c) for c in cols[:4]]
        non_missing_cells += sum(0 if _is_missing(v) else 1 for v in vals)
        pairs = [f"{c}={_to_scalar(v)}" for c, v in zip(cols[:4], vals)]
        bullets.append("- " + ", ".join(pairs))

    if non_missing_cells == 0:
        return {
            "summary": empty_message,
            "bullets": [empty_message],
            "rows": 0,
        }

    summary = f"총 {len(df)}건 조회되었습니다."
    return {"summary": summary, "bullets": bullets, "rows": len(df)}



def _build_condition_line(context: Optional[dict]) -> str:
    if not isinstance(context, dict):
        return ""
    slots = context.get("slots") or {}
    period = context.get("period") or {}
    agg = str(slots.get("aggregation") or "sum")
    version = _version_hint(context)
    period_label = str(period.get("label") or period.get("start_yyyymm") or "")
    if not period_label:
        period_label = "기간 미지정"
    agg_map = {"sum": "합계", "avg": "평균", "max": "최대", "min": "최소", "count": "건수"}
    agg_label = agg_map.get(agg, agg)
    compare_basis = str(slots.get("compare_basis") or period.get("compare_basis") or "").strip()
    line = f"- 기준: version={version}, 기간={period_label}, 집계={agg_label}"
    if compare_basis:
        line += f", 비교={compare_basis}"
    return line


def build_data_only_answer(sql_summary: dict, context: Optional[dict] = None) -> str:
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
        ]
    )
    cond = _build_condition_line(context)
    if cond:
        lines.append(cond)
    lines.extend(["", "🔗 이슈지 바로가기 👉 https://go/issueG"])
    return "\n".join(lines)



def build_hybrid_prompt(question: str, sql_summary_text: str, rag_context: str, *, sql_context: Optional[dict] = None) -> str:
    condition_line = _build_condition_line(sql_context) or "- SQL 기준 정보 없음"
    return f"""
당신은 GOC 업무 지원 챗봇입니다.
질문에 대해 SQL 결과와 문서 근거를 함께 반영해 답하세요.

[질문]
{question}

[SQL 요약]
{sql_summary_text}

[SQL 기준]
{condition_line}

[RAG 문서]
{rag_context}

출력 형식
📌 한줄 요약
- 핵심 결론 1문장

📊 데이터 기반 답변
- SQL 결과 기반 핵심 수치/건수/기간 2~5개

📂 문서 기반 보강
- 관련 이슈/배경/변경사항 1~4개
- 문서가 약하면 \"관련 문서는 제한적입니다.\"라고 명시

🧭 적용 기준
- SQL 기준 기간/필터/비교 기준
- 문서와 수치가 직접 연결되지 않으면 구분해서 설명

💡 참고
- SQL과 문서의 기준 시점이 다를 수 있으면 명시

🔗 이슈지 바로가기 👉 https://go/issueG
""".strip()



def build_hybrid_fallback_answer(sql_summary: dict, *, rag_found: bool, context: Optional[dict] = None) -> str:
    lines = [
        "📌 한줄 요약",
        f"- {sql_summary.get('summary', '조회 결과를 확인했습니다.')}",
        "",
        "📊 데이터 기반 답변",
    ]
    lines.extend(sql_summary.get("bullets") or ["- 조회 결과가 없습니다."])
    lines.extend(["", "📂 문서 기반 보강"])
    if rag_found:
        lines.append("- 관련 문서는 있었지만 질문과 직접 연결되는 근거는 제한적이었습니다.")
    else:
        lines.append("- 관련 문서는 제한적이어서 수치 중심으로 먼저 답변했습니다.")
    cond = _build_condition_line(context)
    lines.extend(["", "🧭 적용 기준"])
    if cond:
        lines.append(cond)
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
