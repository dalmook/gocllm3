import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

try:
    import pandas as pd
except Exception:  # pragma: no cover - local test shim
    class _PandasShim:
        class DataFrame:  # type: ignore[override]
            pass

        @staticmethod
        def isna(_value: Any) -> bool:
            return False

    pd = _PandasShim()  # type: ignore[assignment]

from app.sql_registry import SQLRegistryMatch, build_sql_params_with_missing


@dataclass
class HybridRouteDecision:
    intent: str
    use_sql: bool
    use_rag: bool
    use_llm: bool


def _metric_label(metric: str) -> str:
    return {
        "sales": "판매",
        "net_prod": "순생산",
        "net_ipgo": "순입고",
    }.get(str(metric or "").strip(), str(metric or "").strip())


def _format_period_tokens(period: Dict[str, Any]) -> List[str]:
    if not isinstance(period, dict):
        return []
    start = str(period.get("start_yyyymm") or "")
    end = str(period.get("end_yyyymm") or "")
    label = str(period.get("label") or "").strip()
    tokens: List[str] = []
    if label:
        tokens.append(label)
    if start and end and start == end and len(start) == 6:
        tokens.append(f"{start[:4]}-{start[4:6]}")
    elif start and end:
        tokens.append(f"{start[:4]}-{start[4:6]}~{end[:4]}-{end[4:6]}")
    return [x for x in tokens if x]


def _dedup_keep_order(items: List[str], *, max_items: int = 6) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        value = " ".join(str(item or "").split())
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
        if len(out) >= max_items:
            break
    return out


def _one_line_sql(sql: str, max_len: int = 320) -> str:
    s = " ".join((sql or "").split())
    if len(s) <= max_len:
        return s
    return s[:max_len] + "..."


def _preview_first_row(df: pd.DataFrame, max_cols: int = 4) -> Dict[str, Any]:
    if df is None or df.empty:
        return {}
    out: Dict[str, Any] = {}
    row = df.iloc[0]
    for col in list(df.columns)[:max_cols]:
        v = row.get(col)
        try:
            if pd.isna(v):
                v = None
        except Exception:
            pass
        out[str(col)] = v
    return out



def build_route_decision(intent: str, sql_match: Optional[SQLRegistryMatch]) -> HybridRouteDecision:
    if intent == "general_llm":
        return HybridRouteDecision(intent=intent, use_sql=False, use_rag=False, use_llm=True)
    if intent == "data_only":
        if sql_match:
            return HybridRouteDecision(intent=intent, use_sql=True, use_rag=False, use_llm=True)
        return HybridRouteDecision(intent="rag_only", use_sql=False, use_rag=True, use_llm=True)
    if intent == "hybrid":
        return HybridRouteDecision(intent=intent, use_sql=bool(sql_match), use_rag=True, use_llm=True)
    return HybridRouteDecision(intent="rag_only", use_sql=False, use_rag=True, use_llm=True)


def build_hybrid_search_queries(
    question: str,
    base_queries: List[str],
    *,
    sql_context: Optional[Dict[str, Any]] = None,
    sql_summary: Optional[Dict[str, Any]] = None,
    max_queries: int = 6,
) -> List[str]:
    queries = list(base_queries or [])
    if not sql_context:
        return _dedup_keep_order(queries, max_items=max_queries)

    slots = dict((sql_context or {}).get("slots") or {})
    period = dict((sql_context or {}).get("period") or {})
    metric = _metric_label(str(slots.get("metric") or ""))
    versions = [str(v).strip().upper() for v in (slots.get("versions") or []) if str(v).strip()]
    filters = dict(slots.get("filters") or {})
    compare_basis = str(slots.get("compare_basis") or period.get("compare_basis") or "").strip()
    period_tokens = _format_period_tokens(period)
    summary_text = str((sql_summary or {}).get("summary") or "").strip()

    focus_terms: List[str] = []
    if versions:
        focus_terms.extend(versions)
    for _, vals in filters.items():
        items = vals if isinstance(vals, list) else [vals]
        focus_terms.extend([str(v).strip().upper() for v in items if str(v).strip()])
    if metric:
        focus_terms.append(metric)
    focus_terms.extend(period_tokens[:2])

    intent_terms: List[str] = []
    q = str(question or "")
    if any(tok in q for tok in ("원인", "이유", "왜", "떨어", "감소", "급감", "안좋")):
        intent_terms.extend(["원인", "이슈", "변경 보고"])
    elif any(tok in q for tok in ("관련", "같이", "이슈")):
        intent_terms.extend(["관련 이슈", "변경 보고", "회의"])
    else:
        intent_terms.extend(["관련 이슈", "변경", "보고"])
    if compare_basis or str(slots.get("compare") or ""):
        intent_terms.append("차이 원인")

    enriched = list(queries)
    if focus_terms:
        enriched.append(" ".join([*focus_terms, *intent_terms[:2]]))
        enriched.append(" ".join([*focus_terms, intent_terms[-1]]))
    if summary_text and focus_terms:
        enriched.append(" ".join([*focus_terms[:3], summary_text, "관련 문서"]))
    return _dedup_keep_order(enriched, max_items=max_queries)



def execute_sql_match(
    match: SQLRegistryMatch,
    *,
    question: str,
    run_oracle_query: Callable[..., pd.DataFrame],
) -> Dict[str, Any]:
    params, missing_params = build_sql_params_with_missing(match, question)
    started = time.perf_counter()
    sql_id = match.item.id
    sql_preview = _one_line_sql(match.item.sql)
    print(f"[SQL_DEBUG] id={sql_id} question={question!r}")
    print(f"[SQL_DEBUG] id={sql_id} params={params}")
    print(f"[SQL_DEBUG] id={sql_id} sql={sql_preview}")

    if missing_params:
        print(f"[SQL_DEBUG] id={sql_id} missing_params={missing_params}")
        return {
            "ok": False,
            "df": pd.DataFrame(),
            "params": params,
            "missing_params": missing_params,
            "runner": f"SQL:{sql_id}",
            "elapsed_ms": int((time.perf_counter() - started) * 1000),
            "error": f"missing required params: {', '.join(missing_params)}",
        }

    try:
        df = run_oracle_query(match.item.sql, params=params)
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        rows = len(df.index) if isinstance(df, pd.DataFrame) else 0
        cols = list(df.columns) if isinstance(df, pd.DataFrame) else []
        first_row = _preview_first_row(df if isinstance(df, pd.DataFrame) else pd.DataFrame())
        print(f"[SQL_DEBUG] id={sql_id} ok=True rows={rows} cols={cols}")
        print(f"[SQL_DEBUG] id={sql_id} first_row={first_row}")
        return {
            "ok": True,
            "df": df if isinstance(df, pd.DataFrame) else pd.DataFrame(),
            "params": params,
            "missing_params": [],
            "runner": f"SQL:{sql_id}",
            "elapsed_ms": elapsed_ms,
            "sql_id": sql_id,
            "intent": str(match.intent or ""),
            "slots": dict(match.slots or {}),
            "period": dict(match.period or {}),
            "llm_used": bool(match.llm_used),
            "fallback_used": bool(match.fallback_used),
            "result_mode": match.item.result.mode,
            "result_field": match.item.result.field,
            "empty_message": match.item.result.empty_message,
        }
    except Exception as e:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        print(f"[SQL_DEBUG] id={sql_id} ok=False error={e}")
        return {
            "ok": False,
            "df": pd.DataFrame(),
            "params": params,
            "missing_params": [],
            "runner": f"SQL:{sql_id}",
            "elapsed_ms": elapsed_ms,
            "error": str(e),
        }
