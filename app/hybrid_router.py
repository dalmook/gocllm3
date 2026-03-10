import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

import pandas as pd

from app.sql_registry import SQLRegistryMatch, build_sql_params_with_missing


@dataclass
class HybridRouteDecision:
    intent: str
    use_sql: bool
    use_rag: bool
    use_llm: bool


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
        return HybridRouteDecision(intent=intent, use_sql=bool(sql_match), use_rag=False, use_llm=True)
    if intent == "hybrid":
        return HybridRouteDecision(intent=intent, use_sql=bool(sql_match), use_rag=True, use_llm=True)
    if intent in {"doc_nav", "doc_summary"}:
        return HybridRouteDecision(intent=intent, use_sql=False, use_rag=True, use_llm=True)
    return HybridRouteDecision(intent="rag_only", use_sql=False, use_rag=True, use_llm=True)



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
