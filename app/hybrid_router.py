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



def build_route_decision(intent: str, sql_match: Optional[SQLRegistryMatch]) -> HybridRouteDecision:
    if intent == "general_llm":
        return HybridRouteDecision(intent=intent, use_sql=False, use_rag=False, use_llm=True)
    if intent == "data_only":
        return HybridRouteDecision(intent=intent, use_sql=bool(sql_match), use_rag=False, use_llm=True)
    if intent == "hybrid":
        return HybridRouteDecision(intent=intent, use_sql=bool(sql_match), use_rag=True, use_llm=True)
    return HybridRouteDecision(intent="rag_only", use_sql=False, use_rag=True, use_llm=True)



def execute_sql_match(
    match: SQLRegistryMatch,
    *,
    question: str,
    run_oracle_query: Callable[..., pd.DataFrame],
) -> Dict[str, Any]:
    params, missing_params = build_sql_params_with_missing(match, question)
    started = time.perf_counter()

    if missing_params:
        return {
            "ok": False,
            "df": pd.DataFrame(),
            "params": params,
            "missing_params": missing_params,
            "runner": f"SQL:{match.item.id}",
            "elapsed_ms": int((time.perf_counter() - started) * 1000),
            "error": f"missing required params: {', '.join(missing_params)}",
        }

    try:
        df = run_oracle_query(match.item.sql, params=params)
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return {
            "ok": True,
            "df": df if isinstance(df, pd.DataFrame) else pd.DataFrame(),
            "params": params,
            "missing_params": [],
            "runner": f"SQL:{match.item.id}",
            "elapsed_ms": elapsed_ms,
            "sql_id": match.item.id,
            "result_mode": match.item.result.mode,
            "result_field": match.item.result.field,
            "empty_message": match.item.result.empty_message,
        }
    except Exception as e:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return {
            "ok": False,
            "df": pd.DataFrame(),
            "params": params,
            "missing_params": [],
            "runner": f"SQL:{match.item.id}",
            "elapsed_ms": elapsed_ms,
            "error": str(e),
        }
