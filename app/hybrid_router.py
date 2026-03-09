import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

import pandas as pd

from app.sql_registry import SQLRegistryMatch, build_sql_params


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
    runners: Dict[str, Any],
    run_oracle_query: Callable[..., pd.DataFrame],
) -> Dict[str, Any]:
    params = build_sql_params(match, question)
    started = time.perf_counter()
    try:
        if match.item.runner:
            runner = runners.get(match.item.runner)
            if not callable(runner):
                raise RuntimeError(f"runner not found: {match.item.runner}")
            df = runner(params or {})
            runner_name = match.item.runner
        elif match.item.sql:
            df = run_oracle_query(match.item.sql, params=params)
            runner_name = "RAW_SQL"
        else:
            raise RuntimeError(f"registry item has no runner/sql: {match.item.id}")

        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return {
            "ok": True,
            "df": df if isinstance(df, pd.DataFrame) else pd.DataFrame(),
            "params": params,
            "runner": runner_name,
            "elapsed_ms": elapsed_ms,
        }
    except Exception as e:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return {
            "ok": False,
            "df": pd.DataFrame(),
            "params": params,
            "runner": match.item.runner or "RAW_SQL",
            "elapsed_ms": elapsed_ms,
            "error": str(e),
        }
