import json
import re
from typing import Any, Callable, Dict, List, Optional


def _get_df_rows(df: Any) -> List[Dict[str, Any]]:
    if df is None:
        return []
    try:
        if getattr(df, "empty", False):
            return []
    except Exception:
        pass
    try:
        return [dict(x) for x in df.to_dict(orient="records")]
    except Exception:
        return []


def _format_number(value: Any, unit: str = "개") -> str:
    if value is None:
        return f"0{unit}"
    try:
        n = float(value)
    except Exception:
        return str(value)
    if abs(n - round(n)) < 0.05:
        return f"{int(round(n)):,}{unit}"
    return f"{round(n, 1):,}{unit}"


def _pick_scalar_value(rows: List[Dict[str, Any]]) -> Optional[float]:
    if not rows:
        return None
    row = rows[0]
    for key in ("sales", "SALES", "current_sales", "CURRENT_SALES"):
        if key in row:
            try:
                return float(row[key])
            except Exception:
                pass
    for _, v in row.items():
        try:
            return float(v)
        except Exception:
            continue
    return None


def _pick_compare_values(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    out = {"current": 0.0, "previous": 0.0, "diff": 0.0}
    if not rows:
        return out
    row = rows[0]
    for key, target in [
        ("current_sales", "current"),
        ("CURRENT_SALES", "current"),
        ("previous_sales", "previous"),
        ("PREVIOUS_SALES", "previous"),
        ("diff_sales", "diff"),
        ("DIFF_SALES", "diff"),
    ]:
        if key in row:
            try:
                out[target] = float(row[key])
            except Exception:
                pass
    if out["diff"] == 0.0:
        out["diff"] = out["current"] - out["previous"]
    return out


def compute_diff_and_ratio(base: float, other: float) -> Dict[str, float]:
    diff = base - other
    if other == 0:
        ratio = 0.0
    else:
        ratio = (diff / other) * 100.0
    return {"diff": diff, "ratio": ratio}


def render_compare_versions_answer(
    *,
    rows: List[Dict[str, Any]],
    metric: str,
    unit: str,
    period_label: str,
    source_name: str,
) -> str:
    if len(rows) < 2:
        return (
            "📌 한줄 요약\n"
            "- 비교 가능한 버전 데이터가 부족합니다.\n\n"
            "📊 비교 결과\n"
            "- 버전 2개 이상이 필요합니다.\n\n"
            "⚠️ 기준\n"
            f"- 기준 기간: {period_label}\n"
            f"- 기준 source: {source_name}\n"
            "- 최신 적재 기준으로 조회"
        )

    parsed = []
    for row in rows:
        version = str(row.get("VERSION") or row.get("version") or "").strip().upper()
        value = row.get("VALUE") if "VALUE" in row else row.get("value")
        try:
            fval = float(value or 0.0)
        except Exception:
            fval = 0.0
        if version:
            parsed.append((version, fval))

    if len(parsed) < 2:
        return (
            "📌 한줄 요약\n"
            "- 비교 가능한 버전 데이터가 부족합니다.\n\n"
            "📊 비교 결과\n"
            "- 버전 값 파싱에 실패했습니다.\n\n"
            "⚠️ 기준\n"
            f"- 기준 기간: {period_label}\n"
            f"- 기준 source: {source_name}\n"
            "- 최신 적재 기준으로 조회"
        )

    parsed.sort(key=lambda x: x[0])
    left_ver, left_val = parsed[0]
    right_ver, right_val = parsed[1]
    if left_val >= right_val:
        top_ver, top_val = left_ver, left_val
        low_ver, low_val = right_ver, right_val
    else:
        top_ver, top_val = right_ver, right_val
        low_ver, low_val = left_ver, left_val

    stat = compute_diff_and_ratio(top_val, low_val)
    diff = stat["diff"]
    ratio = stat["ratio"]
    ratio_text = f"{ratio:+.1f}%"

    metric_label = {
        "sales": "판매",
        "net_prod": "순생산",
        "net_ipgo": "순입고",
    }.get(metric, metric)

    analysis_lines: List[str] = []
    if abs(ratio) >= 20:
        analysis_lines.append(f"- {top_ver} 우세가 뚜렷합니다.")
    elif abs(ratio) >= 5:
        analysis_lines.append(f"- {top_ver}가 {low_ver}보다 높게 나타났습니다.")
    else:
        analysis_lines.append("- 두 버전 간 차이가 크지 않습니다.")
    analysis_lines.append("- 추가 기간 비교 시 추세 판단 정확도가 높아집니다.")

    return "\n".join(
        [
            "📌 한줄 요약",
            f"- {top_ver} {metric_label}은 {low_ver} 대비 {_format_number(diff, f' {unit}')} 높습니다.",
            "",
            "📊 비교 결과",
            f"- {left_ver}: {_format_number(left_val, f' {unit}')}",
            f"- {right_ver}: {_format_number(right_val, f' {unit}')}",
            f"- 차이: {_format_number(diff, f' {unit}')}",
            f"- 증감률: {ratio_text}",
            "",
            "💡 분석",
            *analysis_lines,
            "",
            "⚠️ 기준",
            f"- 기준 기간: {period_label}",
            f"- 기준 source: {source_name}",
            "- 최신 적재 기준으로 조회",
        ]
    )


def _ym_label(yyyymm: str) -> str:
    if re.fullmatch(r"20\d{2}(0[1-9]|1[0-2])", str(yyyymm or "")):
        return f"{yyyymm[:4]}-{yyyymm[4:]}"
    return str(yyyymm or "")


def render_trend_answer(
    *,
    rows: List[Dict[str, Any]],
    metric: str,
    unit: str,
    source_name: str,
) -> str:
    metric_label = {
        "sales": "판매",
        "net_prod": "순생산",
        "net_ipgo": "순입고",
    }.get(metric, metric)

    if not rows:
        return (
            "📌 한줄 요약\n"
            "- 추이 데이터가 없습니다.\n\n"
            "📈 기간별 추이\n"
            "- 조회 결과가 없습니다.\n\n"
            "⚠️ 기준\n"
            f"- 기준 source: {source_name}\n"
            "- 최신 적재 기준 조회"
        )

    agg_by_period: Dict[str, float] = {}
    for row in rows:
        period = str(row.get("PERIOD") or row.get("period") or row.get("YEARMONTH") or row.get("yearmonth") or "")
        value = row.get("VALUE") if "VALUE" in row else row.get("value")
        try:
            fval = float(value or 0.0)
        except Exception:
            fval = 0.0
        if not period:
            continue
        agg_by_period[period] = agg_by_period.get(period, 0.0) + fval

    ordered = sorted(agg_by_period.items(), key=lambda x: x[0])
    if not ordered:
        return (
            "📌 한줄 요약\n"
            "- 추이 데이터가 없습니다.\n\n"
            "📈 기간별 추이\n"
            "- 조회 결과가 없습니다.\n\n"
            "⚠️ 기준\n"
            f"- 기준 source: {source_name}\n"
            "- 최신 적재 기준 조회"
        )

    first_v = ordered[0][1]
    last_v = ordered[-1][1]
    if last_v > first_v:
        pattern = "상승"
    elif last_v < first_v:
        pattern = "하락"
    else:
        pattern = "변동"

    trend_lines = [f"- {_ym_label(p)}: {_format_number(v, f' {unit}')}" for p, v in ordered]
    analysis = []
    if len(ordered) >= 3:
        deltas = [ordered[i + 1][1] - ordered[i][1] for i in range(len(ordered) - 1)]
        pos = sum(1 for d in deltas if d > 0)
        neg = sum(1 for d in deltas if d < 0)
        if pos and neg:
            analysis.append("- 중간 변동이 있어 단조 추세보다는 변동성이 있습니다.")
        elif pos > 0:
            analysis.append("- 기간이 진행될수록 증가하는 흐름입니다.")
        elif neg > 0:
            analysis.append("- 기간이 진행될수록 감소하는 흐름입니다.")
    analysis.append("- 추가 기간 확장 시 계절성/일시 변동 구분이 쉬워집니다.")

    return "\n".join(
        [
            "📌 한줄 요약",
            f"- {metric_label}은 {_ym_label(ordered[0][0])}→{_ym_label(ordered[-1][0])} 기준 {pattern} 패턴입니다.",
            "",
            "📈 기간별 추이",
            *trend_lines,
            "",
            "💡 분석",
            *analysis,
            "",
            "⚠️ 기준",
            f"- 기준 source: {source_name}",
            "- 최신 적재 기준 조회",
        ]
    )


def _top_months(rows: List[Dict[str, Any]], top_n: int = 2) -> List[str]:
    vals = []
    for r in rows:
        ym = str(r.get("YEARMONTH") or r.get("yearmonth") or "")
        sales = r.get("sales") if "sales" in r else r.get("SALES")
        if not ym:
            continue
        try:
            vals.append((ym, float(sales or 0)))
        except Exception:
            continue
    vals.sort(key=lambda x: x[1], reverse=True)
    out = []
    for ym, v in vals[:top_n]:
        if re.fullmatch(r"20\d{2}(0[1-9]|1[0-2])", ym):
            out.append(f"{ym[:4]}-{ym[4:]} {_format_number(v)}")
        else:
            out.append(f"{ym} {_format_number(v)}")
    return out


def render_answer_rule_based(
    question: str,
    *,
    intent: str,
    slots: Dict[str, Any],
    period: Dict[str, Any],
    results: List[Dict[str, Any]],
    period_infer_reason: str = "",
) -> str:
    by_role = {str(r.get("role") or ""): r for r in results}
    primary = by_role.get("primary") or (results[0] if results else {})
    primary_rows = _get_df_rows(primary.get("df"))

    summary = "조회 결과를 확인했습니다."
    data_lines: List[str] = []

    metric = str(slots.get("metric") or "sales")
    unit = str(slots.get("metric_unit") or "MEQ")
    source_name = str(slots.get("source_name") or "psi_simul")
    period_label = str(period.get("label") or f"{period.get('start_yyyymm','')}~{period.get('end_yyyymm','')}")

    if intent == "metric_compare_versions":
        return render_compare_versions_answer(
            rows=primary_rows,
            metric=metric,
            unit=unit,
            period_label=period_label,
            source_name=source_name,
        )
    if intent == "metric_trend_by_period":
        return render_trend_answer(
            rows=primary_rows,
            metric=metric,
            unit=unit,
            source_name=source_name,
        )
    if intent == "sales_compare":
        if primary_rows and (("VERSION" in primary_rows[0]) or ("version" in primary_rows[0])):
            return render_compare_versions_answer(
                rows=primary_rows,
                metric=metric,
                unit=unit,
                period_label=period_label,
                source_name=source_name,
            )
        c = _pick_compare_values(primary_rows)
        summary = f"비교 기준 판매량은 현재 {_format_number(c['current'])}, 이전 {_format_number(c['previous'])}입니다."
        data_lines.append(f"- 현재 기간: {_format_number(c['current'])}")
        data_lines.append(f"- 비교 기간: {_format_number(c['previous'])}")
        data_lines.append(f"- 증감: {_format_number(c['diff'])}")
    elif intent == "sales_trend":
        peaks = _top_months(primary_rows, top_n=2)
        summary = f"{period.get('label') or '지정 기간'} 판매 추이를 조회했습니다."
        if peaks:
            data_lines.append(f"- 상위 월: {', '.join(peaks)}")
        aux = by_role.get("aux")
        if aux:
            aux_rows = _get_df_rows(aux.get("df"))
            total = _pick_scalar_value(aux_rows)
            if total is not None:
                data_lines.append(f"- 기간 총합: {_format_number(total)}")
    elif intent == "sales_grouped":
        top = []
        for r in primary_rows[:5]:
            ver = str(r.get("VERSION") or r.get("version") or "-")
            sales = r.get("sales") if "sales" in r else r.get("SALES")
            top.append(f"{ver} {_format_number(sales)}")
        summary = f"{period.get('label') or '지정 기간'} 버전별 판매량을 조회했습니다."
        if top:
            data_lines.append(f"- 상위 버전: {', '.join(top)}")
    else:
        if primary_rows and (("VERSION" in primary_rows[0]) or ("version" in primary_rows[0])) and (("VALUE" in primary_rows[0]) or ("value" in primary_rows[0])):
            top = []
            for r in primary_rows[:5]:
                ver = str(r.get("VERSION") or r.get("version") or "-")
                val = r.get("VALUE") if "VALUE" in r else r.get("value")
                top.append(f"{ver} {_format_number(val, f' {unit}')}")
            summary = f"{period_label} 기준 {metric}를 조회했습니다."
            data_lines.append(f"- 버전별 값: {', '.join(top)}")
        else:
            total = _pick_scalar_value(primary_rows)
            if total is None:
                total = 0.0
            version = str(slots.get("version") or "전체")
            summary = f"{period.get('label') or '지정 기간'} {version} 누적 판매량은 {_format_number(total)}입니다."
            data_lines.append(f"- 누적 판매량: {_format_number(total)}")
            aux = by_role.get("aux")
            if aux:
                aux_peaks = _top_months(_get_df_rows(aux.get("df")), top_n=3)
                if aux_peaks:
                    data_lines.append(f"- 월별 breakdown: {', '.join(aux_peaks)}")

    if not data_lines:
        data_lines.append("- 조회 결과가 없습니다.")

    agg = str(slots.get("aggregation") or "sum")
    agg_map = {"sum": "합계", "avg": "평균", "max": "최대", "min": "최소", "count": "건수"}
    dim = str(slots.get("dimension") or "-")
    version = str(slots.get("version") or "전체")
    period_label = str(period.get("label") or f"{period.get('start_yyyymm','')}~{period.get('end_yyyymm','')}")

    lines = [
        "📌 한줄 요약",
        f"- {summary}",
        "",
        "📊 데이터 기반 답변",
    ]
    lines.extend(data_lines)
    lines.extend(["", "🧭 해석 기준"])
    if period_infer_reason:
        lines.append(f"- {period_infer_reason}")
    lines.append(f"- 기준: version={version}, 기간={period_label}, 집계={agg_map.get(agg, agg)}, 차원={dim}")
    lines.extend(["", "🔗 이슈지 바로가기 👉 https://go/issueG"])
    return "\n".join(lines)


def build_sql_render_prompt(payload: Dict[str, Any]) -> str:
    return (
        "다음 structured 결과를 바탕으로 한국어 답변을 작성하세요. SQL 작성 금지. "
        "반드시 섹션 3개(한줄 요약, 데이터 기반 답변, 해석 기준)로 답하세요.\n"
        f"INPUT={json.dumps(payload, ensure_ascii=False)}"
    )


def render_answer_with_llm(
    *,
    llm_render_fn: Callable[[str], Optional[str]],
    question: str,
    intent: str,
    slots: Dict[str, Any],
    period: Dict[str, Any],
    results: List[Dict[str, Any]],
    period_infer_reason: str = "",
) -> Optional[str]:
    if not bool((slots or {}).get("analysis")) and intent in {"metric_compare_versions", "metric_trend_by_period"}:
        return None

    payload = {
        "question": question,
        "intent": intent,
        "slots": slots,
        "period": period,
        "period_infer_reason": period_infer_reason,
        "result_rows": [
            {
                "query_id": r.get("query_id"),
                "role": r.get("role"),
                "rows": _get_df_rows(r.get("df"))[:12],
            }
            for r in results
        ],
    }
    prompt = build_sql_render_prompt(payload)
    try:
        text = llm_render_fn(prompt)
        if not text or not isinstance(text, str):
            return None
        t = text.strip()
        if "📌" not in t:
            return None
        return t
    except Exception:
        return None
