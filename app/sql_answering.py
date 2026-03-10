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


def _format_percent(value: float) -> str:
    return f"{value:+.1f}%"


def _format_filters(filters: Dict[str, Any]) -> str:
    if not isinstance(filters, dict) or not filters:
        return ""
    parts: List[str] = []
    for k, vals in filters.items():
        items = vals if isinstance(vals, list) else [vals]
        cleaned = [str(v).strip() for v in items if str(v).strip()]
        if cleaned:
            parts.append(f"{str(k).upper()}={','.join(cleaned)}")
    return ", ".join(parts)


def _metric_label(metric: str) -> str:
    return {
        "sales": "판매",
        "net_prod": "순생산",
        "net_ipgo": "순입고",
    }.get(metric, metric)


def _build_common_criteria_lines(
    *,
    period_label: str = "",
    resolved_period_line: str = "",
    source_name: str,
    filter_text: str = "",
    agg_label: str = "",
    version: str = "",
    dimension: str = "",
) -> List[str]:
    lines: List[str] = []
    if period_label:
        lines.append(f"- 기준 기간: {period_label}")
    if resolved_period_line:
        lines.append(resolved_period_line)
    if filter_text:
        lines.append(f"- 적용 필터: {filter_text}")
    if version:
        lines.append(f"- 버전 기준: {version}")
    if dimension and dimension != "-":
        lines.append(f"- 분석 차원: {dimension}")
    if agg_label:
        lines.append(f"- 집계 방식: {agg_label}")
    lines.append(f"- 기준 source: {source_name}")
    lines.append("- 최신 적재 기준으로 조회")
    return lines


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


def _format_yyyymm_korean(yyyymm: str) -> str:
    value = str(yyyymm or "")
    if re.fullmatch(r"20\d{2}(0[1-9]|1[0-2])", value):
        return f"{value[:4]}년 {int(value[4:6])}월"
    return value


def _build_resolved_period_line(period: Dict[str, Any]) -> str:
    if not isinstance(period, dict) or not period:
        return ""

    start = str(period.get("start_yyyymm") or "")
    end = str(period.get("end_yyyymm") or "")
    label = str(period.get("label") or "").strip()
    compare_start = str(period.get("compare_start_yyyymm") or "")
    compare_end = str(period.get("compare_end_yyyymm") or "")

    if not start or not end:
        return ""

    if start == end:
        resolved = _format_yyyymm_korean(start)
    else:
        resolved = f"{_format_yyyymm_korean(start)}~{_format_yyyymm_korean(end)}"

    line = f"- 해석 기간: {resolved}"
    if label and label != resolved:
        line += f" (질문 표현: {label})"

    if compare_start and compare_end:
        if compare_start == compare_end:
            compare_label = _format_yyyymm_korean(compare_start)
        else:
            compare_label = f"{_format_yyyymm_korean(compare_start)}~{_format_yyyymm_korean(compare_end)}"
        line += f" / 비교 기간: {compare_label}"

    return line


def _build_exact_period_label(period: Dict[str, Any]) -> str:
    if not isinstance(period, dict) or not period:
        return ""

    start = str(period.get("start_yyyymm") or "")
    end = str(period.get("end_yyyymm") or "")
    if not start and not end:
        return str(period.get("label") or "")
    if start and end:
        if start == end:
            return _format_yyyymm_korean(start)
        return f"{_format_yyyymm_korean(start)}~{_format_yyyymm_korean(end)}"
    return str(period.get("label") or "")


def render_compare_versions_answer(
    *,
    rows: List[Dict[str, Any]],
    metric: str,
    unit: str,
    period_label: str,
    resolved_period_line: str,
    source_name: str,
    filter_text: str = "",
) -> str:
    if len(rows) < 2:
        lines = [
            "📌 한줄 요약",
            "- 비교 가능한 버전 데이터가 부족합니다.",
            "",
            "📊 비교 결과",
            "- 버전 2개 이상이 필요합니다.",
            "",
            "⚠️ 기준",
            f"- 기준 기간: {period_label}",
        ]
        if resolved_period_line:
            lines.append(resolved_period_line)
        lines.extend([f"- 기준 source: {source_name}", "- 최신 적재 기준으로 조회"])
        return "\n".join(lines)

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
        lines = [
            "📌 한줄 요약",
            "- 비교 가능한 버전 데이터가 부족합니다.",
            "",
            "📊 비교 결과",
            "- 버전 값 파싱에 실패했습니다.",
            "",
            "⚠️ 기준",
            f"- 기준 기간: {period_label}",
        ]
        if resolved_period_line:
            lines.append(resolved_period_line)
        lines.extend([f"- 기준 source: {source_name}", "- 최신 적재 기준으로 조회"])
        return "\n".join(lines)

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

    metric_label = _metric_label(metric)
    total = left_val + right_val
    top_share = (top_val / total * 100.0) if total else 0.0
    gap_vs_total = (diff / total * 100.0) if total else 0.0

    analysis_lines: List[str] = []
    if abs(ratio) >= 20:
        analysis_lines.append(f"- {top_ver} 우세가 뚜렷하며 격차가 전체 합계의 {_format_percent(gap_vs_total)} 수준입니다.")
    elif abs(ratio) >= 5:
        analysis_lines.append(f"- {top_ver}가 {low_ver}보다 의미 있게 높고 점유 비중은 {top_share:.1f}%입니다.")
    else:
        analysis_lines.append(f"- 두 버전 간 차이가 크지 않아 단일 월 판단보다 다기간 비교가 적절합니다. (상위 비중 {top_share:.1f}%)")
    analysis_lines.append(f"- 절대 차이는 {_format_number(diff, f' {unit}')}이고 상대 격차는 {ratio_text}입니다.")
    analysis_lines.append("- 다음 단계로는 전월/전년 동월 비교를 붙이면 구조적 차이인지 일시 변동인지 구분하기 쉽습니다.")

    return "\n".join(
        [
            "📌 한줄 요약",
            f"- {filter_text + ' 기준 ' if filter_text else ''}{period_label} {metric_label} 비교에서 {top_ver}가 {low_ver} 대비 {_format_number(diff, f' {unit}')} 높고, 격차는 {ratio_text}입니다.",
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
            *_build_common_criteria_lines(
                period_label=period_label,
                resolved_period_line=resolved_period_line,
                source_name=source_name,
                filter_text=filter_text,
            ),
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
    resolved_period_line: str,
    source_name: str,
    filter_text: str = "",
) -> str:
    metric_label = _metric_label(metric)

    if not rows:
        lines = [
            "📌 한줄 요약",
            "- 추이 데이터가 없습니다.",
            "",
            "📈 기간별 추이",
            "- 조회 결과가 없습니다.",
            "",
            "⚠️ 기준",
        ]
        if resolved_period_line:
            lines.append(resolved_period_line)
        lines.extend([f"- 기준 source: {source_name}", "- 최신 적재 기준 조회"])
        return "\n".join(lines)

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
        lines = [
            "📌 한줄 요약",
            "- 추이 데이터가 없습니다.",
            "",
            "📈 기간별 추이",
            "- 조회 결과가 없습니다.",
            "",
            "⚠️ 기준",
        ]
        if resolved_period_line:
            lines.append(resolved_period_line)
        lines.extend([f"- 기준 source: {source_name}", "- 최신 적재 기준 조회"])
        return "\n".join(lines)

    first_v = ordered[0][1]
    last_v = ordered[-1][1]
    if last_v > first_v:
        pattern = "상승"
    elif last_v < first_v:
        pattern = "하락"
    else:
        pattern = "변동"

    trend_lines = [f"- {_ym_label(p)}: {_format_number(v, f' {unit}')}" for p, v in ordered]
    values = [v for _, v in ordered]
    peak_period, peak_val = max(ordered, key=lambda x: x[1])
    low_period, low_val = min(ordered, key=lambda x: x[1])
    avg_val = sum(values) / len(values)
    span_ratio = ((peak_val - low_val) / low_val * 100.0) if low_val else 0.0
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
    analysis.append(f"- 최고치는 {_ym_label(peak_period)} {_format_number(peak_val, f' {unit}')}, 최저치는 {_ym_label(low_period)} {_format_number(low_val, f' {unit}')}입니다.")
    analysis.append(f"- 기간 평균은 {_format_number(avg_val, f' {unit}')}이며 고저 차는 {_format_percent(span_ratio)}입니다.")
    analysis.append("- 추가 기간 확장 시 계절성/일시 변동 구분이 쉬워집니다.")

    return "\n".join(
        [
            "📌 한줄 요약",
            f"- {filter_text + ' 기준 ' if filter_text else ''}{metric_label}은 {_ym_label(ordered[0][0])}→{_ym_label(ordered[-1][0])} 구간에서 {pattern} 흐름이며, 최고치는 {_ym_label(peak_period)}입니다.",
            "",
            "📈 기간별 추이",
            *trend_lines,
            "",
            "💡 분석",
            *analysis,
            "",
            "⚠️ 기준",
            *_build_common_criteria_lines(
                resolved_period_line=resolved_period_line,
                source_name=source_name,
                filter_text=filter_text,
            ),
        ]
    )


def render_compare_period_groups_answer(
    *,
    rows: List[Dict[str, Any]],
    metric: str,
    unit: str,
    resolved_period_line: str,
    source_name: str,
    filter_text: str = "",
) -> str:
    metric_label = _metric_label(metric)

    parsed = []
    for row in rows:
        label = str(row.get("PERIOD_GROUP") or row.get("period_group") or row.get("LABEL") or "").strip()
        value = row.get("VALUE") if "VALUE" in row else row.get("value")
        try:
            fval = float(value or 0.0)
        except Exception:
            fval = 0.0
        if label:
            parsed.append((label, fval))

    if len(parsed) < 2:
        lines = [
            "📌 한줄 요약",
            "- 기간 그룹 비교 데이터가 부족합니다.",
            "",
            "📊 기간 그룹 비교",
            "- 비교할 그룹 2개 이상이 필요합니다.",
            "",
            "⚠️ 기준",
        ]
        if resolved_period_line:
            lines.append(resolved_period_line)
        lines.extend([f"- 기준 source: {source_name}", "- 최신 적재 기준 조회"])
        return "\n".join(lines)

    parsed.sort(key=lambda x: x[0])
    left_label, left_val = parsed[0]
    right_label, right_val = parsed[1]
    if right_val >= left_val:
        top_label, top_val = right_label, right_val
        low_label, low_val = left_label, left_val
    else:
        top_label, top_val = left_label, left_val
        low_label, low_val = right_label, right_val

    stat = compute_diff_and_ratio(top_val, low_val)
    ratio_text = f"{stat['ratio']:+.1f}%"
    direction = "높습니다" if stat["diff"] >= 0 else "낮습니다"
    total = left_val + right_val
    top_share = (top_val / total * 100.0) if total else 0.0

    return "\n".join(
        [
            "📌 한줄 요약",
            f"- {filter_text + ' 기준 ' if filter_text else ''}{metric_label}은 {top_label}이 우세하며 {low_label} 대비 {_format_number(abs(stat['diff']), f' {unit}')} 차이입니다.",
            "",
            "📊 기간 그룹 비교",
            f"- {left_label}: {_format_number(left_val, f' {unit}')}",
            f"- {right_label}: {_format_number(right_val, f' {unit}')}",
            f"- 차이: {_format_number(stat['diff'], f' {unit}')}",
            f"- 증감률: {ratio_text}",
            "",
            "💡 분석",
            f"- 상위 구간 {top_label}의 비중은 비교 대상 합계 기준 {top_share:.1f}%입니다.",
            f"- 격차는 {ratio_text}로, 구조적 차이 확인을 위해 월 단위 드릴다운이 유효합니다.",
            "- 필요하면 월 단위 드릴다운으로 원인 구간을 추가 확인할 수 있습니다.",
            "",
            "⚠️ 기준",
            *_build_common_criteria_lines(
                resolved_period_line=resolved_period_line,
                source_name=source_name,
                filter_text=filter_text,
            ),
        ]
    )


def render_grouped_dimension_answer(
    *,
    rows: List[Dict[str, Any]],
    metric: str,
    unit: str,
    resolved_period_line: str,
    source_name: str,
    dimension: str,
    filter_text: str = "",
) -> str:
    metric_label = _metric_label(metric)
    dim_label = {
        "version": "버전",
        "yearmonth": "년월",
        "fam1": "FAM1",
    }.get(dimension, dimension or "차원")

    parsed = []
    for row in rows:
        key = str(row.get("DIMENSION_VALUE") or row.get("dimension_value") or row.get("VERSION") or row.get("version") or "").strip()
        value = row.get("VALUE") if "VALUE" in row else row.get("value")
        try:
            fval = float(value or 0.0)
        except Exception:
            fval = 0.0
        if key:
            parsed.append((key, fval))
    if not parsed:
        lines = [
            "📌 한줄 요약",
            "- 그룹 데이터가 없습니다.",
            "",
            "📊 그룹별 결과",
            "- 조회 결과가 없습니다.",
            "",
            "⚠️ 기준",
        ]
        if resolved_period_line:
            lines.append(resolved_period_line)
        lines.extend([f"- 기준 source: {source_name}", "- 최신 적재 기준 조회"])
        return "\n".join(lines)

    parsed.sort(key=lambda x: x[1], reverse=True)
    top_key, top_val = parsed[0]
    bottom_key, bottom_val = parsed[-1]
    total_val = sum(v for _, v in parsed)
    top_share = (top_val / total_val * 100.0) if total_val else 0.0
    lines = [f"- {k}: {_format_number(v, f' {unit}')}" for k, v in parsed[:10]]
    return "\n".join(
        [
            "📌 한줄 요약",
            f"- {(filter_text + '에서 ' if filter_text else '')}{dim_label} 기준 {metric_label}은 {top_key}가 최댓값이며 비중은 {top_share:.1f}%입니다.",
            "",
            "📊 그룹별 결과",
            *lines,
            "",
            "💡 분석",
            f"- 상위 항목 {top_key}가 {_format_number(top_val, f' {unit}')}로 가장 크고, 하위 항목 {bottom_key}는 {_format_number(bottom_val, f' {unit}')}입니다.",
            f"- 상하위 격차는 {_format_number(top_val - bottom_val, f' {unit}')}입니다.",
            "- 상위/하위 그룹 편차를 함께 보면 분산 정도를 빠르게 파악할 수 있습니다.",
            "",
            "⚠️ 기준",
            *_build_common_criteria_lines(
                resolved_period_line=resolved_period_line,
                source_name=source_name,
                filter_text=filter_text,
                dimension=dim_label,
            ),
        ]
    )


def render_total_answer(
    *,
    rows: List[Dict[str, Any]],
    metric: str,
    unit: str,
    period_label: str,
    resolved_period_line: str,
    source_name: str,
    version_hint: str = "전체",
    filter_text: str = "",
) -> str:
    total = 0.0
    values: List[float] = []
    if rows:
        if ("VALUE" in rows[0]) or ("value" in rows[0]):
            for row in rows:
                try:
                    num = float(row.get("VALUE") if "VALUE" in row else row.get("value") or 0.0)
                    total += num
                    values.append(num)
                except Exception:
                    continue
        else:
            picked = _pick_scalar_value(rows)
            total = float(picked or 0.0)
            if picked is not None:
                values.append(float(picked))
    avg_value = (sum(values) / len(values)) if values else total
    return "\n".join(
        [
            "📌 한줄 요약",
            f"- {filter_text + ' / ' if filter_text else ''}{period_label} 기준 {version_hint} {_metric_label(metric)} 합계는 {_format_number(total, f' {unit}')}입니다.",
            "",
            "📊 데이터 기반 답변",
            f"- 합계: {_format_number(total, f' {unit}')}",
            f"- 평균 기준값: {_format_number(avg_value, f' {unit}')}",
            f"- 집계 대상 건수: {len(values) or len(rows)}건",
            "",
            "💡 분석",
            "- 단일 합계 값은 수준 판단에는 유효하지만 변동 원인 분석에는 세부 breakdown이 추가로 필요합니다.",
            "- 필요하면 버전별/월별/차원별 드릴다운으로 구성 차이를 이어서 확인할 수 있습니다.",
            "",
            "⚠️ 기준",
            *_build_common_criteria_lines(
                period_label=period_label,
                resolved_period_line=resolved_period_line,
                source_name=source_name,
                filter_text=filter_text,
                version=version_hint,
            ),
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
    period_label = _build_exact_period_label(period) or str(period.get("label") or f"{period.get('start_yyyymm','')}~{period.get('end_yyyymm','')}")
    resolved_period_line = _build_resolved_period_line(period)
    filter_text = _format_filters(dict(slots.get("filters") or {}))
    family = str(slots.get("family") or "")
    primary_query_id = str(primary.get("query_id") or "")
    if not family and primary_query_id:
        family = primary_query_id

    if intent == "metric_compare_versions" or family == "compare_versions_same_period":
        return render_compare_versions_answer(
            rows=primary_rows,
            metric=metric,
            unit=unit,
            period_label=period_label,
            resolved_period_line=resolved_period_line,
            source_name=source_name,
            filter_text=filter_text,
        )
    if intent == "metric_trend_by_period" or family == "trend_by_period":
        return render_trend_answer(
            rows=primary_rows,
            metric=metric,
            unit=unit,
            resolved_period_line=resolved_period_line,
            source_name=source_name,
            filter_text=filter_text,
        )
    if intent == "metric_compare_period_groups" or family == "compare_period_groups":
        return render_compare_period_groups_answer(
            rows=primary_rows,
            metric=metric,
            unit=unit,
            resolved_period_line=resolved_period_line,
            source_name=source_name,
            filter_text=filter_text,
        )
    if intent == "metric_grouped_dimension" or family == "grouped_by_dimension":
        return render_grouped_dimension_answer(
            rows=primary_rows,
            metric=metric,
            unit=unit,
            resolved_period_line=resolved_period_line,
            source_name=source_name,
            dimension=str(slots.get("dimension") or ""),
            filter_text=filter_text,
        )
    if intent == "sales_compare":
        if primary_rows and (("VERSION" in primary_rows[0]) or ("version" in primary_rows[0])):
            return render_compare_versions_answer(
                rows=primary_rows,
                metric=metric,
                unit=unit,
                period_label=period_label,
                resolved_period_line=resolved_period_line,
                source_name=source_name,
                filter_text=filter_text,
            )
        c = _pick_compare_values(primary_rows)
        summary = f"비교 기준 판매량은 현재 {_format_number(c['current'])}, 이전 {_format_number(c['previous'])}입니다."
        data_lines.append(f"- 현재 기간: {_format_number(c['current'])}")
        data_lines.append(f"- 비교 기간: {_format_number(c['previous'])}")
        data_lines.append(f"- 증감: {_format_number(c['diff'])}")
    elif intent == "sales_trend":
        peaks = _top_months(primary_rows, top_n=2)
        summary = f"{period_label or '지정 기간'} 판매 추이를 조회했습니다."
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
        summary = f"{period_label or '지정 기간'} 버전별 판매량을 조회했습니다."
        if top:
            data_lines.append(f"- 상위 버전: {', '.join(top)}")
    else:
        if primary_rows and (("VERSION" in primary_rows[0]) or ("version" in primary_rows[0])) and (("VALUE" in primary_rows[0]) or ("value" in primary_rows[0])):
            return render_total_answer(
                rows=primary_rows,
                metric=metric,
                unit=unit,
                period_label=period_label,
                resolved_period_line=resolved_period_line,
                source_name=source_name,
                version_hint="전체",
            )
        else:
            total = _pick_scalar_value(primary_rows)
            if total is None:
                total = 0.0
            version = str(slots.get("version") or "전체")
            prefix = f"{filter_text} 기준 " if filter_text else ""
            metric_label = _metric_label(metric)
            summary = f"{prefix}{period_label or '지정 기간'} {version} {metric_label} 합계는 {_format_number(total)}이며, 현재 질의는 총량 수준을 확인하는 용도에 적합합니다."
            data_lines.append(f"- {metric_label} 합계: {_format_number(total)}")
            data_lines.append(f"- 평균 기준값: {_format_number(total)}")
            data_lines.append("- 집계 대상 건수: 1건")
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
    period_label = _build_exact_period_label(period) or str(period.get("label") or f"{period.get('start_yyyymm','')}~{period.get('end_yyyymm','')}")

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
    lines.extend(
        _build_common_criteria_lines(
            period_label=period_label,
            resolved_period_line=resolved_period_line,
            source_name=source_name,
            filter_text=filter_text,
            agg_label=agg_map.get(agg, agg),
            version=version,
            dimension=dim,
        )
    )
    lines.extend(["", "🔗 이슈지 바로가기 👉 https://go/issueG"])
    return "\n".join(lines)


def build_sql_render_prompt(payload: Dict[str, Any]) -> str:
    return (
        "다음 structured 결과를 바탕으로 한국어 답변을 작성하세요. SQL 작성 금지. "
        "반드시 섹션 3개(한줄 요약, 데이터 기반 답변, 해석 기준)로 답하세요. "
        "해석 기준 섹션의 기준 기간은 반드시 exact_period_label 값을 사용하세요. "
        "resolved_period_line이 있으면 그대로 반영해 질문의 기간 표현과 실제 적용 기간을 명확히 드러내세요. "
        "한줄 요약은 단순 재진술이 아니라 우세 항목, 변동 방향, 격차 수준 중 최소 하나를 포함하세요. "
        "데이터 기반 답변에는 핵심 수치 2개 이상을 넣고, 해석 기준에는 적용 필터/집계 방식/분석 차원을 드러내세요.\n"
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
    if not bool((slots or {}).get("analysis")) and intent in {
        "metric_compare_versions",
        "metric_trend_by_period",
        "metric_compare_period_groups",
        "metric_grouped_dimension",
    }:
        return None

    payload = {
        "question": question,
        "intent": intent,
        "slots": slots,
        "period": period,
        "exact_period_label": _build_exact_period_label(period),
        "resolved_period_line": _build_resolved_period_line(period),
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
