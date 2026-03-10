import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import yaml

from app.sql_period import PeriodResolution, resolve_period_slots


SQL_REGISTRY_YAML_PATH = os.getenv("SQL_REGISTRY_YAML_PATH", os.path.join(os.path.dirname(__file__), "sql_registry.yaml"))
SQL_INTENT_LLM_ENABLE = os.getenv("SQL_INTENT_LLM_ENABLE", "true").lower() == "true"
SQL_INTENT_LLM_MIN_CONF = float(os.getenv("SQL_INTENT_LLM_MIN_CONF", "0.6"))


@dataclass
class SQLParamSpec:
    type: str
    required: bool
    aliases: List[str]


@dataclass
class SQLResultSpec:
    mode: str = "table"
    field: str = ""
    empty_message: str = "조회 결과가 없습니다."


@dataclass
class SQLRegistryItem:
    id: str
    description: str
    sql: str
    params: Dict[str, SQLParamSpec]
    result: SQLResultSpec
    keywords: List[str]
    patterns: List[str]
    intent: str = ""
    supported_slots: List[str] = field(default_factory=list)
    default_aggregation: str = "sum"
    supports_compare: bool = False
    supports_trend: bool = False
    groupable_dimensions: List[str] = field(default_factory=list)
    deprecated: bool = False


@dataclass
class SQLRegistryMatch:
    item: SQLRegistryItem
    score: float
    intent: str = ""
    slots: Dict[str, Any] = field(default_factory=dict)
    period: Dict[str, Any] = field(default_factory=dict)
    llm_used: bool = False
    fallback_used: bool = False


@dataclass
class SQLExecutionPlanStep:
    query_id: str
    role: str = "primary"
    reason: str = ""


@dataclass
class SQLSourceSpec:
    id: str
    table: str
    period_column: str
    snapshot_column: str
    version_column: str
    quarter_column: str = ""
    description: str = ""
    latest_snapshot_strategy: str = "max_snapshot_with_filters"
    default_filters: List[str] = field(default_factory=list)
    dimensions: List[str] = field(default_factory=list)
    columns: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass
class SQLMetricSpec:
    id: str
    aliases: List[str]
    source: str
    value_column: str
    unit: str = "MEQ"
    description: str = ""
    semantic_type: str = "additive"
    default_aggregation: str = "sum"
    allowed_aggregations: List[str] = field(default_factory=lambda: ["sum", "avg", "max", "min", "latest"])
    numerator_column: str = ""
    denominator_column: str = ""
    percent_scale: str = "percent"


@dataclass
class SQLDimensionSpec:
    id: str
    source: str
    column: str
    aliases: List[str] = field(default_factory=list)
    supports_filter: bool = True
    supports_groupby: bool = True
    value_mode: str = "free_text"
    sample_values: List[str] = field(default_factory=list)
    value_aliases: Dict[str, str] = field(default_factory=dict)
    description: str = ""


@dataclass
class SQLQueryFamilySpec:
    id: str
    source: str
    description: str = ""


_SQL_REGISTRY_CACHE: List[SQLRegistryItem] = []
_SQL_REGISTRY_MTIME: float = -1.0
_LAST_SQL_NLU_TRACE: Dict[str, Any] = {}
_SQL_INTENT_LLM_CLASSIFIER: Optional[Callable[[str, Dict[str, Any]], Optional[Dict[str, Any]]]] = None
_SQL_SOURCES: Dict[str, SQLSourceSpec] = {}
_SQL_METRICS: Dict[str, SQLMetricSpec] = {}
_SQL_DIMENSIONS: Dict[str, SQLDimensionSpec] = {}
_SQL_QUERY_FAMILIES: Dict[str, SQLQueryFamilySpec] = {}
_SQL_METRIC_ALIAS_MAP: Dict[str, str] = {}
_SQL_DIMENSION_ALIAS_MAP: Dict[str, str] = {}

_LEGACY_FAMILY_TO_ANALYSIS_TYPE = {
    "total_single_period": "total",
    "total_period_range": "total",
    "trend_by_period": "trend",
    "compare_versions_same_period": "compare",
    "compare_period_groups": "compare",
    "grouped_by_dimension": "grouped",
}

_ANALYSIS_TYPE_TO_DYNAMIC_QUERY_ID = {
    "total": "total",
    "trend": "trend",
    "compare": "compare",
    "compare_groups": "compare_groups",
    "grouped": "grouped",
}

_COMMON_VERSION_VALUE_ALIASES = {
    "vh": "VH",
    "v/h": "VH",
    "vl": "VL",
    "v/l": "VL",
    "wc": "WC",
    "w/c": "WC",
}

_COMMON_METRIC_ALIASES = {
    "sales": "sales",
    "qty": "sales",
    "quantity": "sales",
    "출하": "sales",
    "production": "net_prod",
    "production qty": "net_prod",
    "생산량": "net_prod",
    "net production": "net_prod",
    "ipgo": "net_ipgo",
}

_COMMON_DIMENSION_ALIASES = {
    "model": "version",
    "모델": "version",
    "모델별": "version",
    "version": "version",
    "버전": "version",
    "버전별": "version",
    "월별": "yearmonth",
    "분기별": "quarter",
    "app별": "app",
    "그룹별": "",
}

_COMMON_ANALYSIS_ALIASES = {
    "trend": ["추이", "트렌드", "흐름", "trend", "flow"],
    "compare": ["비교", "대비", "vs", "versus", "compare"],
    "grouped": ["별", "별로", "기준", "그룹별"],
}

_NORMALIZATION_REPLACEMENTS = {
    "sales trend": "판매 추이",
    "sales compare": "판매 비교",
    "sales": "판매",
    "qty": "판매",
    "quantity": "판매",
    "trend": "추이",
    "flow": "흐름",
    "compare": "비교",
    "versus": "비교",
    "vs": "비교",
    "model": "버전",
    "group by": "그룹별",
    "groupby": "그룹별",
    "production qty": "생산량",
    "production": "생산",
    "ipgo": "입고",
}

_FILLER_WORDS = [
    "알려줘", "알려줄래", "보여줘", "보여줄래", "말해줘", "부탁해", "해줘", "해주세요", "좀", "한번",
]


_STOPWORDS = {
    "판매", "판매량", "매출", "실적", "합계", "조회", "알려줘", "알려", "몇개", "몇", "수량", "월", "년월",
    "sql", "데이터", "값", "이번", "지난", "이번달", "저번달", "요청", "분기", "올해", "작년",
}

_METRIC_WORDS = {
    "sales": ["판매", "판매량", "매출", "실적", "수량", "출하", "sales"],
    "net_prod": ["순생산", "생산", "net production", "net_prod"],
    "net_ipgo": ["순입고", "입고", "net ipgo", "net_ipgo"],
}

_AGG_WORDS = {
    "sum": ["합계", "총", "총합", "누적", "얼마", "몇개", "몇 개"],
    "avg": ["평균"],
    "max": ["최대", "피크"],
    "min": ["최소"],
    "latest": ["최신", "최근값", "마지막", "월말", "latest"],
}

_SUPPORTED_AGGREGATIONS = {"sum", "avg", "max", "min", "latest", "weighted_avg"}
_SEMANTIC_DEFAULT_AGGREGATION = {
    "additive": "sum",
    "ratio": "avg",
    "snapshot": "latest",
}

_TREND_WORDS = ["추이", "트렌드", "흐름"]
_COMPARE_WORDS = {
    "prev_month": ["전월대비", "전월 대비", "지난달대비", "지난달 대비"],
    "prev_quarter": ["전분기대비", "전분기 대비", "전분기", "지난분기대비"],
    "prev_year": ["전년대비", "전년 대비", "작년대비", "작년 대비"],
}

_DIMENSION_WORDS = {
    "version": ["버전별", "버전 별", "version별", "version 별"],
    "yearmonth": ["월별", "월 별", "월단위", "월 단위"],
    "quarter": ["분기별", "분기 별"],
    "fam1": ["fam1별", "fam1 별", "family1별", "패밀리1별"],
}

_GROUP_BY_HINTS = ["별", "별로", "기준", "그룹별"]

_PERIOD_RELATIVE_WORDS = {
    "this_year": ["올해", "금년"],
    "last_year": ["작년", "전년"],
    "this_month": ["이번달", "당월", "금월"],
    "last_month": ["지난달", "전월", "저번달"],
    "this_quarter": ["이번분기", "이번 분기", "금분기"],
    "prev_quarter": ["전분기", "지난분기", "전 분기"],
}

_INTENT_VALUES = {
    "sales_total",
    "sales_trend",
    "sales_grouped",
    "sales_compare",
    "metric_compare_versions",
    "metric_trend_by_period",
    "metric_compare_period_groups",
    "metric_grouped_dimension",
}


def configure_sql_intent_llm_classifier(
    classifier: Optional[Callable[[str, Dict[str, Any]], Optional[Dict[str, Any]]]]
) -> None:
    global _SQL_INTENT_LLM_CLASSIFIER
    _SQL_INTENT_LLM_CLASSIFIER = classifier


def get_last_sql_nlu_trace() -> Dict[str, Any]:
    return dict(_LAST_SQL_NLU_TRACE)


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _normalize_version_token(token: str) -> str:
    raw = str(token or "").strip().lower()
    if not raw:
        return ""
    compact = re.sub(r"[^a-z0-9]", "", raw)
    for alias, canonical in _COMMON_VERSION_VALUE_ALIASES.items():
        alias_compact = re.sub(r"[^a-z0-9]", "", alias.lower())
        if compact == alias_compact:
            return canonical
    return raw.upper()


def _shift_yyyymm(yyyymm: str, delta: int) -> str:
    value = str(yyyymm or "")
    if not re.fullmatch(r"20\d{2}(0[1-9]|1[0-2])", value):
        return ""
    year = int(value[:4])
    month = int(value[4:6])
    total = (year * 12 + (month - 1)) + delta
    next_year = total // 12
    next_month = (total % 12) + 1
    return f"{next_year:04d}{next_month:02d}"


def _latest_complete_yyyymm(now: Optional[datetime] = None) -> str:
    current = now or datetime.now()
    this_month = f"{current.year:04d}{current.month:02d}"
    return _shift_yyyymm(this_month, -1)


def normalize_question(text: str) -> str:
    q = (text or "").strip().lower()
    q = re.sub(r"(?<!\d)(\d{2})\s*년(?!\d)", r"20\1년", q)
    q = re.sub(r"\bv\s*[/\-]?\s*h\b", " vh ", q, flags=re.IGNORECASE)
    q = re.sub(r"\bv\s*[/\-]?\s*l\b", " vl ", q, flags=re.IGNORECASE)
    q = re.sub(r"\bw\s*[/\-]?\s*c\b", " wc ", q, flags=re.IGNORECASE)
    q = re.sub(r"\b(vh|vl|wc)\s*[,/]\s*(vh|vl|wc)\b", r"\1 비교 \2", q, flags=re.IGNORECASE)
    q = re.sub(r"[,/(){}\[\]|]+", " ", q)
    q = re.sub(r"([a-z0-9])([가-힣])", r"\1 \2", q)
    q = re.sub(r"([가-힣])([a-z0-9])", r"\1 \2", q)
    q = re.sub(r"([0-9])([a-z])", r"\1 \2", q)
    q = re.sub(r"([a-z])([0-9])", r"\1 \2", q)
    q = re.sub(r"\bfam\s+(\d+)\b", r"fam\1", q)
    for src, dst in sorted(_NORMALIZATION_REPLACEMENTS.items(), key=lambda x: len(x[0]), reverse=True):
        q = re.sub(rf"(?<![a-z0-9가-힣]){re.escape(src)}(?![a-z0-9가-힣])", f" {dst} ", q, flags=re.IGNORECASE)
    for filler in _FILLER_WORDS:
        q = q.replace(filler, " ")
    q = re.sub(r"\s+", " ", q).strip()
    return q


def _contains_any(haystack: str, needles: List[str]) -> bool:
    return any(n in haystack for n in needles)


def _as_param_spec(raw: Dict[str, Any]) -> SQLParamSpec:
    return SQLParamSpec(
        type=str(raw.get("type") or "string").strip().lower(),
        required=bool(raw.get("required", False)),
        aliases=[str(x).strip() for x in (raw.get("aliases") or []) if str(x).strip()],
    )


def _as_result_spec(raw: Dict[str, Any]) -> SQLResultSpec:
    return SQLResultSpec(
        mode=str(raw.get("mode") or "table").strip().lower(),
        field=str(raw.get("field") or "").strip(),
        empty_message=str(raw.get("empty_message") or "조회 결과가 없습니다.").strip(),
    )


def _quarter_label_from_yyyymm(yyyymm: str) -> str:
    value = str(yyyymm or "").strip()
    m = re.fullmatch(r"(20\d{2})(0[1-9]|1[0-2])", value)
    if not m:
        return value
    year = int(m.group(1))
    month = int(m.group(2))
    quarter = ((month - 1) // 3) + 1
    return f"{year:04d}Q{quarter}"


def _quarter_range_labels(start_yyyymm: str, end_yyyymm: str) -> Tuple[str, str]:
    return _quarter_label_from_yyyymm(start_yyyymm), _quarter_label_from_yyyymm(end_yyyymm)


def _normalize_query_family_id(family: str, analysis_type: str = "", compare_period_groups: bool = False) -> str:
    raw = str(family or "").strip()
    if raw in _SQL_QUERY_FAMILIES:
        return raw
    normalized_analysis = str(analysis_type or "").strip().lower()
    if compare_period_groups and normalized_analysis == "compare":
        normalized_analysis = "compare_groups"
    if normalized_analysis in _ANALYSIS_TYPE_TO_DYNAMIC_QUERY_ID:
        return _ANALYSIS_TYPE_TO_DYNAMIC_QUERY_ID[normalized_analysis]
    return raw


def _analysis_type_from_family(family: str, compare_period_groups: bool = False) -> str:
    raw = str(family or "").strip()
    if compare_period_groups and raw in {"compare", "compare_groups", "compare_period_groups"}:
        return "compare_groups"
    return _LEGACY_FAMILY_TO_ANALYSIS_TYPE.get(raw, raw)


def _normalize_aggregation_name(raw: str) -> str:
    agg = str(raw or "").strip().lower()
    return agg if agg in _SUPPORTED_AGGREGATIONS else ""


def _normalize_metric_semantic_type(raw: str) -> str:
    semantic = str(raw or "").strip().lower()
    return semantic if semantic in _SEMANTIC_DEFAULT_AGGREGATION else "additive"


def _normalize_percent_scale(raw: str) -> str:
    scale = str(raw or "").strip().lower()
    return scale if scale in {"fraction", "percent"} else "percent"


def _resolve_metric_aggregation(metric: SQLMetricSpec, requested: str = "") -> str:
    allowed = [
        _normalize_aggregation_name(x)
        for x in (metric.allowed_aggregations or [])
        if _normalize_aggregation_name(x)
    ]
    semantic_type = _normalize_metric_semantic_type(metric.semantic_type)
    semantic_default = _SEMANTIC_DEFAULT_AGGREGATION.get(semantic_type, "sum")
    if not allowed:
        allowed = [semantic_default]

    default_agg = _normalize_aggregation_name(metric.default_aggregation) or semantic_default
    if default_agg not in allowed:
        default_agg = semantic_default if semantic_default in allowed else allowed[0]

    req = _normalize_aggregation_name(requested)
    if req and req in allowed:
        if metric.unit == "%" and req == "sum":
            print(f"[SQL_REGISTRY] WARN: percent metric '{metric.id}' requested sum -> fallback to avg")
            if "avg" in allowed:
                return "avg"
            return semantic_default if semantic_default in allowed else default_agg
        return req
    if not req and semantic_type == "ratio" and default_agg == "sum":
        return semantic_default if semantic_default in allowed else default_agg
    return default_agg


def _build_aggregation_expr(value_column: str, aggregation: str) -> str:
    agg = _normalize_aggregation_name(aggregation) or "sum"
    if agg == "latest":
        # "latest" semantics are handled by the existing latest_snapshot_filter.
        # Stage-1 behavior keeps numeric roll-up as SUM on that latest snapshot.
        return f"SUM({value_column})"
    if agg == "weighted_avg":
        # TODO(stage-2): implement weighted_avg using metric numerator/denominator columns.
        return f"AVG({value_column})"
    return f"{agg.upper()}({value_column})"


def _parse_semantic_sections(data: Dict[str, Any]) -> None:
    global _SQL_SOURCES, _SQL_METRICS, _SQL_DIMENSIONS, _SQL_QUERY_FAMILIES
    global _SQL_METRIC_ALIAS_MAP, _SQL_DIMENSION_ALIAS_MAP

    sources_raw = data.get("sources") if isinstance(data, dict) else {}
    metrics_raw = data.get("metrics") if isinstance(data, dict) else {}
    dimensions_raw = data.get("dimensions") if isinstance(data, dict) else {}
    families_raw = data.get("query_families") if isinstance(data, dict) else {}

    sources: Dict[str, SQLSourceSpec] = {}
    metrics: Dict[str, SQLMetricSpec] = {}
    dimensions: Dict[str, SQLDimensionSpec] = {}
    families: Dict[str, SQLQueryFamilySpec] = {}
    alias_map: Dict[str, str] = {}
    dim_alias_map: Dict[str, str] = {}

    def _normalize_source_filters(raw: Any) -> List[str]:
        if isinstance(raw, list):
            return [str(x).strip() for x in raw if str(x).strip()]
        if isinstance(raw, dict):
            clauses: List[str] = []
            for key, val in raw.items():
                k = str(key or "").strip().upper()
                v = str(val or "").strip()
                if not k or not re.fullmatch(r"[A-Z_][A-Z0-9_]*", k) or not v:
                    continue
                safe_v = v.replace("'", "''")
                clauses.append(f"{k} = '{safe_v}'")
            return clauses
        return []

    if isinstance(sources_raw, dict):
        for sid, spec in sources_raw.items():
            if not isinstance(spec, dict):
                continue
            source_id = str(sid or "").strip()
            table = str(spec.get("table") or "").strip()
            period_column = str(spec.get("period_column") or "YEARMONTH").strip()
            snapshot_column = str(spec.get("snapshot_column") or spec.get("workdate_column") or "WORKDATE").strip()
            version_column = str(spec.get("version_column") or "VERSION").strip()
            quarter_column = str(spec.get("quarter_column") or "").strip()
            if not source_id or not table:
                continue
            sources[source_id] = SQLSourceSpec(
                id=source_id,
                table=table,
                description=str(spec.get("description") or "").strip(),
                period_column=period_column,
                snapshot_column=snapshot_column,
                version_column=version_column,
                quarter_column=quarter_column,
                latest_snapshot_strategy=str(spec.get("latest_snapshot_strategy") or "max_snapshot_with_filters").strip(),
                default_filters=_normalize_source_filters(spec.get("default_filters")),
                dimensions=[str(x).strip() for x in (spec.get("dimensions") or []) if str(x).strip()],
                columns={
                    str(col).strip(): meta if isinstance(meta, dict) else {}
                    for col, meta in (spec.get("columns") or {}).items()
                    if str(col).strip()
                },
            )

    if isinstance(metrics_raw, dict):
        for mid, spec in metrics_raw.items():
            if not isinstance(spec, dict):
                continue
            metric_id = str(mid or "").strip()
            source = str(spec.get("source") or "").strip()
            value_column = str(spec.get("value_column") or "").strip()
            if not metric_id or not source or not value_column:
                continue
            aliases = [str(x).strip().lower() for x in (spec.get("aliases") or []) if str(x).strip()]
            semantic_type = _normalize_metric_semantic_type(spec.get("semantic_type") or "additive")
            allowed_aggs = [
                _normalize_aggregation_name(str(x))
                for x in (
                    spec.get("allowed_aggregations")
                    or spec.get("supported_aggregations")
                    or [*_SEMANTIC_DEFAULT_AGGREGATION.values()]
                )
                if _normalize_aggregation_name(str(x))
            ]
            if not allowed_aggs:
                allowed_aggs = [_SEMANTIC_DEFAULT_AGGREGATION.get(semantic_type, "sum")]
            default_agg = _normalize_aggregation_name(spec.get("default_aggregation") or "sum")
            if not default_agg:
                default_agg = _SEMANTIC_DEFAULT_AGGREGATION.get(semantic_type, "sum")
            if default_agg not in allowed_aggs:
                fallback = _SEMANTIC_DEFAULT_AGGREGATION.get(semantic_type, "sum")
                default_agg = fallback if fallback in allowed_aggs else allowed_aggs[0]
            metric = SQLMetricSpec(
                id=metric_id,
                aliases=aliases,
                source=source,
                value_column=value_column,
                unit=str(spec.get("unit") or "MEQ").strip() or "MEQ",
                description=str(spec.get("description") or "").strip(),
                semantic_type=semantic_type,
                default_aggregation=default_agg,
                allowed_aggregations=allowed_aggs,
                numerator_column=str(spec.get("numerator_column") or "").strip(),
                denominator_column=str(spec.get("denominator_column") or "").strip(),
                percent_scale=_normalize_percent_scale(spec.get("percent_scale") or "percent"),
            )
            metrics[metric_id] = metric
            alias_map[metric_id.lower()] = metric_id
            for alias in aliases:
                alias_map[alias] = metric_id
    for alias, metric_id in _COMMON_METRIC_ALIASES.items():
        alias_map[str(alias).lower()] = str(metric_id)

    if isinstance(dimensions_raw, dict):
        for did, spec in dimensions_raw.items():
            if not isinstance(spec, dict):
                continue
            dim_id = str(did or "").strip()
            source = str(spec.get("source") or "").strip()
            column = str(spec.get("column") or "").strip()
            if not dim_id or not source or not column:
                continue
            aliases = [str(x).strip().lower() for x in (spec.get("aliases") or []) if str(x).strip()]
            dim = SQLDimensionSpec(
                id=dim_id,
                source=source,
                column=column,
                aliases=aliases,
                supports_filter=bool(spec.get("supports_filter", True)),
                supports_groupby=bool(spec.get("supports_groupby", True)),
                value_mode=str(spec.get("value_mode") or "free_text").strip().lower() or "free_text",
                sample_values=[str(x).strip() for x in (spec.get("sample_values") or []) if str(x).strip()],
                value_aliases={
                    str(k).strip().lower(): str(v).strip()
                    for k, v in (spec.get("value_aliases") or {}).items()
                    if str(k).strip() and str(v).strip()
                },
                description=str(spec.get("description") or "").strip(),
            )
            dimensions[dim_id] = dim
            dim_alias_map[dim_id.lower()] = dim_id
            for alias in aliases:
                dim_alias_map[alias] = dim_id
    for alias, dim_id in _COMMON_DIMENSION_ALIASES.items():
        if dim_id:
            dim_alias_map[str(alias).lower()] = str(dim_id)

    if isinstance(families_raw, dict):
        for fid, spec in families_raw.items():
            if not isinstance(spec, dict):
                continue
            family_id = str(fid or "").strip()
            if not family_id:
                continue
            families[family_id] = SQLQueryFamilySpec(
                id=family_id,
                source=str(spec.get("source") or "").strip(),
                description=str(spec.get("description") or "").strip(),
            )

    for sid, src in list(sources.items()):
        if src.dimensions:
            continue
        src.dimensions = [d.id for d in dimensions.values() if d.source == sid]

    _SQL_SOURCES = sources
    _SQL_METRICS = metrics
    _SQL_DIMENSIONS = dimensions
    _SQL_QUERY_FAMILIES = families
    _SQL_METRIC_ALIAS_MAP = alias_map
    _SQL_DIMENSION_ALIAS_MAP = dim_alias_map


def _load_registry_from_yaml(path: str) -> List[SQLRegistryItem]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    _parse_semantic_sections(data if isinstance(data, dict) else {})

    rows = data.get("queries") if isinstance(data, dict) else data
    if not isinstance(rows, list):
        return []

    out: List[SQLRegistryItem] = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        item_id = str(row.get("id") or "").strip()
        description = str(row.get("description") or "").strip()
        sql = str(row.get("sql") or "").strip()
        if not item_id or not sql:
            continue

        raw_params = row.get("params") or {}
        params: Dict[str, SQLParamSpec] = {}
        if isinstance(raw_params, dict):
            for pname, pspec in raw_params.items():
                if isinstance(pspec, dict):
                    params[str(pname).strip()] = _as_param_spec(pspec)

        raw_result = row.get("result") or {}
        result = _as_result_spec(raw_result if isinstance(raw_result, dict) else {})

        keywords = [str(x).strip().lower() for x in (row.get("keywords") or []) if str(x).strip()]
        patterns = [str(x).strip() for x in (row.get("patterns") or []) if str(x).strip()]

        out.append(
            SQLRegistryItem(
                id=item_id,
                description=description,
                sql=sql,
                params=params,
                result=result,
                keywords=keywords,
                patterns=patterns,
                intent=str(row.get("intent") or "").strip().lower(),
                supported_slots=[str(x).strip().lower() for x in (row.get("supported_slots") or []) if str(x).strip()],
                default_aggregation=str(row.get("default_aggregation") or "sum").strip().lower(),
                supports_compare=bool(row.get("supports_compare", False)),
                supports_trend=bool(row.get("supports_trend", False)),
                groupable_dimensions=[str(x).strip().lower() for x in (row.get("groupable_dimensions") or []) if str(x).strip()],
                deprecated=bool(row.get("deprecated", False)),
            )
        )
    return out


def get_sql_registry_items() -> List[SQLRegistryItem]:
    global _SQL_REGISTRY_CACHE, _SQL_REGISTRY_MTIME

    path = SQL_REGISTRY_YAML_PATH
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return []

    if _SQL_REGISTRY_CACHE and mtime == _SQL_REGISTRY_MTIME:
        return _SQL_REGISTRY_CACHE

    try:
        items = _load_registry_from_yaml(path)
        _SQL_REGISTRY_CACHE = items
        _SQL_REGISTRY_MTIME = mtime
        return items
    except Exception as e:
        print(f"[SQL_REGISTRY] load failed: {e}")
        return _SQL_REGISTRY_CACHE


def get_sql_registry_item_by_id(query_id: str) -> Optional[SQLRegistryItem]:
    qid = str(query_id or "").strip().lower()
    if not qid:
        return None
    for item in get_sql_registry_items():
        if item.id.lower() == qid:
            return item
    return None


def _extract_metric(norm: str) -> str:
    q = norm.lower()
    for alias, metric_id in _SQL_METRIC_ALIAS_MAP.items():
        if alias and alias in q:
            return metric_id
    for metric, words in _METRIC_WORDS.items():
        if _contains_any(q, words):
            return metric
    return ""


def _extract_aggregation(norm: str) -> str:
    for agg, words in _AGG_WORDS.items():
        if _contains_any(norm, words):
            return agg
    return ""


def _extract_period_slots(norm: str, now: Optional[datetime] = None) -> Tuple[str, str]:
    compact = norm.replace(" ", "")
    for key, words in _PERIOD_RELATIVE_WORDS.items():
        if any(w in compact for w in [x.replace(" ", "") for x in words]):
            if "quarter" in key:
                return "quarter", key
            if "year" in key:
                return "year", key
            return "month", key

    m_recent = re.search(r"최근\s*(\d+)\s*개월", norm)
    if m_recent:
        n = max(1, int(m_recent.group(1)))
        return "relative", f"recent_{n}_months"

    m_ym = re.search(r"\b(20\d{2})(0[1-9]|1[0-2])\b", norm)
    if m_ym:
        return "month", f"{m_ym.group(1)}{m_ym.group(2)}"

    m_y_m = re.search(r"(20\d{2})\s*년\s*(\d{1,2})\s*월", norm)
    if m_y_m:
        mm = int(m_y_m.group(2))
        if 1 <= mm <= 12:
            return "month", f"{int(m_y_m.group(1)):04d}{mm:02d}"

    m_q = re.search(r"(20\d{2})\s*년?\s*([1-4])\s*분기", norm)
    if m_q:
        return "quarter", f"{int(m_q.group(1)):04d}q{int(m_q.group(2))}"

    m_q2 = re.search(r"([1-4])\s*분기", norm)
    if m_q2:
        yy = (now or datetime.utcnow()).year
        return "quarter", f"{yy:04d}q{int(m_q2.group(1))}"

    m_y = re.search(r"(20\d{2})\s*년", norm)
    if m_y:
        return "year", f"{int(m_y.group(1)):04d}"

    m_mm = re.search(r"\b(\d{1,2})\s*월", norm)
    if m_mm:
        mm = int(m_mm.group(1))
        if 1 <= mm <= 12:
            return "month", str(mm)

    return "", ""


def _extract_period_debug_info(question: str, norm: str) -> Dict[str, Any]:
    raw_expr = ""
    parsed_year = ""
    parsed_month = ""
    parsed_quarter = ""
    normalized_year = ""

    for pattern in (
        r"((?:20\d{2}|\d{2})\s*년\s*\d{1,2}\s*월)",
        r"((?:20\d{2}|\d{2})\s*년\s*[1-4]\s*분기)",
        r"((?:20\d{2}|\d{2})\s*년)",
        r"(\d{1,2}\s*월)",
        r"([1-4]\s*분기)",
    ):
        m = re.search(pattern, question, flags=re.IGNORECASE)
        if m:
            raw_expr = str(m.group(1) or "").strip()
            break

    m_year_month = re.search(r"(20\d{2})\s*년\s*(\d{1,2})\s*월", norm)
    if m_year_month:
        parsed_year = m_year_month.group(1)
        parsed_month = str(int(m_year_month.group(2)))
        normalized_year = parsed_year
        return {
            "raw_expression": raw_expr,
            "parsed_year": parsed_year,
            "parsed_month": parsed_month,
            "parsed_quarter": parsed_quarter,
            "normalized_year": normalized_year,
            "explicit_year": True,
        }

    m_year_quarter = re.search(r"(20\d{2})\s*년?\s*([1-4])\s*분기", norm)
    if m_year_quarter:
        parsed_year = m_year_quarter.group(1)
        parsed_quarter = str(int(m_year_quarter.group(2)))
        normalized_year = parsed_year
        return {
            "raw_expression": raw_expr,
            "parsed_year": parsed_year,
            "parsed_month": parsed_month,
            "parsed_quarter": parsed_quarter,
            "normalized_year": normalized_year,
            "explicit_year": True,
        }

    m_year = re.search(r"(20\d{2})\s*년", norm)
    if m_year:
        parsed_year = m_year.group(1)
        normalized_year = parsed_year

    m_month = re.search(r"(?<!\d)(1[0-2]|0?[1-9])\s*월", norm)
    if m_month:
        parsed_month = str(int(m_month.group(1)))

    m_quarter = re.search(r"([1-4])\s*분기", norm)
    if m_quarter:
        parsed_quarter = str(int(m_quarter.group(1)))

    return {
        "raw_expression": raw_expr,
        "parsed_year": parsed_year,
        "parsed_month": parsed_month,
        "parsed_quarter": parsed_quarter,
        "normalized_year": normalized_year,
        "explicit_year": bool(normalized_year),
    }


def _extract_periods(question: str, norm: str, now: Optional[datetime] = None) -> List[str]:
    current = now or datetime.now()
    periods: List[str] = []
    for yy, mm in re.findall(r"\b(20\d{2})(0[1-9]|1[0-2])\b", norm):
        periods.append(f"{yy}{mm}")
    if periods:
        return list(dict.fromkeys(periods))

    month_nums = re.findall(r"(?<!\d)(1[0-2]|0?[1-9])\s*월", norm)
    for mm in month_nums:
        periods.append(f"{current.year:04d}{int(mm):02d}")
    return list(dict.fromkeys(periods))


def _extract_dimension(norm: str) -> str:
    q = norm.lower()
    compact = q.replace(" ", "")
    group_signal = any(tok in compact for tok in _GROUP_BY_HINTS)
    for alias, dim_id in _SQL_DIMENSION_ALIAS_MAP.items():
        if not alias:
            continue
        dim_spec = _SQL_DIMENSIONS.get(dim_id)
        if dim_spec is not None:
            alias_lower = alias.lower()
            sample_lowers = {str(x).lower() for x in (dim_spec.sample_values or [])}
            if alias_lower in dim_spec.value_aliases or alias_lower in sample_lowers:
                continue
        if alias in {"버전", "version", "월", "년월", "month"}:
            if group_signal and alias.replace(" ", "") in compact:
                return dim_id
            continue
        if alias in q:
            return dim_id
    if group_signal:
        for dim_id, spec in _SQL_DIMENSIONS.items():
            for alias in spec.aliases:
                if alias and alias.replace(" ", "") in compact:
                    return dim_id
    for dim, words in _DIMENSION_WORDS.items():
        if _contains_any(norm, words):
            return dim
    return ""


def _extract_compare(norm: str) -> str:
    compact = norm.replace(" ", "")
    for key, words in _COMPARE_WORDS.items():
        if any(w.replace(" ", "") in compact for w in words):
            return key
    if any(x in compact for x in [w.replace(" ", "") for w in _COMMON_ANALYSIS_ALIASES["compare"]] + ["차이"]):
        return "compare_versions"
    if "와" in compact or "과" in compact:
        return "compare_versions"
    return ""


def _extract_versions(question: str, norm: str) -> List[str]:
    tokens = re.findall(r"[A-Za-z][A-Za-z0-9_/\-]{0,12}", (question or "") + " " + (norm or ""))
    stop = {
        "SQL", "VERSION", "SALES", "SUM", "AVG", "MAX", "MIN", "COUNT",
        "MONTH", "YEAR", "QUARTER", "THIS", "LAST", "RECENT", "VS", "AND",
        "NET", "PRODUCTION", "IPGO", "COMPARE", "ANALYSIS", "TREND", "FLOW",
        "FAM", "APP",
    }

    versions: List[str] = []
    for tok in tokens:
        up = _normalize_version_token(tok)
        low = tok.lower()
        if up in stop:
            continue
        if low in _SQL_DIMENSION_ALIAS_MAP or low in _SQL_METRIC_ALIAS_MAP:
            continue
        if re.fullmatch(r"fam\d+", low):
            continue
        if low in {"fam", "app"}:
            continue
        if re.fullmatch(r"20\d{2}", up):
            continue
        if re.fullmatch(r"[A-Z]{1,4}", up) or re.fullmatch(r"[A-Z]{1,3}\d{1,2}", up):
            if up not in versions:
                versions.append(up)

    compact = re.sub(r"\s+", " ", norm.lower()).strip()
    m = re.findall(r"\b(vh|vl|wc)\b", compact, flags=re.IGNORECASE)
    for tok in m:
        up = tok.upper()
        if up not in versions:
            versions.append(up)
    return versions


def resolve_metric(question: str, slots: Dict[str, Any]) -> str:
    metric = str((slots or {}).get("metric") or "").strip().lower()
    if metric in _SQL_METRICS:
        return metric
    if metric in _SQL_METRIC_ALIAS_MAP:
        return _SQL_METRIC_ALIAS_MAP[metric]
    return _extract_metric(normalize_question(question))


def resolve_versions(question: str, slots: Dict[str, Any]) -> List[str]:
    versions = [str(x).strip().upper() for x in ((slots or {}).get("versions") or []) if str(x).strip()]
    extracted_versions = _extract_versions(question, normalize_question(question))
    for v in extracted_versions:
        if v not in versions:
            versions.append(v)
    single = str((slots or {}).get("version") or "").strip().upper()
    if single and single not in versions:
        versions.append(single)
    uniq: List[str] = []
    for v in versions:
        if v and v not in uniq:
            uniq.append(v)
    return uniq


def resolve_source_for_metric(metric: str) -> str:
    spec = _SQL_METRICS.get(str(metric or ""))
    return str(spec.source) if spec else ""


def resolve_source(plan_source: str, metric: str, dimension: str) -> str:
    source = str(plan_source or "").strip()
    if source and source in _SQL_SOURCES:
        return source
    metric_spec = _SQL_METRICS.get(str(metric or ""))
    if metric_spec:
        return metric_spec.source
    dim_spec = _SQL_DIMENSIONS.get(str(dimension or ""))
    if dim_spec:
        return dim_spec.source
    return ""


def resolve_dimension(dimension: str, question: str, slots: Dict[str, Any]) -> str:
    dim = str(dimension or "").strip().lower()
    if dim in _SQL_DIMENSIONS:
        return dim
    if dim in _SQL_DIMENSION_ALIAS_MAP:
        return _SQL_DIMENSION_ALIAS_MAP[dim]
    from_slots = str((slots or {}).get("dimension") or "").strip().lower()
    if from_slots in _SQL_DIMENSIONS:
        return from_slots
    if from_slots in _SQL_DIMENSION_ALIAS_MAP:
        return _SQL_DIMENSION_ALIAS_MAP[from_slots]
    return _extract_dimension(normalize_question(question))


def normalize_dimension_value(dimension_name: str, raw_value: str) -> str:
    dim = _SQL_DIMENSIONS.get(str(dimension_name or "").strip().lower())
    value = str(raw_value or "").strip().strip(",.()[]{}")
    if not value:
        return ""
    if dim is None:
        return value.upper()

    lowered = value.lower()
    if lowered in dim.value_aliases:
        return str(dim.value_aliases[lowered]).strip()
    for sample in dim.sample_values:
        if sample.lower() == lowered:
            return sample
    if dim.value_mode in {"catalog_or_free_text", "free_text"}:
        if re.search(r"[A-Za-z]", value):
            return value.upper()
        return value
    return value


def _extract_dimension_filters(norm: str) -> Dict[str, List[str]]:
    filters: Dict[str, List[str]] = {}
    metric_alias_words = set(_SQL_METRIC_ALIAS_MAP.keys()) | {
        "판매", "판매량", "순생산", "순입고", "생산", "입고", "트렌드", "추이", "비교", "분석", "알려줘", "보여줘",
    }
    for dim_id, dim in _SQL_DIMENSIONS.items():
        if not dim.supports_filter:
            continue
        if dim_id in {"yearmonth", "version"}:
            continue
        aliases = sorted(set([dim_id] + list(dim.aliases or [])), key=len, reverse=True)
        for alias in aliases:
            alias = str(alias or "").strip()
            if not alias:
                continue
            pat = rf"(?<!\w){re.escape(alias)}(?!\s*별)(?:\s*(?:=|:)\s*|\s+)([A-Za-z0-9가-힣_\-]+)"
            for m in re.finditer(pat, norm, flags=re.IGNORECASE):
                raw = str(m.group(1) or "").strip()
                if not raw:
                    continue
                if raw.lower() in metric_alias_words:
                    continue
                normalized = normalize_dimension_value(dim_id, raw)
                if not normalized:
                    continue
                bucket = filters.setdefault(dim_id, [])
                if normalized not in bucket:
                    bucket.append(normalized)

    # Allow direct value mentions like "dram 올해 판매 트렌드" to map to catalog dimensions
    # before generic version-token extraction consumes them as version-like codes.
    for dim_id, dim in _SQL_DIMENSIONS.items():
        if not dim.supports_filter or dim_id in {"yearmonth", "version"}:
            continue
        candidates: Dict[str, str] = {}
        for sample in dim.sample_values:
            normalized = normalize_dimension_value(dim_id, sample)
            if normalized:
                candidates[str(sample).lower()] = normalized
        for raw_alias, mapped in (dim.value_aliases or {}).items():
            normalized = normalize_dimension_value(dim_id, str(mapped))
            if normalized:
                candidates[str(raw_alias).lower()] = normalized

        for token, normalized in candidates.items():
            if not token:
                continue
            token_pattern = rf"(?<![A-Za-z0-9_]){re.escape(token)}(?![A-Za-z0-9_])"
            if re.search(token_pattern, norm, flags=re.IGNORECASE):
                bucket = filters.setdefault(dim_id, [])
                if normalized not in bucket:
                    bucket.append(normalized)
    return filters


def resolve_filters(question: str, slots: Dict[str, Any]) -> Dict[str, List[str]]:
    merged: Dict[str, List[str]] = {}
    raw = (slots or {}).get("filters")
    if isinstance(raw, dict):
        for k, vals in raw.items():
            dim = resolve_dimension(str(k), question, slots)
            if not dim:
                continue
            items = vals if isinstance(vals, list) else [vals]
            for v in items:
                nv = normalize_dimension_value(dim, str(v))
                if not nv:
                    continue
                bucket = merged.setdefault(dim, [])
                if nv not in bucket:
                    bucket.append(nv)

    inferred = _extract_dimension_filters(normalize_question(question))
    for dim, vals in inferred.items():
        bucket = merged.setdefault(dim, [])
        for v in vals:
            if v not in bucket:
                bucket.append(v)
    return merged


def resolve_periods(question: str, slots: Dict[str, Any], *, now: Optional[datetime] = None) -> List[str]:
    current = now or datetime.now()
    periods = [str(x).strip() for x in ((slots or {}).get("periods") or []) if str(x).strip()]

    norm = normalize_question(question)
    explicit_year = str((slots or {}).get("period_normalized_year") or "").strip()
    year_for_periods = int(explicit_year) if re.fullmatch(r"20\d{2}", explicit_year) else current.year
    if len(periods) == 2 and ("부터" in norm and "까지" in norm):
        try:
            sm = int(str(periods[0])[4:6])
            em = int(str(periods[1])[4:6])
            sy = int(str(periods[0])[:4])
            ey = int(str(periods[1])[:4])
            if sy == ey and 1 <= sm <= em <= 12:
                periods = [f"{sy:04d}{mm:02d}" for mm in range(sm, em + 1)]
        except Exception:
            pass
    if not periods:
        explicit = re.findall(r"\b(20\d{2})(0[1-9]|1[0-2])\b", norm)
        for yy, mm in explicit:
            periods.append(f"{yy}{mm}")

    if not periods:
        m_range = re.search(r"(1[0-2]|0?[1-9])\s*월\s*부터\s*(1[0-2]|0?[1-9])\s*월\s*까지", norm)
        if m_range:
            sm = int(m_range.group(1))
            em = int(m_range.group(2))
            if sm <= em:
                for mm in range(sm, em + 1):
                    periods.append(f"{year_for_periods:04d}{mm:02d}")

    if not periods:
        m_recent = re.search(r"최근\s*(\d+)\s*개월", norm)
        if m_recent:
            n = max(1, int(m_recent.group(1)))
            yy, mm = current.year, current.month
            acc = []
            for _ in range(n):
                acc.append(f"{yy:04d}{mm:02d}")
                mm -= 1
                if mm == 0:
                    yy -= 1
                    mm = 12
            periods.extend(reversed(acc))

    if not periods and ("상반기" in norm or "하반기" in norm):
        if "상반기" in norm:
            rng = range(1, 7)
        else:
            rng = range(7, 13)
        for mm in rng:
            periods.append(f"{year_for_periods:04d}{mm:02d}")

    if not periods:
        m_q = re.search(r"([1-4])\s*분기", norm)
        if m_q:
            q = int(m_q.group(1))
            start = (q - 1) * 3 + 1
            for mm in range(start, start + 3):
                periods.append(f"{year_for_periods:04d}{mm:02d}")

    if not periods:
        month_nums = re.findall(r"(?<!\d)(1[0-2]|0?[1-9])\s*월", norm)
        for m in month_nums:
            periods.append(f"{year_for_periods:04d}{int(m):02d}")

    if not periods:
        pv = str((slots or {}).get("period_value") or "")
        if re.fullmatch(r"20\d{2}(0[1-9]|1[0-2])", pv):
            periods.append(pv)

    uniq: List[str] = []
    for p in periods:
        if re.fullmatch(r"20\d{2}(0[1-9]|1[0-2])", p) and p not in uniq:
            uniq.append(p)
    return uniq


def resolve_period_groups(question: str, slots: Dict[str, Any], *, now: Optional[datetime] = None) -> List[Dict[str, str]]:
    current = now or datetime.now()
    norm = normalize_question(question)
    seeded = [x for x in ((slots or {}).get("period_groups") or []) if isinstance(x, dict)]
    if seeded:
        return seeded
    groups: List[Dict[str, str]] = []

    for a, b in re.findall(r"\b(20\d{2})\s*년?\s*대비\s*(20\d{2})\s*년?\b", norm):
        groups.append({"label": a, "start_yyyymm": f"{a}01", "end_yyyymm": f"{a}12"})
        groups.append({"label": b, "start_yyyymm": f"{b}01", "end_yyyymm": f"{b}12"})
        return groups

    for a, b in re.findall(r"\b(\d{2})\s*년\s*대비\s*(\d{2})\s*년\b", norm):
        yy1 = f"20{int(a):02d}"
        yy2 = f"20{int(b):02d}"
        groups.append({"label": yy1, "start_yyyymm": f"{yy1}01", "end_yyyymm": f"{yy1}12"})
        groups.append({"label": yy2, "start_yyyymm": f"{yy2}01", "end_yyyymm": f"{yy2}12"})
        return groups

    compact = norm.replace(" ", "")
    if ("작년" in compact and "올해" in compact) or ("전년" in compact and "올해" in compact):
        y1 = current.year - 1
        y2 = current.year
        groups.append({"label": str(y1), "start_yyyymm": f"{y1:04d}01", "end_yyyymm": f"{y1:04d}12"})
        groups.append({"label": str(y2), "start_yyyymm": f"{y2:04d}01", "end_yyyymm": f"{y2:04d}12"})
    return groups


def infer_query_family(question: str, slots: Dict[str, Any]) -> str:
    norm = normalize_question(question)
    versions = resolve_versions(question, slots)
    periods = resolve_periods(question, slots)
    period_groups = resolve_period_groups(question, slots)
    filters = resolve_filters(question, slots)
    compare = bool(slots.get("compare")) or any(k in norm for k in ("비교", "대비", "차이", "vs", "versus"))
    trend = bool(slots.get("trend"))
    dimension = resolve_dimension(str(slots.get("dimension") or ""), question, slots)
    compact = norm.replace(" ", "")

    if period_groups and compare:
        return "compare"
    if trend or any(k in norm for k in ("트렌드", "추이", "흐름")):
        return "trend"
    if dimension and any(k in compact for k in ("별", "기준", "그룹")):
        return "grouped"
    if dimension and dimension not in filters and dimension in {"fam1", "app"}:
        return "grouped"
    if compare and len(versions) >= 2:
        return "compare"
    if len(periods) >= 2:
        return "total"
    return "total"


def canonicalize_plan(question: str, slots: Dict[str, Any], *, now: Optional[datetime] = None) -> Dict[str, Any]:
    metric = resolve_metric(question, slots) or "sales"
    dimension = resolve_dimension(str(slots.get("dimension") or ""), question, slots)
    source = resolve_source(str((slots or {}).get("source") or ""), metric, dimension)
    versions = resolve_versions(question, slots)
    periods = resolve_periods(question, slots, now=now)
    period_groups = resolve_period_groups(question, slots, now=now)
    filters = resolve_filters(question, slots)

    filter_values_upper = {
        str(v).strip().upper()
        for vals in filters.values()
        for v in (vals if isinstance(vals, list) else [vals])
        if str(v).strip()
    }
    if filter_values_upper:
        versions = [v for v in versions if str(v).strip().upper() not in filter_values_upper]
    if not versions and filters.get("version"):
        versions = [str(v).upper() for v in filters.get("version") or [] if str(v).strip()]
    if versions and "version" in filters:
        del filters["version"]

    norm = normalize_question(question)
    compact = norm.replace(" ", "")
    compare = bool((slots or {}).get("compare")) or _extract_compare(norm) != "" or len(versions) >= 2
    trend = bool((slots or {}).get("trend")) or any(w in compact for w in [x.replace(" ", "") for x in _COMMON_ANALYSIS_ALIASES["trend"]])
    grouped_hint = bool((slots or {}).get("group_requested")) or any(tok in compact for tok in _GROUP_BY_HINTS)
    analysis = bool((slots or {}).get("analysis"))

    compare_target = ""
    group_by = ""
    if compare and period_groups:
        analysis_type = "compare"
        compare_target = "period_groups"
    elif compare and len(versions) >= 2:
        analysis_type = "compare"
        compare_target = "versions"
        group_by = "version"
    elif trend:
        analysis_type = "trend"
    elif dimension in {"fam1", "app"} and not filters.get(dimension) and not versions:
        analysis_type = "grouped"
        group_by = dimension
    elif dimension and grouped_hint:
        analysis_type = "grouped"
        group_by = dimension
    else:
        analysis_type = "total"

    family = _normalize_query_family_id("", analysis_type=analysis_type, compare_period_groups=(compare_target == "period_groups"))
    return {
        "source": source,
        "metric": metric,
        "aggregation": _normalize_aggregation_name(str((slots or {}).get("aggregation") or "")),
        "periods": periods,
        "period_groups": period_groups,
        "filters": filters,
        "applied_filters": filters,
        "group_by": group_by,
        "dimension": dimension,
        "analysis_type": analysis_type,
        "versions": versions,
        "compare_target": compare_target,
        "compare": compare,
        "trend": trend,
        "analysis": analysis,
        "family": family,
        "inferred_defaults": list(slots.get("inferred_defaults") or []),
        "compare_basis": str(slots.get("compare_basis") or ""),
        "applied_periods": dict(slots.get("applied_periods") or {}),
        "raw_question": question,
        "normalized_question": norm,
        "intent_hint": str((slots or {}).get("intent_hint") or ""),
    }


def build_latest_snapshot_filter(source: SQLSourceSpec) -> str:
    period_column = _safe_identifier(source.period_column)
    snapshot_column = _safe_identifier(source.snapshot_column)
    table = _safe_identifier(source.table)
    default_filter_sql = _build_default_filter_sql(source.default_filters)
    return (
        f"{snapshot_column} = (SELECT MAX({snapshot_column}) FROM {table} "
        f"WHERE {period_column} BETWEEN :start_yyyymm AND :end_yyyymm{default_filter_sql})"
    )


def resolve_dimension_column(dimension_name: str, source_id: str) -> str:
    dim = _SQL_DIMENSIONS.get(str(dimension_name or "").strip().lower())
    if dim is None or dim.source != source_id:
        raise ValueError(f"unsupported dimension: {dimension_name}")
    return _safe_identifier(dim.column)


def build_dimension_filters(
    filters: Dict[str, List[str]],
    *,
    source_id: str,
    params: Dict[str, SQLParamSpec],
) -> Tuple[str, Dict[str, List[str]]]:
    clauses: List[str] = []
    normalized: Dict[str, List[str]] = {}
    for raw_dim, raw_vals in (filters or {}).items():
        dim_id = str(raw_dim or "").strip().lower()
        dim = _SQL_DIMENSIONS.get(dim_id)
        if dim is None or dim.source != source_id or not dim.supports_filter:
            continue
        values = raw_vals if isinstance(raw_vals, list) else [raw_vals]
        cleaned: List[str] = []
        for v in values:
            nv = normalize_dimension_value(dim_id, str(v))
            if nv and nv not in cleaned:
                cleaned.append(nv)
        if not cleaned:
            continue
        dim_col = _safe_identifier(dim.column)
        bind_names: List[str] = []
        for idx, _ in enumerate(cleaned, start=1):
            pname = f"{dim_id}_{idx}"
            params[pname] = SQLParamSpec(type="string", required=True, aliases=[pname])
            bind_names.append(f":{pname}")
        clauses.append(f" AND UPPER({dim_col}) IN ({', '.join(bind_names)})")
        normalized[dim_id] = cleaned
    return "".join(clauses), normalized


def build_total_sql(
    *,
    table: str,
    value_column: str,
    latest_snapshot_filter: str,
    default_filter_sql: str,
    period_filter: str,
    version_filter: str,
    dimension_filter_sql: str,
    aggregation: str,
) -> str:
    agg_expr = _build_aggregation_expr(value_column, aggregation)
    return f"""
SELECT NVL({agg_expr}, 0) AS VALUE
FROM {table}
WHERE 1=1
  AND {latest_snapshot_filter}
  {default_filter_sql}
  {period_filter}
  {version_filter}
  {dimension_filter_sql}
""".strip()


def build_trend_sql(
    *,
    table: str,
    period_column: str,
    version_column: str,
    value_column: str,
    latest_snapshot_filter: str,
    default_filter_sql: str,
    period_filter: str,
    version_filter: str,
    dimension_filter_sql: str,
    aggregation: str,
) -> str:
    agg_expr = _build_aggregation_expr(value_column, aggregation)
    return f"""
SELECT {period_column} AS PERIOD, UPPER({version_column}) AS VERSION, NVL({agg_expr}, 0) AS VALUE
FROM {table}
WHERE 1=1
  AND {latest_snapshot_filter}
  {default_filter_sql}
  {period_filter}
  {version_filter}
  {dimension_filter_sql}
GROUP BY {period_column}, UPPER({version_column})
ORDER BY {period_column}, UPPER({version_column})
""".strip()


def build_compare_versions_sql(
    *,
    table: str,
    version_column: str,
    value_column: str,
    latest_snapshot_filter: str,
    default_filter_sql: str,
    period_filter: str,
    version_filter: str,
    dimension_filter_sql: str,
    aggregation: str,
) -> str:
    agg_expr = _build_aggregation_expr(value_column, aggregation)
    return f"""
SELECT UPPER({version_column}) AS VERSION, NVL({agg_expr}, 0) AS VALUE
FROM {table}
WHERE 1=1
  AND {latest_snapshot_filter}
  {default_filter_sql}
  {period_filter}
  {version_filter}
  {dimension_filter_sql}
GROUP BY UPPER({version_column})
ORDER BY UPPER({version_column})
""".strip()


def build_groupby_dimension_sql(
    *,
    table: str,
    dimension_column: str,
    value_column: str,
    latest_snapshot_filter: str,
    default_filter_sql: str,
    period_filter: str,
    version_filter: str,
    dimension_filter_sql: str,
    aggregation: str,
) -> str:
    agg_expr = _build_aggregation_expr(value_column, aggregation)
    return f"""
SELECT {dimension_column} AS DIMENSION_VALUE, NVL({agg_expr}, 0) AS VALUE
FROM {table}
WHERE 1=1
  AND {latest_snapshot_filter}
  {default_filter_sql}
  {period_filter}
  {version_filter}
  {dimension_filter_sql}
GROUP BY {dimension_column}
ORDER BY VALUE DESC
""".strip()


def build_execution_plan_from_slots(question: str, slots: Dict[str, Any], *, now: Optional[datetime] = None) -> Dict[str, Any]:
    # 2nd-pass automation canonicalizes noisy expressions into a stable builder plan.
    return canonicalize_plan(question, slots, now=now)


def build_sql_from_plan(plan: Dict[str, Any], *, period: Dict[str, Any]) -> Optional[SQLRegistryMatch]:
    source_id = str(plan.get("source") or "")
    metric_id = str(plan.get("metric") or "")
    period_groups = [x for x in (plan.get("period_groups") or []) if isinstance(x, dict)]
    analysis_type = str(plan.get("analysis_type") or _analysis_type_from_family(str(plan.get("family") or ""), bool(period_groups))).strip().lower()
    family = _normalize_query_family_id(str(plan.get("family") or ""), analysis_type=analysis_type, compare_period_groups=bool(period_groups))
    dimension_id = str(plan.get("group_by") or plan.get("dimension") or "")
    intent_hint = str(plan.get("intent_hint") or "")
    versions = [str(x).strip().upper() for x in (plan.get("versions") or []) if str(x).strip()]
    periods = [str(x).strip() for x in (plan.get("periods") or []) if str(x).strip()]
    filters = dict(plan.get("filters") or {})

    if family not in _SQL_QUERY_FAMILIES:
        return None

    metric = _SQL_METRICS.get(metric_id)
    if metric is None:
        return None
    aggregation = _resolve_metric_aggregation(metric, str(plan.get("aggregation") or ""))
    source_id = resolve_source(source_id, metric_id, dimension_id) or metric.source
    source = _SQL_SOURCES.get(source_id)
    if source is None or metric.source != source.id:
        return None
    family_spec = _SQL_QUERY_FAMILIES.get(family)
    if family_spec and family_spec.source and family_spec.source != source.id:
        return None

    if not periods:
        anchor = str(period.get("anchor_yyyymm") or "")
        start = str(period.get("start_yyyymm") or anchor)
        end = str(period.get("end_yyyymm") or anchor)
        if start and end:
            periods = [start] if start == end else [start, end]

    if dimension_id:
        dimension_id = resolve_dimension(dimension_id, str(plan.get("raw_question") or ""), plan)
    if not filters:
        filters = resolve_filters(str(plan.get("raw_question") or ""), plan)
    filter_values_upper = {
        str(v).strip().upper()
        for vals in filters.values()
        for v in (vals if isinstance(vals, list) else [vals])
        if str(v).strip()
    }
    if filter_values_upper:
        versions = [v for v in versions if str(v).strip().upper() not in filter_values_upper]
    if not versions and filters.get("version"):
        versions = [str(v).strip().upper() for v in filters.get("version") or [] if str(v).strip()]
    if "version" in filters:
        del filters["version"]
    if analysis_type == "compare" and not period_groups and len(versions) >= 2:
        dimension_id = "version"
    if analysis_type == "grouped" and not dimension_id:
        return None

    dim_spec = _SQL_DIMENSIONS.get(dimension_id) if dimension_id else None
    if dim_spec is not None and dim_spec.source != source.id:
        return None

    table = _safe_identifier(source.table)
    period_column = _safe_identifier(source.period_column)
    snapshot_column = _safe_identifier(source.snapshot_column)
    version_column = _safe_identifier(source.version_column)
    value_column = _safe_identifier(metric.value_column)
    default_filter_sql = _build_default_filter_sql(source.default_filters)
    period_type = str((period or {}).get("period_type") or (period or {}).get("type") or "").strip().lower()
    use_quarter_column = bool(source.quarter_column) and period_type == "quarter"
    active_period_column = _safe_identifier(source.quarter_column) if use_quarter_column else period_column

    params: Dict[str, SQLParamSpec] = {}
    slot_meta: Dict[str, Any] = {
        "metric": metric_id,
        "metric_unit": metric.unit,
        "metric_semantic_type": metric.semantic_type,
        "percent_scale": metric.percent_scale,
        "source_name": source.id,
        "versions": versions,
        "periods": periods,
        "period_groups": period_groups,
        "dimension": dimension_id,
        "group_by": dimension_id,
        "analysis_type": analysis_type,
        "family": family,
        "aggregation": aggregation,
        "compare": bool(plan.get("compare")),
        "trend": bool(plan.get("trend")),
        "analysis": bool(plan.get("analysis")),
        "raw_question": str(plan.get("raw_question") or ""),
        "normalized_question": str(plan.get("normalized_question") or ""),
        "filters": {},
        "applied_filters": dict(plan.get("applied_filters") or {}),
        "inferred_defaults": list(plan.get("inferred_defaults") or []),
        "compare_basis": str(plan.get("compare_basis") or ""),
        "applied_periods": dict(plan.get("applied_periods") or {}),
    }

    period_filter = ""
    if use_quarter_column:
        quarter_start, quarter_end = _quarter_range_labels(period.get("start_yyyymm") or "", period.get("end_yyyymm") or "")
        if quarter_start and quarter_end and quarter_start == quarter_end:
            params["anchor_quarter"] = SQLParamSpec(type="quarter", required=True, aliases=["anchor_quarter"])
            period_filter = f" AND {active_period_column} = :anchor_quarter"
        else:
            params["start_quarter"] = SQLParamSpec(type="quarter", required=True, aliases=["start_quarter"])
            params["end_quarter"] = SQLParamSpec(type="quarter", required=True, aliases=["end_quarter"])
            period_filter = f" AND {active_period_column} BETWEEN :start_quarter AND :end_quarter"
    elif periods:
        if len(periods) == 1:
            params["anchor_yyyymm"] = SQLParamSpec(type="yyyymm", required=True, aliases=["anchor_yyyymm"])
            period_filter = f" AND {active_period_column} = :anchor_yyyymm"
        elif len(periods) == 2 and all(re.fullmatch(r"20\d{2}(0[1-9]|1[0-2])", p) for p in periods):
            params["start_yyyymm"] = SQLParamSpec(type="yyyymm", required=True, aliases=["start_yyyymm"])
            params["end_yyyymm"] = SQLParamSpec(type="yyyymm", required=True, aliases=["end_yyyymm"])
            period_filter = f" AND {active_period_column} BETWEEN :start_yyyymm AND :end_yyyymm"
        else:
            p_binds = []
            for idx, _ in enumerate(periods, start=1):
                key = f"p{idx}"
                params[key] = SQLParamSpec(type="yyyymm", required=True, aliases=[key])
                p_binds.append(f":{key}")
            period_filter = f" AND {active_period_column} IN ({', '.join(p_binds)})"
    else:
        params["start_yyyymm"] = SQLParamSpec(type="yyyymm", required=True, aliases=["start_yyyymm"])
        params["end_yyyymm"] = SQLParamSpec(type="yyyymm", required=True, aliases=["end_yyyymm"])
        period_filter = f" AND {active_period_column} BETWEEN :start_yyyymm AND :end_yyyymm"

    version_filter = ""
    if versions:
        v_binds = []
        for idx, _ in enumerate(versions, start=1):
            key = f"v{idx}"
            params[key] = SQLParamSpec(type="string", required=True, aliases=[key])
            v_binds.append(f":{key}")
        version_filter = f" AND UPPER({version_column}) IN ({', '.join(v_binds)})"

    dimension_filter_sql, normalized_filters = build_dimension_filters(
        filters,
        source_id=source.id,
        params=params,
    )
    slot_meta["filters"] = normalized_filters

    snapshot_period_filter = f"{active_period_column} BETWEEN :start_yyyymm AND :end_yyyymm"
    if "anchor_quarter" in params:
        snapshot_period_filter = f"{active_period_column} = :anchor_quarter"
    elif "start_quarter" in params and "end_quarter" in params:
        snapshot_period_filter = f"{active_period_column} BETWEEN :start_quarter AND :end_quarter"
    elif "anchor_yyyymm" in params:
        snapshot_period_filter = f"{active_period_column} = :anchor_yyyymm"
    elif any(k.startswith("p") and k[1:].isdigit() for k in params):
        pkeys = [k for k in params.keys() if re.fullmatch(r"p\d+", k)]
        snapshot_period_filter = f"{active_period_column} IN ({', '.join([f':{k}' for k in pkeys])})"
    elif len(period_groups) >= 2:
        snapshot_period_filter = " OR ".join(
            [f"({active_period_column} BETWEEN :g{i}_start AND :g{i}_end)" for i in range(1, len(period_groups) + 1)]
        )
    latest_snapshot_filter = (
        f"{snapshot_column} = (SELECT MAX({snapshot_column}) FROM {table} "
        f"WHERE ({snapshot_period_filter}){default_filter_sql}{version_filter}{dimension_filter_sql})"
    )

    if family == "compare":
        if len(versions) < 2:
            return None
        sql = build_compare_versions_sql(
            table=table,
            version_column=version_column,
            value_column=value_column,
            latest_snapshot_filter=latest_snapshot_filter,
            default_filter_sql=default_filter_sql,
            period_filter=period_filter,
            version_filter=version_filter,
            dimension_filter_sql=dimension_filter_sql,
            aggregation=aggregation,
        )
        intent = "metric_compare_versions"
        query_id = "compare"
    elif family == "trend":
        sql = build_trend_sql(
            table=table,
            period_column=active_period_column,
            version_column=version_column,
            value_column=value_column,
            latest_snapshot_filter=latest_snapshot_filter,
            default_filter_sql=default_filter_sql,
            period_filter=period_filter,
            version_filter=version_filter,
            dimension_filter_sql=dimension_filter_sql,
            aggregation=aggregation,
        )
        intent = "metric_trend_by_period"
        query_id = "trend"
    elif family == "grouped":
        if dim_spec is None or not dim_spec.supports_groupby:
            return None
        dim_col = resolve_dimension_column(dimension_id, source.id)
        sql = build_groupby_dimension_sql(
            table=table,
            dimension_column=dim_col,
            value_column=value_column,
            latest_snapshot_filter=latest_snapshot_filter,
            default_filter_sql=default_filter_sql,
            period_filter=period_filter,
            version_filter=version_filter,
            dimension_filter_sql=dimension_filter_sql,
            aggregation=aggregation,
        )
        intent = "metric_grouped_dimension"
        query_id = "grouped"
    elif family == "compare_groups":
        if len(period_groups) < 2:
            return None
        case_parts: List[str] = []
        where_parts: List[str] = []
        for idx, grp in enumerate(period_groups, start=1):
            key_s = f"g{idx}_start"
            key_e = f"g{idx}_end"
            params[key_s] = SQLParamSpec(type="yyyymm", required=True, aliases=[key_s])
            params[key_e] = SQLParamSpec(type="yyyymm", required=True, aliases=[key_e])
            label = str(grp.get("label") or f"G{idx}").replace("'", "''")
            case_parts.append(f"WHEN {active_period_column} BETWEEN :{key_s} AND :{key_e} THEN '{label}'")
            where_parts.append(f"({active_period_column} BETWEEN :{key_s} AND :{key_e})")
        sql = f"""
SELECT CASE {' '.join(case_parts)} ELSE 'OTHER' END AS PERIOD_GROUP,
       NVL({_build_aggregation_expr(value_column, aggregation)}, 0) AS VALUE
FROM {table}
WHERE 1=1
  AND {latest_snapshot_filter}
  {default_filter_sql}
  AND ({' OR '.join(where_parts)})
  {version_filter}
  {dimension_filter_sql}
GROUP BY CASE {' '.join(case_parts)} ELSE 'OTHER' END
HAVING CASE {' '.join(case_parts)} ELSE 'OTHER' END <> 'OTHER'
ORDER BY PERIOD_GROUP
        """.strip()
        intent = "metric_compare_period_groups"
        query_id = "compare_groups"
    elif family == "total":
        sql = build_total_sql(
            table=table,
            value_column=value_column,
            latest_snapshot_filter=latest_snapshot_filter,
            default_filter_sql=default_filter_sql,
            period_filter=period_filter,
            version_filter=version_filter,
            dimension_filter_sql=dimension_filter_sql,
            aggregation=aggregation,
        )
        intent = intent_hint or "sales_total"
        query_id = "total"
    else:
        return None

    item = SQLRegistryItem(
        id=query_id,
        description=f"{family}:{metric_id}",
        sql=sql,
        params=params,
        result=SQLResultSpec(mode="table", field="", empty_message="해당 조건의 데이터가 없습니다."),
        keywords=[],
        patterns=[],
        intent=intent,
        supported_slots=["metric", "versions", "periods", "period_groups", "dimension", "analysis", "compare", "trend"],
        default_aggregation=aggregation,
        supports_compare=family == "compare",
        supports_trend=family == "trend",
        groupable_dimensions=list(_SQL_DIMENSIONS.keys()),
        deprecated=False,
    )

    period_payload = dict(period or {})
    if periods:
        if len(periods) == 1:
            period_payload.setdefault("start_yyyymm", periods[0])
            period_payload.setdefault("end_yyyymm", periods[0])
            period_payload.setdefault("anchor_yyyymm", periods[0])
        elif len(periods) >= 2:
            period_payload.setdefault("start_yyyymm", min(periods))
            period_payload.setdefault("end_yyyymm", max(periods))
    for idx, grp in enumerate(period_groups, start=1):
        period_payload[f"g{idx}_start"] = str(grp.get("start_yyyymm") or "")
        period_payload[f"g{idx}_end"] = str(grp.get("end_yyyymm") or "")

    return SQLRegistryMatch(
        item=item,
        score=30.0,
        intent=intent,
        slots=slot_meta,
        period=period_payload,
        llm_used=False,
        fallback_used=False,
    )


def _extract_version(question: str, norm: str) -> str:
    # explicit key-value first
    m_kv = re.search(r"(?:version|버전)\s*[:=]\s*([A-Za-z0-9_\-]{1,16})", question, flags=re.IGNORECASE)
    if m_kv:
        return _normalize_version_token(m_kv.group(1))

    m_after = re.search(r"(?:version|버전)\s*([A-Za-z0-9_\-]{1,16})", question, flags=re.IGNORECASE)
    if m_after:
        return _normalize_version_token(m_after.group(1))

    tokens = re.findall(r"[A-Za-z][A-Za-z0-9_\-]{1,12}", question)
    stop = {
        "SQL", "VERSION", "SALES", "SUM", "AVG", "MAX", "MIN", "COUNT",
        "MONTH", "YEAR", "QUARTER", "THIS", "LAST", "RECENT", "TREND", "FLOW",
    }
    for tok in tokens:
        up = tok.upper()
        low = tok.lower()
        if up in stop:
            continue
        if low in _SQL_DIMENSION_ALIAS_MAP or low in _SQL_METRIC_ALIAS_MAP:
            continue
        if re.fullmatch(r"fam\d+", low):
            continue
        if re.fullmatch(r"20\d{2}", up):
            continue
        return _normalize_version_token(up)

    if " vh " in f" {norm} ":
        return "VH"
    return ""


def extract_slots_rule_based(question: str, *, now: Optional[datetime] = None) -> Dict[str, Any]:
    norm = normalize_question(question)
    compact = norm.replace(" ", "")
    period_debug = _extract_period_debug_info(question, norm)

    metric = _extract_metric(norm)
    aggregation = _extract_aggregation(norm)
    period_type, period_value = _extract_period_slots(norm, now=now)
    periods = _extract_periods(question, norm, now=now)
    dimension = _extract_dimension(norm)
    compare = _extract_compare(norm)
    trend = any(w.replace(" ", "") in compact for w in [*_TREND_WORDS, *_COMMON_ANALYSIS_ALIASES["trend"]])
    version = _extract_version(question, norm)
    versions = _extract_versions(question, norm)
    analysis = any(x in norm for x in ("분석", "해석", "비교 분석", "비교분석"))
    filters = _extract_dimension_filters(norm)
    group_requested = any(x in compact for x in _GROUP_BY_HINTS)

    if trend and not dimension:
        dimension = "yearmonth"
    if "버전별" in norm.replace(" ", ""):
        dimension = "version"
    if "fam1별" in norm.replace(" ", "") or "fam1 기준" in norm.replace(" ", ""):
        dimension = "fam1"
    if version and version not in versions:
        versions.append(version)
    if not versions and filters.get("version"):
        versions = [str(v).strip().upper() for v in filters.get("version") or [] if str(v).strip()]
    if "version" in filters:
        del filters["version"]

    if compare:
        if not period_value or period_value in ("prev_quarter", "last_month", "last_year"):
            if compare == "prev_month":
                period_type, period_value = "month", "this_month"
            elif compare == "prev_quarter":
                period_type, period_value = "quarter", "this_quarter"
            elif compare == "prev_year":
                period_type, period_value = "year", "this_year"

    slots: Dict[str, Any] = {
        "metric": metric or "sales",
        "aggregation": aggregation,
        "period_type": period_type,
        "period_value": period_value,
        "periods": periods,
        "dimension": dimension,
        "version": version,
        "versions": versions,
        "compare": compare,
        "compare_flag": bool(compare),
        "analysis": analysis,
        "trend": trend,
        "group_requested": group_requested,
        "filters": filters,
        "period_raw_expression": str(period_debug.get("raw_expression") or ""),
        "period_parsed_year": str(period_debug.get("parsed_year") or ""),
        "period_parsed_month": str(period_debug.get("parsed_month") or ""),
        "period_parsed_quarter": str(period_debug.get("parsed_quarter") or ""),
        "period_normalized_year": str(period_debug.get("normalized_year") or ""),
        "period_has_explicit_year": bool(period_debug.get("explicit_year")),
    }
    return slots


def classify_intent_rule_based(question: str, slots: Dict[str, Any]) -> Tuple[str, bool, List[str]]:
    norm = normalize_question(question)
    compact = norm.replace(" ", "")
    group_signal = any(x in compact for x in ("별", "기준"))
    versions = resolve_versions(question, slots)
    metric = resolve_metric(question, slots) or "sales"
    period_groups = resolve_period_groups(question, slots)
    compare_token = str(slots.get("compare") or "")
    compare_requested = bool(compare_token) or len(versions) >= 2

    scores = {
        "sales_total": 0.4,
        "sales_trend": 0.0,
        "sales_grouped": 0.0,
        "sales_compare": 0.0,
        "metric_compare_versions": 0.0,
        "metric_trend_by_period": 0.0,
        "metric_compare_period_groups": 0.0,
        "metric_grouped_dimension": 0.0,
    }

    if slots.get("compare"):
        scores["sales_compare"] += 2.2
    if compare_requested and len(versions) >= 2 and metric in ("sales", "net_prod", "net_ipgo"):
        scores["metric_compare_versions"] += 3.0
    if compare_requested and len(period_groups) >= 2 and metric in ("sales", "net_prod", "net_ipgo"):
        scores["metric_compare_period_groups"] += 3.2
    if slots.get("trend"):
        scores["sales_trend"] += 2.2
        if metric in ("sales", "net_prod", "net_ipgo"):
            scores["metric_trend_by_period"] += 2.8
    if group_signal and slots.get("dimension") in ("version", "yearmonth", "month", "quarter", "fam1"):
        scores["sales_grouped"] += 1.6
        scores["metric_grouped_dimension"] += 2.1
    if any(x in compact for x in ("버전별", "version별", "분기별", "월별", "fam1별", "기준")):
        scores["sales_grouped"] += 1.0
        scores["metric_grouped_dimension"] += 1.2
    if slots.get("aggregation"):
        scores["sales_total"] += 0.5

    ordered = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    intent = ordered[0][0]
    gap = ordered[0][1] - ordered[1][1]

    reasons: List[str] = []
    if slots.get("compare"):
        reasons.append("compare keyword")
    if len(versions) >= 2:
        reasons.append("multi-version detected")
    if len(period_groups) >= 2:
        reasons.append("period-groups detected")
    if slots.get("trend"):
        reasons.append("trend keyword")
    if slots.get("dimension"):
        reasons.append("dimension detected")

    ambiguous = gap < 0.75
    if intent == "sales_grouped" and not slots.get("dimension"):
        ambiguous = True
    if not slots.get("period_value") and not slots.get("version") and not slots.get("dimension") and len(versions) < 2:
        ambiguous = True

    return intent, ambiguous, reasons


def infer_default_period(intent: str, slots: Dict[str, Any], question: str, *, now: Optional[datetime] = None) -> Tuple[Dict[str, Any], bool, str]:
    merged = dict(slots or {})
    if merged.get("period_value") or merged.get("periods") or merged.get("period_groups"):
        return merged, False, ""
    if merged.get("period_has_explicit_year"):
        return merged, False, ""

    norm = normalize_question(question)
    current = now or datetime.now()
    latest_complete = _latest_complete_yyyymm(current)
    previous_month = _shift_yyyymm(latest_complete, -1)
    reason = ""
    inferred = False
    inferred_defaults = list(merged.get("inferred_defaults") or [])

    compare_requested = bool(merged.get("compare")) or _extract_compare(norm) != ""
    trend_requested = bool(merged.get("trend")) or any(tok in norm.replace(" ", "") for tok in ("추이", "트렌드", "흐름"))
    grouped_requested = bool(merged.get("group_requested")) or any(tok in norm.replace(" ", "") for tok in _GROUP_BY_HINTS)
    versions = [str(v).strip().upper() for v in (merged.get("versions") or []) if str(v).strip()]
    dimension = str(merged.get("dimension") or "").strip().lower()
    analysis_hint = "total"
    if grouped_requested and dimension and not versions and not merged.get("period_groups"):
        analysis_hint = "grouped"
    elif compare_requested:
        analysis_hint = "compare"
    elif trend_requested:
        analysis_hint = "trend"
    elif grouped_requested or dimension in {"version", "fam1", "app", "yearmonth", "quarter"}:
        analysis_hint = "grouped"

    def add_default(field: str, applied: str, note: str) -> None:
        inferred_defaults.append({"field": field, "applied": applied, "note": note})

    if analysis_hint == "grouped" and not dimension:
        merged["dimension"] = "version"
        add_default("group_by", "version", "그룹 차원이 없어 버전별 기준으로 조회했습니다.")
        inferred = True

    if analysis_hint == "compare" and not versions and not merged.get("period_groups"):
        current_label = _format_yyyymm_for_group(latest_complete)
        current_group = {"label": current_label, "start_yyyymm": latest_complete, "end_yyyymm": latest_complete}
        previous_group = {"label": _format_yyyymm_for_group(previous_month), "start_yyyymm": previous_month, "end_yyyymm": previous_month}
        merged["period_type"] = "month"
        merged["period_value"] = latest_complete
        merged["compare"] = merged.get("compare") or "prev_month"
        merged["period_groups"] = [previous_group, current_group]
        merged["compare_basis"] = "최신 월 vs 전월"
        add_default("period", f"latest_complete_month:{latest_complete}", f"비교 대상이 없어 최신 완결 월({current_label})과 전월을 비교했습니다.")
        add_default("compare_basis", "latest_month_vs_prev_month", "비교 대상이 없어 최신 월 vs 전월 기준을 적용했습니다.")
        inferred = True
        reason = f"비교 대상이 없어 최신 완결 월({current_label})과 전월을 비교했습니다."
    elif analysis_hint == "compare" and versions:
        merged["period_type"] = "month"
        merged["period_value"] = latest_complete
        merged["compare_basis"] = "최신 완결 월 단일 기준"
        add_default("period", f"latest_complete_month:{latest_complete}", f"기간 지정이 없어 최신 완결 월({_format_yyyymm_for_group(latest_complete)}) 기준으로 비교했습니다.")
        inferred = True
        reason = f"기간 지정이 없어 최신 완결 월({_format_yyyymm_for_group(latest_complete)}) 기준으로 비교했습니다."
    elif analysis_hint == "trend":
        merged["period_type"] = "relative"
        merged["period_value"] = "recent_3_months"
        if not merged.get("dimension"):
            merged["dimension"] = "yearmonth"
        merged["compare_basis"] = ""
        add_default("period", "recent_3_months", "기간 지정이 없어 최근 3개월 추이로 해석했습니다.")
        inferred = True
        reason = "기간 지정이 없어 최근 3개월 추이로 해석했습니다."
    elif analysis_hint == "grouped":
        merged["period_type"] = "month"
        merged["period_value"] = latest_complete
        add_default("period", f"latest_complete_month:{latest_complete}", f"기간 지정이 없어 최신 완결 월({_format_yyyymm_for_group(latest_complete)}) 기준으로 조회했습니다.")
        inferred = True
        reason = f"기간 지정이 없어 최신 완결 월({_format_yyyymm_for_group(latest_complete)}) 기준으로 조회했습니다."
    else:
        merged["period_type"] = "month"
        merged["period_value"] = latest_complete
        add_default("period", f"latest_complete_month:{latest_complete}", f"기간 지정이 없어 최신 완결 월({_format_yyyymm_for_group(latest_complete)}) 기준으로 조회했습니다.")
        inferred = True
        reason = f"기간 지정이 없어 최신 완결 월({_format_yyyymm_for_group(latest_complete)}) 기준으로 조회했습니다."

    if inferred_defaults:
        merged["inferred_defaults"] = inferred_defaults

    return merged, inferred, reason


def _format_yyyymm_for_group(yyyymm: str) -> str:
    value = str(yyyymm or "")
    if re.fullmatch(r"20\d{2}(0[1-9]|1[0-2])", value):
        return f"{value[:4]}-{value[4:]}"
    return value


def _sanitize_slots(raw_slots: Any) -> Dict[str, Any]:
    if not isinstance(raw_slots, dict):
        return {}
    out: Dict[str, Any] = {}
    allowed_keys = {
        "metric", "aggregation", "period_type", "period_value",
        "periods", "period_groups", "dimension", "version", "versions", "compare", "compare_flag", "analysis", "trend",
        "metric_unit", "metric_semantic_type", "percent_scale", "source_name", "source", "family", "filters",
    }
    for k, v in raw_slots.items():
        kk = str(k).strip().lower()
        if kk not in allowed_keys:
            continue
        out[kk] = v
    return out


def _maybe_classify_with_llm(question: str, trace: Dict[str, Any]) -> Tuple[str, Dict[str, Any], bool, bool]:
    rule_intent = str(trace.get("rule_intent") or "sales_total")
    slots = dict(trace.get("slots") or {})

    if not SQL_INTENT_LLM_ENABLE or _SQL_INTENT_LLM_CLASSIFIER is None:
        return rule_intent, slots, False, True
    if not bool(trace.get("ambiguous")):
        return rule_intent, slots, False, False

    ctx = {
        "normalized_question": trace.get("normalized_question"),
        "rule_intent": rule_intent,
        "slots": slots,
        "allowed_intents": sorted(_INTENT_VALUES),
    }

    try:
        llm_res = _SQL_INTENT_LLM_CLASSIFIER(question, ctx) or {}
        if not isinstance(llm_res, dict):
            return rule_intent, slots, True, True

        intent = str(llm_res.get("intent") or "").strip().lower()
        conf = float(llm_res.get("confidence") or 0.0)
        llm_slots = _sanitize_slots(llm_res.get("slots"))

        if intent not in _INTENT_VALUES or conf < SQL_INTENT_LLM_MIN_CONF:
            return rule_intent, slots, True, True

        merged = dict(slots)
        for k, v in llm_slots.items():
            if v not in (None, ""):
                merged[k] = v

        trace["llm_intent_result"] = {"intent": intent, "confidence": conf, "slots": llm_slots}
        return intent, merged, True, False
    except Exception as e:
        trace["llm_intent_error"] = str(e)
        return rule_intent, slots, True, True


def _safe_identifier(name: str) -> str:
    value = str(name or "").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"invalid identifier: {name}")
    return value


def select_query_family(intent: str, slots: Dict[str, Any], metric: str) -> str:
    if intent == "metric_compare_versions" and len(slots.get("versions") or []) >= 2:
        return "compare"
    if intent == "metric_compare_period_groups":
        return "compare"
    if intent == "metric_grouped_dimension":
        return "grouped"
    if intent in ("sales_trend", "metric_trend_by_period"):
        return "trend"
    if len(slots.get("periods") or []) >= 2:
        return "total"
    if intent in ("sales_total", "sales_grouped"):
        return "total"
    return ""


def _build_default_filter_sql(default_filters: List[str]) -> str:
    if not default_filters:
        return ""
    return "".join([f" AND {flt}" for flt in default_filters if flt])


def build_compare_plan(metric: str, versions: List[str], period: Dict[str, Any]) -> Optional[SQLRegistryMatch]:
    metric_spec = _SQL_METRICS.get(metric)
    if metric_spec is None or len(versions) < 2:
        return None
    plan = {
        "source": metric_spec.source,
        "metric": metric,
        "analysis_type": "compare",
        "family": "compare",
        "versions": versions,
        "periods": [],
        "analysis": bool(period.get("analysis") if isinstance(period, dict) else False),
    }
    return build_sql_from_plan(plan, period=period)


def _score_item(question_norm: str, item: SQLRegistryItem) -> float:
    score = 0.0

    if item.id.lower() in question_norm:
        score += 3.0

    for kw in item.keywords:
        if kw and kw in question_norm:
            score += 1.0

    for pat in item.patterns:
        try:
            if re.search(pat, question_norm, flags=re.IGNORECASE):
                score += 1.5
        except re.error:
            continue

    desc = item.description.lower()
    if desc and desc in question_norm:
        score += 1.0

    for _, spec in item.params.items():
        for alias in spec.aliases:
            if alias.lower() in question_norm:
                score += 0.7

    if item.deprecated:
        score -= 0.5
    return score


def _select_query_template(intent: str, slots: Dict[str, Any], question_norm: str) -> Optional[SQLRegistryMatch]:
    items = get_sql_registry_items()
    if not items:
        return None

    candidates: List[Tuple[SQLRegistryItem, float]] = []
    for item in items:
        score = _score_item(question_norm, item)

        if item.intent:
            if item.intent == intent:
                score += 3.0
            else:
                score -= 2.0

        if slots.get("trend") and item.supports_trend:
            score += 0.8
        if slots.get("compare") and item.supports_compare:
            score += 1.0

        dim = str(slots.get("dimension") or "").lower()
        if dim:
            if dim in item.groupable_dimensions:
                score += 0.8
            elif item.groupable_dimensions:
                score -= 0.6

        if slots.get("version") and "version" in item.params:
            score += 0.4

        period_type = str(slots.get("period_type") or "")
        if item.id == "sales_total_month":
            score += 0.9 if period_type == "month" else -0.8
        if item.id == "sales_total_period_range":
            score += 0.9 if period_type in ("year", "quarter", "relative", "") else -0.2
        if item.id == "sales_trend_monthly":
            score += 1.0 if slots.get("trend") else 0.0
        if item.id == "sales_grouped_by_version":
            score += 1.0 if str(slots.get("dimension") or "") == "version" else -0.5
        if item.id == "sales_compare_periods":
            score += 1.0 if slots.get("compare") else -0.5

        if score > 0:
            candidates.append((item, score))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[1], reverse=True)
    return SQLRegistryMatch(item=candidates[0][0], score=float(candidates[0][1]))


def build_match_for_query_id(
    query_id: str,
    *,
    slots: Dict[str, Any],
    period: Dict[str, Any],
    intent: str,
    llm_used: bool = False,
    fallback_used: bool = False,
    score: float = 10.0,
) -> Optional[SQLRegistryMatch]:
    item = get_sql_registry_item_by_id(query_id)
    if item is None:
        if str(query_id or "") in {
            "total",
            "trend",
            "compare",
            "grouped",
            "compare_groups",
            "compare_versions_same_period",
            "trend_by_period",
            "total_single_period",
            "total_period_range",
            "grouped_by_dimension",
            "compare_period_groups",
        }:
            metric = str((slots or {}).get("metric") or "sales")
            versions = [str(x).strip().upper() for x in ((slots or {}).get("versions") or []) if str(x).strip()]
            periods = [str(x).strip() for x in ((slots or {}).get("periods") or []) if str(x).strip()]
            period_groups = [x for x in ((slots or {}).get("period_groups") or []) if isinstance(x, dict)]
            source = resolve_source_for_metric(metric)
            dynamic = build_sql_from_plan(
                {
                    "source": source,
                    "metric": metric,
                    "analysis_type": _analysis_type_from_family(str(query_id or ""), str(query_id or "") in {"compare_groups", "compare_period_groups"}),
                    "family": str(query_id or ""),
                    "aggregation": str((slots or {}).get("aggregation") or ""),
                    "versions": versions,
                    "periods": periods,
                    "period_groups": period_groups,
                    "dimension": str((slots or {}).get("dimension") or ""),
                    "group_by": str((slots or {}).get("group_by") or (slots or {}).get("dimension") or ""),
                    "filters": dict((slots or {}).get("filters") or {}),
                    "analysis": bool((slots or {}).get("analysis")),
                },
                period=period,
            )
            if dynamic is None:
                return None
            dynamic.intent = intent
            dynamic.slots = dict(slots or {})
            dynamic.period = dict(period or {})
            dynamic.llm_used = llm_used
            dynamic.fallback_used = fallback_used
            dynamic.score = score
            return dynamic
        return None
    return SQLRegistryMatch(
        item=item,
        score=score,
        intent=intent,
        slots=dict(slots or {}),
        period=dict(period or {}),
        llm_used=llm_used,
        fallback_used=fallback_used,
    )


def build_execution_plan(question: str, intent: str, slots: Dict[str, Any], selected_query_id: str) -> List[SQLExecutionPlanStep]:
    plan: List[SQLExecutionPlanStep] = []
    analysis_type = str((slots or {}).get("analysis_type") or "").strip().lower()
    primary = selected_query_id or ""
    if not primary:
        if analysis_type == "total":
            primary = "total"
        elif analysis_type == "trend":
            primary = "trend"
        elif analysis_type == "compare":
            primary = "compare_groups" if len(slots.get("period_groups") or []) >= 2 else "compare"
        elif analysis_type == "grouped":
            primary = "grouped"
    if not primary:
        if intent == "sales_total":
            ptype = str(slots.get("period_type") or "")
            primary = "sales_total_month" if ptype == "month" else "sales_total_period_range"
        elif intent == "sales_trend":
            primary = "sales_trend_monthly"
        elif intent == "sales_grouped":
            primary = "sales_grouped_by_version"
        elif intent == "sales_compare":
            primary = "sales_compare_periods"
        elif intent == "metric_compare_versions":
            primary = "compare_versions_same_period"
        elif intent == "metric_trend_by_period":
            primary = "trend_by_period"
        elif intent == "metric_compare_period_groups":
            primary = "compare_period_groups"
        elif intent == "metric_grouped_dimension":
            primary = "grouped_by_dimension"

    if primary:
        plan.append(SQLExecutionPlanStep(query_id=primary, role="primary", reason="intent-primary"))

    qnorm = normalize_question(question)
    if analysis_type == "total" or intent == "sales_total":
        # 질문이 단순 합계일 때 요약 품질 향상을 위해 월별 breakdown 보조 조회
        if (slots.get("version") or slots.get("versions")) and not slots.get("trend") and "추이" not in qnorm:
            plan.append(SQLExecutionPlanStep(query_id="trend", role="aux", reason="common-plan-breakdown"))
    elif analysis_type == "trend" or intent == "sales_trend":
        # 추이 질문은 총합 요약 보조값도 함께 조회
        plan.append(SQLExecutionPlanStep(query_id="total", role="aux", reason="common-plan-total-summary"))

    uniq: List[SQLExecutionPlanStep] = []
    seen = set()
    for step in plan:
        if step.query_id in seen:
            continue
        seen.add(step.query_id)
        uniq.append(step)
    return uniq


def analyze_sql_question(question: str, *, now: Optional[datetime] = None) -> Dict[str, Any]:
    # ensure semantic sections(sources/metrics/query_families) are loaded
    get_sql_registry_items()
    norm = normalize_question(question)
    slots = extract_slots_rule_based(question, now=now)
    rule_intent, ambiguous, reasons = classify_intent_rule_based(question, slots)

    trace: Dict[str, Any] = {
        "original_question": question,
        "normalized_question": norm,
        "slots": dict(slots),
        "rule_intent": rule_intent,
        "rule_reasons": reasons,
        "ambiguous": ambiguous,
        "fallback_used": False,
    }

    final_intent, merged_slots, llm_used, fallback_used = _maybe_classify_with_llm(question, trace)
    trace["llm_used"] = llm_used
    trace["fallback_used"] = fallback_used
    merged_slots["metric"] = resolve_metric(question, merged_slots) or "sales"
    metric_spec = _SQL_METRICS.get(str(merged_slots["metric"]))
    if metric_spec:
        merged_slots["metric_unit"] = metric_spec.unit
        merged_slots["metric_semantic_type"] = metric_spec.semantic_type
        merged_slots["percent_scale"] = metric_spec.percent_scale
        merged_slots["source_name"] = metric_spec.source
        merged_slots["aggregation"] = _resolve_metric_aggregation(
            metric_spec,
            str(merged_slots.get("aggregation") or ""),
        )
    else:
        merged_slots["aggregation"] = _normalize_aggregation_name(str(merged_slots.get("aggregation") or "")) or "sum"
    merged_slots["versions"] = resolve_versions(question, merged_slots)
    merged_slots["filters"] = resolve_filters(question, merged_slots)
    filter_values_upper = {
        str(v).strip().upper()
        for vals in (merged_slots.get("filters") or {}).values()
        for v in (vals if isinstance(vals, list) else [vals])
        if str(v).strip()
    }
    if filter_values_upper:
        merged_slots["versions"] = [
            v for v in (merged_slots.get("versions") or []) if str(v).strip().upper() not in filter_values_upper
        ]
    merged_slots["periods"] = resolve_periods(question, merged_slots, now=now)
    merged_slots["period_groups"] = resolve_period_groups(question, merged_slots, now=now)
    if not merged_slots.get("versions") and (merged_slots.get("filters") or {}).get("version"):
        merged_slots["versions"] = [str(v).upper() for v in (merged_slots["filters"] or {}).get("version") or []]
    if (merged_slots.get("filters") or {}).get("version"):
        merged_slots["filters"].pop("version", None)
    if len(merged_slots.get("versions") or []) >= 2:
        merged_slots["compare"] = True
    merged_slots, period_inferred, period_infer_reason = infer_default_period(final_intent, merged_slots, question, now=now)
    trace["period_inferred"] = period_inferred
    trace["period_infer_reason"] = period_infer_reason

    period = resolve_period_slots(merged_slots, now=now)
    period_debug = {
        "raw_expression": str(merged_slots.get("period_raw_expression") or ""),
        "parsed_year": str(merged_slots.get("period_parsed_year") or ""),
        "parsed_month": str(merged_slots.get("period_parsed_month") or ""),
        "parsed_quarter": str(merged_slots.get("period_parsed_quarter") or ""),
        "normalized_year": str(merged_slots.get("period_normalized_year") or ""),
        "explicit_year": bool(merged_slots.get("period_has_explicit_year")),
        "final_period": {
            "type": period.period_type,
            "value": period.period_value,
            "start_yyyymm": period.start_yyyymm,
            "end_yyyymm": period.end_yyyymm,
            "anchor_yyyymm": period.anchor_yyyymm,
            "label": period.label,
        },
    }
    trace["period_debug"] = period_debug
    print(
        "[SQL_PERIOD] "
        f"raw_expression={period_debug['raw_expression']!r} "
        f"parsed_year={period_debug['parsed_year']!r} "
        f"parsed_month={period_debug['parsed_month']!r} "
        f"parsed_quarter={period_debug['parsed_quarter']!r} "
        f"normalized_year={period_debug['normalized_year']!r} "
        f"final_period={period_debug['final_period']}"
    )
    merged_slots["applied_periods"] = {
        "period_type": period.period_type,
        "period_value": period.period_value,
        "start_yyyymm": period.start_yyyymm,
        "end_yyyymm": period.end_yyyymm,
        "anchor_yyyymm": period.anchor_yyyymm,
        "label": period.label,
        "compare_start_yyyymm": period.compare_start_yyyymm,
        "compare_end_yyyymm": period.compare_end_yyyymm,
    }
    merged_slots["applied_filters"] = dict(merged_slots.get("filters") or {})
    trace["resolved_period"] = {
        "type": period.period_type,
        "value": period.period_value,
        "start_yyyymm": period.start_yyyymm,
        "end_yyyymm": period.end_yyyymm,
        "anchor_yyyymm": period.anchor_yyyymm,
        "label": period.label,
        "ambiguous_adjusted": period.ambiguous_adjusted,
        "compare_start_yyyymm": period.compare_start_yyyymm,
        "compare_end_yyyymm": period.compare_end_yyyymm,
        "inferred_defaults": list(merged_slots.get("inferred_defaults") or []),
        "compare_basis": str(merged_slots.get("compare_basis") or ""),
    }

    planner_plan = build_execution_plan_from_slots(question, merged_slots, now=now)
    planner_plan["intent_hint"] = final_intent
    planner_plan["applied_periods"] = dict(merged_slots.get("applied_periods") or {})
    planner_plan["applied_filters"] = dict(merged_slots.get("applied_filters") or {})
    planner_plan["inferred_defaults"] = list(merged_slots.get("inferred_defaults") or [])
    planner_plan["compare_basis"] = str(merged_slots.get("compare_basis") or "")
    if planner_plan.get("source") and planner_plan.get("metric") and planner_plan.get("family"):
        built_match = build_sql_from_plan(
            planner_plan,
            period={
                **trace["resolved_period"],
                "analysis": bool(merged_slots.get("analysis")),
            },
        )
        if built_match is not None:
            built_match.slots = {**dict(merged_slots), **dict(built_match.slots or {})}
            built_match.period = {**dict(trace["resolved_period"]), **dict(built_match.period or {})}
            built_match.llm_used = llm_used
            built_match.fallback_used = fallback_used
            trace["planner_plan"] = planner_plan
            trace["selected_query_id"] = built_match.item.id
            trace["final_intent"] = built_match.intent or final_intent
            trace["final_slots"] = built_match.slots
            trace["match_score"] = built_match.score
            trace["match"] = built_match
            return trace

    selected = _select_query_template(final_intent, merged_slots, norm)
    if selected is None:
        trace["selected_query_id"] = ""
        trace["final_intent"] = final_intent
        trace["final_slots"] = merged_slots
        return trace

    selected.intent = final_intent
    selected.slots = merged_slots
    selected.period = trace["resolved_period"]
    selected.llm_used = llm_used
    selected.fallback_used = fallback_used

    trace["selected_query_id"] = selected.item.id
    trace["final_intent"] = final_intent
    trace["final_slots"] = merged_slots
    trace["match_score"] = selected.score
    trace["match"] = selected
    return trace


def find_best_sql_registry_match(question: str) -> Optional[SQLRegistryMatch]:
    global _LAST_SQL_NLU_TRACE
    trace = analyze_sql_question(question)
    _LAST_SQL_NLU_TRACE = dict(trace)

    match = trace.get("match")
    if isinstance(match, SQLRegistryMatch):
        return match

    # Backward compatibility fallback
    q = _normalize(question)
    best: Optional[SQLRegistryMatch] = None
    for item in get_sql_registry_items():
        s = _score_item(q, item)
        if s <= 0:
            continue
        if best is None or s > best.score:
            best = SQLRegistryMatch(item=item, score=s)

    if best:
        _LAST_SQL_NLU_TRACE["selected_query_id"] = best.item.id
        _LAST_SQL_NLU_TRACE["fallback_used"] = True
    return best


def _extract_key_value(question: str, keys: List[str]) -> Optional[str]:
    q = question or ""
    for key in keys:
        pat = rf"(?:^|\s){re.escape(key)}\s*[:=]\s*([A-Za-z0-9_\-]+)"
        m = re.search(pat, q, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def _extract_after_alias(question: str, alias: str) -> Optional[str]:
    q = question or ""
    m = re.search(rf"{re.escape(alias)}\s*[:=]?\s*([A-Za-z0-9_\-]+)", q, flags=re.IGNORECASE)
    if m:
        v = m.group(1).strip()
        if v:
            return v
    return None


def _fallback_string_tokens(question: str) -> List[str]:
    q = _normalize(question)
    q = re.sub(r"\b(20\d{2})(0[1-9]|1[0-2])\b", " ", q)
    q = re.sub(r"\b\d{1,2}\s*월\b", " ", q)
    tokens = re.findall(r"[a-zA-Z0-9_\-]+", q)
    return [t for t in tokens if t not in _STOPWORDS and not t.isdigit()]


def build_sql_params(match: SQLRegistryMatch, question: str) -> Dict[str, Any]:
    params, _ = build_sql_params_with_missing(match, question)
    return params


def _fill_semantic_params(
    result: Dict[str, Any],
    item: SQLRegistryItem,
    slots: Dict[str, Any],
    period: PeriodResolution,
) -> None:
    versions = [str(x).strip().upper() for x in (slots.get("versions") or []) if str(x).strip()]
    periods = [str(x).strip() for x in (slots.get("periods") or []) if str(x).strip()]
    period_groups = [x for x in (slots.get("period_groups") or []) if isinstance(x, dict)]
    filters = dict(slots.get("filters") or {})
    for pname in item.params.keys():
        key = pname.lower()
        if key in ("version", "ver") and slots.get("version"):
            result[pname] = str(slots.get("version") or "").upper()
        elif re.fullmatch(r"v\d+", key) and versions:
            idx = int(key[1:]) - 1
            if 0 <= idx < len(versions):
                result[pname] = versions[idx]
        elif re.fullmatch(r"p\d+", key) and periods:
            idx = int(key[1:]) - 1
            if 0 <= idx < len(periods):
                result[pname] = periods[idx]
        elif re.fullmatch(r"g\d+_start", key) and period_groups:
            idx = int(key[1 : key.index("_")]) - 1
            if 0 <= idx < len(period_groups):
                result[pname] = str(period_groups[idx].get("start_yyyymm") or "")
        elif re.fullmatch(r"g\d+_end", key) and period_groups:
            idx = int(key[1 : key.index("_")]) - 1
            if 0 <= idx < len(period_groups):
                result[pname] = str(period_groups[idx].get("end_yyyymm") or "")
        elif key in ("yearmonth", "yyyymm", "anchor_yyyymm"):
            result[pname] = period.anchor_yyyymm
        elif key in ("quarter", "anchor_quarter"):
            result[pname] = _quarter_label_from_yyyymm(period.anchor_yyyymm)
        elif key in ("start_yyyymm", "from_yyyymm", "start_ym"):
            result[pname] = period.start_yyyymm
        elif key in ("end_yyyymm", "to_yyyymm", "end_ym"):
            result[pname] = period.end_yyyymm
        elif key in ("start_quarter", "from_quarter"):
            result[pname] = _quarter_label_from_yyyymm(period.start_yyyymm)
        elif key in ("end_quarter", "to_quarter"):
            result[pname] = _quarter_label_from_yyyymm(period.end_yyyymm)
        elif key in ("compare_start_yyyymm", "prev_start_yyyymm"):
            result[pname] = period.compare_start_yyyymm
        elif key in ("compare_end_yyyymm", "prev_end_yyyymm"):
            result[pname] = period.compare_end_yyyymm
        elif key in ("compare_start_quarter", "prev_start_quarter"):
            result[pname] = _quarter_label_from_yyyymm(period.compare_start_yyyymm)
        elif key in ("compare_end_quarter", "prev_end_quarter"):
            result[pname] = _quarter_label_from_yyyymm(period.compare_end_yyyymm)
        elif key in ("period_label",):
            result[pname] = period.label
        elif key in ("aggregation", "agg"):
            agg = _normalize_aggregation_name(str(slots.get("aggregation") or "")) or _normalize_aggregation_name(item.default_aggregation) or "sum"
            result[pname] = agg
        elif key in ("dimension", "group_dimension") and slots.get("dimension"):
            dim = str(slots.get("dimension") or "").lower()
            if dim in _SQL_DIMENSIONS:
                result[pname] = dim
        else:
            m = re.fullmatch(r"([a-z_][a-z0-9_]*)_(\d+)", key)
            if m:
                dim_name = m.group(1)
                idx = int(m.group(2)) - 1
                vals = filters.get(dim_name) or []
                if 0 <= idx < len(vals):
                    result[pname] = str(vals[idx])


def _normalize_param_type(spec: SQLParamSpec, value: Any) -> Any:
    v = str(value).strip()
    if spec.type == "yyyymm":
        m = re.search(r"(20\d{2})(0[1-9]|1[0-2])", v)
        return f"{m.group(1)}{m.group(2)}" if m else v
    if spec.type == "quarter":
        m = re.search(r"(20\d{2})\s*[Qq]\s*([1-4])", v)
        return f"{m.group(1)}Q{m.group(2)}" if m else v.upper()
    return v


def build_sql_params_with_missing(match: SQLRegistryMatch, question: str) -> Tuple[Dict[str, Any], List[str]]:
    item = match.item
    result: Dict[str, Any] = {}

    slots = dict(match.slots or extract_slots_rule_based(question))
    period_info = dict(match.period or {})
    period = resolve_period_slots(slots)
    if period_info:
        # prefer pre-resolved period from analyze phase
        period = PeriodResolution(
            period_type=str(period_info.get("type") or period.period_type),
            period_value=str(period_info.get("value") or period.period_value),
            start_yyyymm=str(period_info.get("start_yyyymm") or period.start_yyyymm),
            end_yyyymm=str(period_info.get("end_yyyymm") or period.end_yyyymm),
            anchor_yyyymm=str(period_info.get("anchor_yyyymm") or period.anchor_yyyymm),
            label=str(period_info.get("label") or period.label),
            ambiguous_adjusted=bool(period_info.get("ambiguous_adjusted", False)),
            compare_start_yyyymm=str(period_info.get("compare_start_yyyymm") or period.compare_start_yyyymm),
            compare_end_yyyymm=str(period_info.get("compare_end_yyyymm") or period.compare_end_yyyymm),
        )

    fallbacks = _fallback_string_tokens(question)
    fallback_idx = 0

    # 1) semantic slot fill first
    _fill_semantic_params(result, item, slots, period)

    # 2) legacy extraction path for compatibility
    for pname, spec in item.params.items():
        if pname in result and str(result[pname]).strip():
            continue

        candidates = [pname] + spec.aliases
        kv = _extract_key_value(question, candidates)
        if kv:
            result[pname] = kv
            continue

        captured = None
        for alias in spec.aliases:
            captured = _extract_after_alias(question, alias)
            if captured:
                break
        if captured:
            result[pname] = captured
            continue

        if spec.type == "string" and fallback_idx < len(fallbacks):
            result[pname] = fallbacks[fallback_idx]
            fallback_idx += 1

    # 3) type normalize / optional normalize
    for pname, spec in item.params.items():
        if pname not in result:
            continue
        val = result.get(pname)
        if val in (None, ""):
            continue
        result[pname] = _normalize_param_type(spec, val)

    # 4) missing checks (required only)
    missing = []
    for k, spec in item.params.items():
        if not spec.required:
            continue
        if str(result.get(k) or "").strip():
            continue
        missing.append(k)

    return result, missing


def build_sql_intent_prompt(question: str, context: Dict[str, Any]) -> str:
    payload = {
        "question": question,
        "normalized_question": context.get("normalized_question"),
        "rule_intent": context.get("rule_intent"),
        "slots": context.get("slots"),
        "allowed_intents": context.get("allowed_intents") or sorted(_INTENT_VALUES),
    }
    return (
        "You are a deterministic classifier. Return strict JSON only. "
        "No markdown, no prose.\n"
        "Schema: {\"intent\":string,\"confidence\":number,\"slots\":object}.\n"
        f"Input: {json.dumps(payload, ensure_ascii=False)}"
    )
