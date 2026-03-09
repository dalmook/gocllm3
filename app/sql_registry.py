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
    workdate_column: str
    version_column: str
    default_filters: List[str] = field(default_factory=list)
    latest_snapshot_rule: str = ""


@dataclass
class SQLMetricSpec:
    id: str
    aliases: List[str]
    source: str
    value_column: str
    unit: str = "MEQ"


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
_SQL_QUERY_FAMILIES: Dict[str, SQLQueryFamilySpec] = {}
_SQL_METRIC_ALIAS_MAP: Dict[str, str] = {}


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
    "count": ["건수", "몇건", "몇 건"],
}

_TREND_WORDS = ["추이", "트렌드", "흐름"]
_COMPARE_WORDS = {
    "prev_month": ["전월대비", "전월 대비", "지난달대비", "지난달 대비"],
    "prev_quarter": ["전분기대비", "전분기 대비", "전분기", "지난분기대비"],
    "prev_year": ["전년대비", "전년 대비", "작년대비", "작년 대비"],
}

_DIMENSION_WORDS = {
    "version": ["버전별", "버전 별", "version별", "version 별"],
    "month": ["월별", "월 별", "월단위", "월 단위"],
    "quarter": ["분기별", "분기 별"],
}

_PERIOD_RELATIVE_WORDS = {
    "this_year": ["올해", "금년"],
    "last_year": ["작년", "전년"],
    "this_month": ["이번달", "당월", "금월"],
    "last_month": ["지난달", "전월", "저번달"],
    "this_quarter": ["이번분기", "이번 분기", "금분기"],
    "prev_quarter": ["전분기", "지난분기", "전 분기"],
}

_INTENT_VALUES = {"sales_total", "sales_trend", "sales_grouped", "sales_compare", "metric_compare_versions"}


def configure_sql_intent_llm_classifier(
    classifier: Optional[Callable[[str, Dict[str, Any]], Optional[Dict[str, Any]]]]
) -> None:
    global _SQL_INTENT_LLM_CLASSIFIER
    _SQL_INTENT_LLM_CLASSIFIER = classifier


def get_last_sql_nlu_trace() -> Dict[str, Any]:
    return dict(_LAST_SQL_NLU_TRACE)


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def normalize_question(text: str) -> str:
    q = (text or "").strip().lower()
    q = re.sub(r"([a-z0-9])([가-힣])", r"\1 \2", q)
    q = re.sub(r"([가-힣])([a-z0-9])", r"\1 \2", q)
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


def _parse_semantic_sections(data: Dict[str, Any]) -> None:
    global _SQL_SOURCES, _SQL_METRICS, _SQL_QUERY_FAMILIES, _SQL_METRIC_ALIAS_MAP

    sources_raw = data.get("sources") if isinstance(data, dict) else {}
    metrics_raw = data.get("metrics") if isinstance(data, dict) else {}
    families_raw = data.get("query_families") if isinstance(data, dict) else {}

    sources: Dict[str, SQLSourceSpec] = {}
    metrics: Dict[str, SQLMetricSpec] = {}
    families: Dict[str, SQLQueryFamilySpec] = {}
    alias_map: Dict[str, str] = {}

    if isinstance(sources_raw, dict):
        for sid, spec in sources_raw.items():
            if not isinstance(spec, dict):
                continue
            source_id = str(sid or "").strip()
            table = str(spec.get("table") or "").strip()
            period_column = str(spec.get("period_column") or "YEARMONTH").strip()
            workdate_column = str(spec.get("workdate_column") or "WORKDATE").strip()
            version_column = str(spec.get("version_column") or "VERSION").strip()
            if not source_id or not table:
                continue
            sources[source_id] = SQLSourceSpec(
                id=source_id,
                table=table,
                period_column=period_column,
                workdate_column=workdate_column,
                version_column=version_column,
                default_filters=[str(x).strip() for x in (spec.get("default_filters") or []) if str(x).strip()],
                latest_snapshot_rule=str(spec.get("latest_snapshot_rule") or "").strip(),
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
            metric = SQLMetricSpec(
                id=metric_id,
                aliases=aliases,
                source=source,
                value_column=value_column,
                unit=str(spec.get("unit") or "MEQ").strip() or "MEQ",
            )
            metrics[metric_id] = metric
            alias_map[metric_id.lower()] = metric_id
            for alias in aliases:
                alias_map[alias] = metric_id

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

    _SQL_SOURCES = sources
    _SQL_METRICS = metrics
    _SQL_QUERY_FAMILIES = families
    _SQL_METRIC_ALIAS_MAP = alias_map


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
    if "몇" in norm or "얼마" in norm:
        return "sum"
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


def _extract_dimension(norm: str) -> str:
    for dim, words in _DIMENSION_WORDS.items():
        if _contains_any(norm, words):
            return dim
    return ""


def _extract_compare(norm: str) -> str:
    compact = norm.replace(" ", "")
    for key, words in _COMPARE_WORDS.items():
        if any(w.replace(" ", "") in compact for w in words):
            return key
    if any(x in compact for x in ("비교", "차이", "대비", "vs", "versus")):
        return "compare_versions"
    if "와" in compact or "과" in compact:
        return "compare_versions"
    return ""


def _extract_versions(question: str, norm: str) -> List[str]:
    tokens = re.findall(r"[A-Za-z][A-Za-z0-9_\-]{0,12}", question or "")
    stop = {
        "SQL", "VERSION", "SALES", "SUM", "AVG", "MAX", "MIN", "COUNT",
        "MONTH", "YEAR", "QUARTER", "THIS", "LAST", "RECENT", "VS", "AND",
        "NET", "PRODUCTION", "IPGO", "COMPARE", "ANALYSIS",
    }

    versions: List[str] = []
    for tok in tokens:
        up = tok.upper()
        if up in stop:
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
    if not versions:
        versions = _extract_versions(question, normalize_question(question))
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


def _extract_version(question: str, norm: str) -> str:
    # explicit key-value first
    m_kv = re.search(r"(?:version|버전)\s*[:=]\s*([A-Za-z0-9_\-]{1,16})", question, flags=re.IGNORECASE)
    if m_kv:
        return m_kv.group(1).strip().upper()

    m_after = re.search(r"(?:version|버전)\s*([A-Za-z0-9_\-]{1,16})", question, flags=re.IGNORECASE)
    if m_after:
        return m_after.group(1).strip().upper()

    tokens = re.findall(r"[A-Za-z][A-Za-z0-9_\-]{1,12}", question)
    stop = {
        "SQL", "VERSION", "SALES", "SUM", "AVG", "MAX", "MIN", "COUNT",
        "MONTH", "YEAR", "QUARTER", "THIS", "LAST", "RECENT",
    }
    for tok in tokens:
        up = tok.upper()
        if up in stop:
            continue
        if re.fullmatch(r"20\d{2}", up):
            continue
        return up

    if " vh " in f" {norm} ":
        return "VH"
    return ""


def extract_slots_rule_based(question: str, *, now: Optional[datetime] = None) -> Dict[str, Any]:
    norm = normalize_question(question)

    metric = _extract_metric(norm)
    aggregation = _extract_aggregation(norm) or ("sum" if metric else "")
    period_type, period_value = _extract_period_slots(norm, now=now)
    dimension = _extract_dimension(norm)
    compare = _extract_compare(norm)
    trend = any(w in norm for w in _TREND_WORDS)
    version = _extract_version(question, norm)
    versions = _extract_versions(question, norm)
    analysis = any(x in norm for x in ("분석", "해석", "비교 분석", "비교분석"))

    if trend and not dimension:
        dimension = "month"
    if "버전별" in norm.replace(" ", ""):
        dimension = "version"
    if version and version not in versions:
        versions.append(version)

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
        "aggregation": aggregation or "sum",
        "period_type": period_type,
        "period_value": period_value,
        "dimension": dimension,
        "version": version,
        "versions": versions,
        "compare": compare,
        "compare_flag": bool(compare),
        "analysis": analysis,
        "trend": trend,
    }
    return slots


def classify_intent_rule_based(question: str, slots: Dict[str, Any]) -> Tuple[str, bool, List[str]]:
    norm = normalize_question(question)
    versions = resolve_versions(question, slots)
    metric = resolve_metric(question, slots) or "sales"
    compare_token = str(slots.get("compare") or "")
    compare_requested = bool(compare_token) or len(versions) >= 2

    scores = {
        "sales_total": 0.4,
        "sales_trend": 0.0,
        "sales_grouped": 0.0,
        "sales_compare": 0.0,
        "metric_compare_versions": 0.0,
    }

    if slots.get("compare"):
        scores["sales_compare"] += 2.2
    if compare_requested and len(versions) >= 2 and metric in ("sales", "net_prod", "net_ipgo"):
        scores["metric_compare_versions"] += 3.0
    if slots.get("trend"):
        scores["sales_trend"] += 2.2
    if slots.get("dimension") in ("version", "month", "quarter"):
        scores["sales_grouped"] += 1.6
    compact = norm.replace(" ", "")
    if any(x in compact for x in ("버전별", "version별", "분기별")):
        scores["sales_grouped"] += 1.0
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


def infer_default_period(intent: str, slots: Dict[str, Any], question: str) -> Tuple[Dict[str, Any], bool, str]:
    merged = dict(slots or {})
    if merged.get("period_value"):
        return merged, False, ""

    norm = normalize_question(question)
    reason = ""
    inferred = False

    if intent in ("sales_total", "sales_grouped"):
        merged["period_type"] = "year"
        merged["period_value"] = "this_year"
        reason = "기간 지정이 없어 올해 누적 기준으로 조회했습니다."
        inferred = True
    elif intent == "sales_trend":
        merged["period_type"] = "relative"
        merged["period_value"] = "recent_3_months"
        if not merged.get("dimension"):
            merged["dimension"] = "month"
        reason = "기간 지정이 없어 최근 3개월 추이로 해석했습니다."
        inferred = True
    elif intent == "sales_compare":
        merged["period_type"] = "quarter"
        merged["period_value"] = "this_quarter"
        if not merged.get("compare"):
            merged["compare"] = "prev_quarter"
        reason = "기준 기간이 없어 이번 분기 대비 전분기로 해석했습니다."
        inferred = True
    elif intent == "metric_compare_versions":
        merged["period_type"] = "month"
        merged["period_value"] = "this_month"
        reason = "기간 지정이 없어 이번달 기준으로 버전 비교를 조회했습니다."
        inferred = True

    if not inferred and any(x in norm for x in ("어때", "흐름", "추이")) and not merged.get("period_value"):
        merged["period_type"] = "relative"
        merged["period_value"] = "recent_3_months"
        if not merged.get("dimension"):
            merged["dimension"] = "month"
        reason = "기간 지정이 없어 최근 3개월 추이로 해석했습니다."
        inferred = True

    return merged, inferred, reason


def _sanitize_slots(raw_slots: Any) -> Dict[str, Any]:
    if not isinstance(raw_slots, dict):
        return {}
    out: Dict[str, Any] = {}
    allowed_keys = {
        "metric", "aggregation", "period_type", "period_value",
        "dimension", "version", "versions", "compare", "compare_flag", "analysis", "trend",
        "metric_unit", "source_name",
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
        return "compare_versions_same_period"
    if intent == "sales_trend":
        return "trend_monthly"
    if intent == "sales_grouped":
        return "grouped_by_version_same_period"
    if intent == "sales_total":
        return "total_single_version_same_period"
    return ""


def _build_default_filter_sql(default_filters: List[str]) -> str:
    if not default_filters:
        return ""
    return "".join([f" AND {flt}" for flt in default_filters if flt])


def build_compare_plan(metric: str, versions: List[str], period: Dict[str, Any]) -> Optional[SQLRegistryMatch]:
    metric_spec = _SQL_METRICS.get(metric)
    if metric_spec is None or len(versions) < 2:
        return None

    source_spec = _SQL_SOURCES.get(metric_spec.source)
    if source_spec is None:
        return None

    table = _safe_identifier(source_spec.table)
    period_column = _safe_identifier(source_spec.period_column)
    workdate_column = _safe_identifier(source_spec.workdate_column)
    version_column = _safe_identifier(source_spec.version_column)
    value_column = _safe_identifier(metric_spec.value_column)

    binds: List[str] = []
    params: Dict[str, SQLParamSpec] = {
        "start_yyyymm": SQLParamSpec(type="yyyymm", required=True, aliases=["시작월"]),
        "end_yyyymm": SQLParamSpec(type="yyyymm", required=True, aliases=["종료월"]),
    }
    for idx, _ in enumerate(versions, start=1):
        key = f"v{idx}"
        binds.append(f":{key}")
        params[key] = SQLParamSpec(type="string", required=True, aliases=[key])

    default_filter_sql = _build_default_filter_sql(source_spec.default_filters)
    if source_spec.latest_snapshot_rule:
        latest_rule = source_spec.latest_snapshot_rule.format(
            table=table,
            period_column=period_column,
            workdate_column=workdate_column,
            default_filter_sql=default_filter_sql,
        )
    else:
        latest_rule = (
            f"{workdate_column} = (SELECT MAX({workdate_column}) FROM {table} "
            f"WHERE {period_column} BETWEEN :start_yyyymm AND :end_yyyymm{default_filter_sql})"
        )

    sql = f"""
SELECT
  UPPER({version_column}) AS VERSION,
  NVL(SUM({value_column}), 0) AS VALUE
FROM {table}
WHERE 1=1
  AND {latest_rule}
  {default_filter_sql}
  AND {period_column} BETWEEN :start_yyyymm AND :end_yyyymm
  AND UPPER({version_column}) IN ({", ".join(binds)})
GROUP BY UPPER({version_column})
ORDER BY UPPER({version_column})
""".strip()

    item = SQLRegistryItem(
        id="compare_versions_same_period",
        description=f"{metric} versions compare",
        sql=sql,
        params=params,
        result=SQLResultSpec(mode="table", field="", empty_message="비교 대상 데이터가 없습니다."),
        keywords=[],
        patterns=[],
        intent="metric_compare_versions",
        supported_slots=["metric", "versions", "period_type", "period_value", "compare", "analysis"],
        default_aggregation="sum",
        supports_compare=True,
        supports_trend=False,
        groupable_dimensions=["version"],
        deprecated=False,
    )
    slots = {
        "metric": metric,
        "versions": versions,
        "compare": True,
        "analysis": bool(period.get("analysis") if isinstance(period, dict) else False),
        "metric_unit": metric_spec.unit,
        "source_name": source_spec.id,
    }
    return SQLRegistryMatch(
        item=item,
        score=20.0,
        intent="metric_compare_versions",
        slots=slots,
        period=dict(period or {}),
        llm_used=False,
        fallback_used=False,
    )


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
        if str(query_id or "") == "compare_versions_same_period":
            metric = str((slots or {}).get("metric") or "sales")
            versions = [str(x).strip().upper() for x in ((slots or {}).get("versions") or []) if str(x).strip()]
            dynamic = build_compare_plan(metric, versions, period)
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
    primary = selected_query_id or ""
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

    if primary:
        plan.append(SQLExecutionPlanStep(query_id=primary, role="primary", reason="intent-primary"))

    qnorm = normalize_question(question)
    if intent == "sales_total":
        # 질문이 단순 합계일 때 요약 품질 향상을 위해 월별 breakdown 보조 조회
        if slots.get("version") and not slots.get("trend") and "추이" not in qnorm:
            plan.append(SQLExecutionPlanStep(query_id="sales_trend_monthly", role="aux", reason="breakdown-monthly"))
    elif intent == "sales_trend":
        # 추이 질문은 총합 요약 보조값도 함께 조회
        ptype = str(slots.get("period_type") or "")
        aux = "sales_total_month" if ptype == "month" else "sales_total_period_range"
        plan.append(SQLExecutionPlanStep(query_id=aux, role="aux", reason="trend-total-summary"))

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
        merged_slots["source_name"] = metric_spec.source
    merged_slots["versions"] = resolve_versions(question, merged_slots)
    if len(merged_slots.get("versions") or []) >= 2:
        merged_slots["compare"] = True
    merged_slots, period_inferred, period_infer_reason = infer_default_period(final_intent, merged_slots, question)
    trace["period_inferred"] = period_inferred
    trace["period_infer_reason"] = period_infer_reason

    period = resolve_period_slots(merged_slots, now=now)
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
    }

    family_id = select_query_family(final_intent, merged_slots, str(merged_slots.get("metric") or "sales"))
    if family_id == "compare_versions_same_period":
        compare_match = build_compare_plan(
            str(merged_slots.get("metric") or "sales"),
            list(merged_slots.get("versions") or []),
            {
                **trace["resolved_period"],
                "analysis": bool(merged_slots.get("analysis")),
            },
        )
        if compare_match is not None:
            compare_match.slots = dict(merged_slots)
            compare_match.period = dict(trace["resolved_period"])
            compare_match.llm_used = llm_used
            compare_match.fallback_used = fallback_used
            trace["selected_query_id"] = compare_match.item.id
            trace["final_intent"] = final_intent
            trace["final_slots"] = merged_slots
            trace["match_score"] = compare_match.score
            trace["match"] = compare_match
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
    for pname in item.params.keys():
        key = pname.lower()
        if key in ("version", "ver") and slots.get("version"):
            result[pname] = str(slots.get("version") or "").upper()
        elif re.fullmatch(r"v\d+", key) and versions:
            idx = int(key[1:]) - 1
            if 0 <= idx < len(versions):
                result[pname] = versions[idx]
        elif key in ("yearmonth", "yyyymm", "anchor_yyyymm"):
            result[pname] = period.anchor_yyyymm
        elif key in ("start_yyyymm", "from_yyyymm", "start_ym"):
            result[pname] = period.start_yyyymm
        elif key in ("end_yyyymm", "to_yyyymm", "end_ym"):
            result[pname] = period.end_yyyymm
        elif key in ("compare_start_yyyymm", "prev_start_yyyymm"):
            result[pname] = period.compare_start_yyyymm
        elif key in ("compare_end_yyyymm", "prev_end_yyyymm"):
            result[pname] = period.compare_end_yyyymm
        elif key in ("period_label",):
            result[pname] = period.label
        elif key in ("aggregation", "agg") and slots.get("aggregation"):
            agg = str(slots.get("aggregation") or "sum").lower()
            if agg not in {"sum", "avg", "max", "min", "count"}:
                agg = "sum"
            result[pname] = agg
        elif key in ("dimension", "group_dimension") and slots.get("dimension"):
            dim = str(slots.get("dimension") or "").lower()
            if dim in {"version", "month", "quarter"}:
                result[pname] = dim


def _normalize_param_type(spec: SQLParamSpec, value: Any) -> Any:
    v = str(value).strip()
    if spec.type == "yyyymm":
        m = re.search(r"(20\d{2})(0[1-9]|1[0-2])", v)
        return f"{m.group(1)}{m.group(2)}" if m else v
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
