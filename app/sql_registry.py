import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import yaml


SQL_REGISTRY_YAML_PATH = os.getenv("SQL_REGISTRY_YAML_PATH", os.path.join(os.path.dirname(__file__), "sql_registry.yaml"))


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


@dataclass
class SQLRegistryMatch:
    item: SQLRegistryItem
    score: float


_SQL_REGISTRY_CACHE: List[SQLRegistryItem] = []
_SQL_REGISTRY_MTIME: float = -1.0


_STOPWORDS = {
    "판매", "합계", "조회", "알려줘", "알려", "몇개", "몇", "수량", "월", "년월",
    "sql", "데이터", "값", "이번", "지난", "이번달", "저번달", "요청",
}



def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())



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



def _load_registry_from_yaml(path: str) -> List[SQLRegistryItem]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

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

    return score



def find_best_sql_registry_match(question: str) -> Optional[SQLRegistryMatch]:
    q = _normalize(question)
    if not q:
        return None

    best: Optional[SQLRegistryMatch] = None
    for item in get_sql_registry_items():
        s = _score_item(q, item)
        if s <= 0:
            continue
        if best is None or s > best.score:
            best = SQLRegistryMatch(item=item, score=s)
    return best



def _extract_yyyymm(question: str) -> Optional[str]:
    q = question or ""
    m = re.search(r"\b(20\d{2})(0[1-9]|1[0-2])\b", q)
    if m:
        return f"{m.group(1)}{m.group(2)}"

    m2 = re.search(r"(?:(20\d{2})\s*년\s*)?(\d{1,2})\s*월", q)
    if m2:
        mm = int(m2.group(2))
        if not (1 <= mm <= 12):
            return None

        if m2.group(1):
            yy = int(m2.group(1))
            return f"{yy:04d}{mm:02d}"

        now = datetime.utcnow()
        yy = now.year
        # 연도 미입력 시: 현재 월보다 큰 월은 전년도 데이터로 간주(사용자 자연어 완화)
        if mm > now.month:
            yy -= 1

        return f"{yy:04d}{mm:02d}"
    return None



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



def build_sql_params_with_missing(match: SQLRegistryMatch, question: str) -> Tuple[Dict[str, Any], List[str]]:
    item = match.item
    result: Dict[str, Any] = {}

    fallbacks = _fallback_string_tokens(question)
    fallback_idx = 0

    for pname, spec in item.params.items():
        candidates = [pname] + spec.aliases

        kv = _extract_key_value(question, candidates)
        if kv:
            result[pname] = kv
            continue

        if spec.type == "yyyymm":
            ym = _extract_yyyymm(question)
            if ym:
                result[pname] = ym
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

    missing = [k for k, spec in item.params.items() if spec.required and not str(result.get(k) or "").strip()]

    # type normalize
    for pname, spec in item.params.items():
        if pname not in result:
            continue
        v = str(result[pname]).strip()
        if spec.type == "yyyymm":
            m = re.search(r"(20\d{2})(0[1-9]|1[0-2])", v)
            result[pname] = f"{m.group(1)}{m.group(2)}" if m else v
        else:
            result[pname] = v

    return result, missing
