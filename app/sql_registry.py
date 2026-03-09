import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
import ui


@dataclass
class SQLRegistryItem:
    id: str
    description: str
    keywords: List[str]
    patterns: List[str]
    intent_type: str
    runner: Optional[str] = None
    sql: Optional[str] = None
    params_builder: Optional[Callable[[str], Dict[str, Any]]] = None
    formatter: Optional[Callable[[pd.DataFrame], str]] = None


@dataclass
class SQLRegistryMatch:
    item: SQLRegistryItem
    score: float



def _extract_month(question: str) -> Optional[str]:
    m = re.search(r"(\d{1,2})\s*월", question)
    if not m:
        return None
    month = int(m.group(1))
    if month < 1 or month > 12:
        return None
    year = re.search(r"(20\d{2})\s*년", question)
    yy = int(year.group(1)) if year else 2026
    return f"{yy:04d}{month:02d}"



def _extract_after_keyword(question: str, words: List[str]) -> str:
    q = (question or "").strip()
    for w in words:
        idx = q.find(w)
        if idx >= 0:
            tail = q[idx + len(w):].strip(" ?")
            if tail:
                return tail
    return q



def _fmt_default(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "조회 결과가 없습니다."
    return f"총 {len(df)}건 조회되었습니다."



def _build_ship_params(question: str) -> Dict[str, Any]:
    ym = _extract_month(question) or ""
    search_key = _extract_after_keyword(question, ["버전", "거래선", "item", "아이템"]).upper().replace(" ", "")
    return {
        "smon": ym,
        "emon": ym,
        "conv": "haversion01" if "버전" in question else "deliverynum01",
        "q": search_key,
    }



def _build_pkg_params(question: str) -> Dict[str, Any]:
    token = _extract_after_keyword(question, ["코드", "pkg", "package"]).upper().replace(" ", "")
    return {"q": token}



def _build_ps_params(question: str) -> Dict[str, Any]:
    token = _extract_after_keyword(question, ["ps", "코드", "조회"]).upper().replace(" ", "")
    gubun = "pscomp01"
    if "module" in question.lower() or "모듈" in question:
        gubun = "psmodule01"
    if "mcp" in question.lower() or "멀티칩" in question:
        gubun = "psmultichip01"
    return {"gubun": gubun, "conv": "pseds03", "q": token}



def _build_rightperson_params(question: str) -> Dict[str, Any]:
    token = _extract_after_keyword(question, ["담당", "담당자", "right person", "rightperson"]).strip()
    return {"q": token}


SQL_REGISTRY: List[SQLRegistryItem] = [
    SQLRegistryItem(
        id="ship_by_version",
        description="출하/선적/버전/거래선 조회",
        keywords=["출하", "선적", "버전", "거래선", "do", "item"],
        patterns=[r"\d+월.*(출하|선적)", r"(버전|거래선).*(판매|수량|몇)"],
        intent_type="data_only",
        runner="ONEVIEW_SHIP",
        params_builder=_build_ship_params,
        formatter=_fmt_default,
    ),
    SQLRegistryItem(
        id="pkg_code_lookup",
        description="PKG 코드 조회",
        keywords=["pkg", "패키지", "코드", "pcbc"],
        patterns=[r"(pkg|패키지).*(코드|조회)", r"코드.*(찾아|조회)"],
        intent_type="data_only",
        runner="PKGCODE",
        params_builder=_build_pkg_params,
        formatter=_fmt_default,
    ),
    SQLRegistryItem(
        id="ps_code_lookup",
        description="PS 코드 조회",
        keywords=["ps", "fab", "eds", "asy", "tst", "module", "mcp"],
        patterns=[r"(ps|module|mcp).*(코드|조회)", r"(fab|eds|asy|tst).*(코드|조회)"],
        intent_type="data_only",
        runner="PS_QUERY",
        params_builder=_build_ps_params,
        formatter=_fmt_default,
    ),
    SQLRegistryItem(
        id="rightperson_lookup",
        description="담당자/Right Person 조회",
        keywords=["담당", "담당자", "right person", "rightperson", "pl", "tl"],
        patterns=[r"(담당자|right\s*person).*(누구|조회|찾아)", r"(pl|tl|팀장).*(누구|조회)"],
        intent_type="data_only",
        runner="RIGHTPERSON",
        params_builder=_build_rightperson_params,
        formatter=_fmt_default,
    ),
]



def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())



def find_best_sql_registry_match(question: str) -> Optional[SQLRegistryMatch]:
    q = _normalize(question)
    if not q:
        return None

    best: Optional[SQLRegistryMatch] = None
    for item in SQL_REGISTRY:
        score = 0.0
        for kw in item.keywords:
            if kw.lower() in q:
                score += 1.0
        for pat in item.patterns:
            if re.search(pat, q, flags=re.IGNORECASE):
                score += 1.5

        if score <= 0:
            continue
        if best is None or score > best.score:
            best = SQLRegistryMatch(item=item, score=score)

    return best



def build_sql_params(match: SQLRegistryMatch, question: str) -> Dict[str, Any]:
    if match.item.params_builder:
        try:
            return match.item.params_builder(question) or {}
        except Exception:
            return {}
    return {}
