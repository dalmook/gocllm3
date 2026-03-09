import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class PeriodResolution:
    period_type: str
    period_value: str
    start_yyyymm: str
    end_yyyymm: str
    anchor_yyyymm: str
    label: str
    ambiguous_adjusted: bool = False
    compare_start_yyyymm: str = ""
    compare_end_yyyymm: str = ""


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    total = (year * 12 + (month - 1)) + delta
    ny = total // 12
    nm = (total % 12) + 1
    return ny, nm


def _quarter_range(year: int, quarter: int) -> tuple[str, str]:
    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 2
    return f"{year:04d}{start_month:02d}", f"{year:04d}{end_month:02d}"


def _infer_month_with_optional_year(value: str, now: datetime) -> tuple[str, bool]:
    mm = int(value)
    yy = now.year
    adjusted = False
    if mm > now.month:
        yy -= 1
        adjusted = True
    return f"{yy:04d}{mm:02d}", adjusted


def resolve_period_slots(
    slots: Dict[str, Any],
    *,
    now: Optional[datetime] = None,
) -> PeriodResolution:
    now_dt = now or datetime.now()
    ptype = str(slots.get("period_type") or "month").strip().lower()
    pvalue = str(slots.get("period_value") or "").strip().lower()

    current_yyyymm = f"{now_dt.year:04d}{now_dt.month:02d}"
    if not pvalue:
        pvalue = "this_month"
        ptype = "relative"

    start = current_yyyymm
    end = current_yyyymm
    anchor = current_yyyymm
    label = "이번달"
    adjusted = False
    cmp_start = ""
    cmp_end = ""

    # explicit absolute forms
    m_ym = re.fullmatch(r"(20\d{2})(0[1-9]|1[0-2])", pvalue)
    m_y = re.fullmatch(r"(20\d{2})", pvalue)
    m_q = re.fullmatch(r"(20\d{2})q([1-4])", pvalue)
    m_recent = re.fullmatch(r"recent_(\d+)_months", pvalue)
    m_mm = re.fullmatch(r"([1-9]|1[0-2])", pvalue)

    if m_ym:
        start = end = anchor = f"{m_ym.group(1)}{m_ym.group(2)}"
        label = f"{m_ym.group(1)}년 {int(m_ym.group(2))}월"
        ptype = "month"
    elif m_y:
        start = f"{m_y.group(1)}01"
        end = f"{m_y.group(1)}12"
        anchor = start
        label = f"{m_y.group(1)}년"
        ptype = "year"
    elif m_q:
        yy = int(m_q.group(1))
        qq = int(m_q.group(2))
        start, end = _quarter_range(yy, qq)
        anchor = start
        label = f"{yy}년 {qq}분기"
        ptype = "quarter"
    elif m_recent:
        n = max(1, int(m_recent.group(1)))
        ey, em = now_dt.year, now_dt.month
        sy, sm = _shift_month(ey, em, -(n - 1))
        start = f"{sy:04d}{sm:02d}"
        end = f"{ey:04d}{em:02d}"
        anchor = end
        label = f"최근 {n}개월"
        ptype = "relative"
    elif pvalue in ("this_year", "올해"):
        start = f"{now_dt.year:04d}01"
        end = f"{now_dt.year:04d}12"
        anchor = start
        label = "올해"
        ptype = "year"
    elif pvalue in ("last_year", "작년", "전년"):
        yy = now_dt.year - 1
        start = f"{yy:04d}01"
        end = f"{yy:04d}12"
        anchor = start
        label = "작년"
        ptype = "year"
    elif pvalue in ("this_month", "이번달", "금월"):
        start = end = anchor = current_yyyymm
        label = "이번달"
        ptype = "month"
    elif pvalue in ("last_month", "지난달", "전월", "저번달"):
        yy, mm = _shift_month(now_dt.year, now_dt.month, -1)
        start = end = anchor = f"{yy:04d}{mm:02d}"
        label = "지난달"
        ptype = "month"
    elif pvalue in ("this_quarter", "이번분기", "금분기"):
        qq = ((now_dt.month - 1) // 3) + 1
        start, end = _quarter_range(now_dt.year, qq)
        anchor = start
        label = "이번 분기"
        ptype = "quarter"
    elif pvalue in ("prev_quarter", "전분기", "지난분기"):
        qq = ((now_dt.month - 1) // 3) + 1
        if qq == 1:
            yy = now_dt.year - 1
            pq = 4
        else:
            yy = now_dt.year
            pq = qq - 1
        start, end = _quarter_range(yy, pq)
        anchor = start
        label = "전분기"
        ptype = "quarter"
    elif m_mm:
        start, adjusted = _infer_month_with_optional_year(m_mm.group(1), now_dt)
        end = anchor = start
        label = f"{int(m_mm.group(1))}월"
        ptype = "month"

    if str(slots.get("compare") or "") in ("prev_month", "prev_quarter", "prev_year"):
        if ptype == "month":
            sy, sm = int(start[:4]), int(start[4:6])
            py, pm = _shift_month(sy, sm, -1)
            cmp_start = cmp_end = f"{py:04d}{pm:02d}"
        elif ptype == "quarter":
            sy, sq = int(start[:4]), ((int(start[4:6]) - 1) // 3) + 1
            if sq == 1:
                py, pq = sy - 1, 4
            else:
                py, pq = sy, sq - 1
            cmp_start, cmp_end = _quarter_range(py, pq)
        elif ptype == "year":
            py = int(start[:4]) - 1
            cmp_start = f"{py:04d}01"
            cmp_end = f"{py:04d}12"

    return PeriodResolution(
        period_type=ptype,
        period_value=pvalue,
        start_yyyymm=start,
        end_yyyymm=end,
        anchor_yyyymm=anchor,
        label=label,
        ambiguous_adjusted=adjusted,
        compare_start_yyyymm=cmp_start,
        compare_end_yyyymm=cmp_end,
    )
