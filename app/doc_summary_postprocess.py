import re
from typing import Any, Dict, List


_SPARSE_PREFIX_RE = re.compile(r"^\s*-\s*(?:\([^)]*\)\s*)?(?:\[?[Ll][Vv]\s*\d+\]?|중요도\s*[=:]?\s*\d+)\b", re.IGNORECASE)
_GENERIC_TOKENS = (
    "관리 이슈",
    "지속중",
    "지속 중",
    "진행중",
    "진행 중",
    "확인중",
    "확인 중",
    "모니터링",
    "대응중",
    "대응 중",
    "리스크",
    "이슈",
)


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _is_sparse_issue_line(line: str) -> bool:
    normalized = _normalize_text(line)
    if not normalized.startswith("-"):
        return False
    if not _SPARSE_PREFIX_RE.search(normalized):
        return False

    cleaned = re.sub(r"^\s*-\s*", "", normalized)
    cleaned = re.sub(r"^\([^)]*\)\s*", "", cleaned)
    cleaned = re.sub(r"^\[?[Ll][Vv]\s*\d+\]?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^중요도\s*[=:]?\s*\d+\s*", "", cleaned, flags=re.IGNORECASE)
    for token in _GENERIC_TOKENS:
        cleaned = cleaned.replace(token, " ")
    cleaned = _normalize_text(cleaned)
    return len(cleaned) < 18


def _split_candidate_sentences(content: str) -> List[str]:
    text = str(content or "").replace("\r", "\n")
    chunks = re.split(r"[\n.!?]+", text)
    return [_normalize_text(chunk) for chunk in chunks if _normalize_text(chunk)]


def _summarize_doc(doc: Dict[str, Any], max_len: int = 90) -> str:
    content = str(doc.get("content") or doc.get("merge_title_content") or "").strip()
    title = _normalize_text(str(doc.get("title") or doc.get("doc_id") or "관련 문서"))
    candidates = _split_candidate_sentences(content)
    chosen = ""
    for candidate in candidates:
        lowered = candidate.lower()
        if len(candidate) < 15:
            continue
        if "http://" in lowered or "https://" in lowered:
            continue
        if _SPARSE_PREFIX_RE.search(candidate) and _is_sparse_issue_line(f"- {candidate}"):
            continue
        chosen = candidate
        break
    if not chosen:
        chosen = candidates[0] if candidates else title
    if len(chosen) > max_len:
        chosen = chosen[: max_len - 1].rstrip() + "…"
    return chosen


def enrich_sparse_issue_lines(answer: str, documents: List[Dict[str, Any]]) -> str:
    if not answer or not documents or "📂 핵심 이슈" not in answer:
        return answer

    lines = answer.splitlines()
    in_issue_section = False
    doc_idx = 0
    changed = False
    updated: List[str] = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("📂 핵심 이슈"):
            in_issue_section = True
            updated.append(line)
            continue
        if in_issue_section and stripped.startswith(("📂 ", "📌 ", "💡 ", "🔗 ", "⚠️ ")):
            in_issue_section = False
        if in_issue_section and _is_sparse_issue_line(line) and doc_idx < len(documents):
            doc = documents[doc_idx]
            doc_idx += 1
            doc_date = str(doc.get("_doc_date") or "날짜 정보 없음").strip()
            updated.append(f"- ({doc_date}) {_summarize_doc(doc)}")
            changed = True
            continue
        if in_issue_section and stripped.startswith("-") and doc_idx < len(documents):
            doc_idx += 1
        updated.append(line)

    return "\n".join(updated) if changed else answer
