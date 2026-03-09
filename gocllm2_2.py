### PART 2/5
            start, end = month_range
            return _mk(f"{target_year}년 {target_month}월", start, end)

    m = re.search(r"작년\s*(\d{1,2})\s*월", q_raw)
    if m:
        target_month = int(m.group(1))
        month_range = _get_month_range(now.year - 1, target_month)
        if month_range:
            start, end = month_range
            return _mk(f"작년 {target_month}월", start, end)

    m = re.search(r"(올해|금년|당해)\s*(\d{1,2})\s*월", q_raw)
    if m:
        target_month = int(m.group(2))
        month_range = _get_month_range(now.year, target_month)
        if month_range:
            start, end = month_range
            return _mk(f"올해 {target_month}월", start, end)

    # 2) 이번주/저번주/이번달/저번달 (캘린더 기간)
    if any(token in q_compact for token in ("이번주", "금주", "이번주간")):
        start, end = _get_week_range(now, week_offset=0)
        return _mk("이번주", start, end)

    if any(token in q_compact for token in ("저번주", "지난주", "전주", "지난주간")):
        start, end = _get_week_range(now, week_offset=-1)
        return _mk("저번주", start, end)

    if any(token in q_compact for token in ("이번달", "금월")):
        month_range = _get_month_range(now.year, now.month)
        if month_range:
            start, end = month_range
            return _mk("이번달", start, end)

    if any(token in q_compact for token in ("저번달", "지난달", "전월")):
        year = now.year
        month = now.month - 1
        if month == 0:
            year -= 1
            month = 12
        month_range = _get_month_range(year, month)
        if month_range:
            start, end = month_range
            return _mk("저번달", start, end)

    # 3) "최근 N일/최근 N주/최근 N개월" (rolling window)
    #    - 숫자 없이 "최근/요즘/근래/최근에"만 있으면 default 7일
    recent_tokens = ("최근", "요즘", "근래", "최근에", "최신", "최신순", "최신이슈", "최근이슈")
    if any(tok in q_compact for tok in recent_tokens):
        # 예: 최근3일 / 최근 2주 / 최근 한달 / 요즘(=default)
        # 숫자: 1~3자리, 단위: 일/주/주일/개월/달
        m = re.search(r"(최근|요즘|근래|최근에)\s*(\d{1,3})?\s*(일|주|주일|개월|달)?", q_raw)
        n = None
        unit = None
        if m:
            if m.group(2):
                try:
                    n = int(m.group(2))
                except:
                    n = None
            unit = (m.group(3) or "").strip()

        # 기본값
        if n is None:
            n = 7
        if not unit:
            unit = "일"

        unit = unit.replace("주일", "주")
        unit = unit.replace("달", "개월")

        if unit == "일":
            delta = timedelta(days=n)
            label = f"최근 {n}일"
        elif unit == "주":
            delta = timedelta(days=7 * n)
            label = f"최근 {n}주"
        else:  # "개월"
            # 월은 정확한 일수로 환산이 애매해서 실무적으로 30일*n로 rolling 처리(캘린더월은 '이번달/저번달'로 이미 커버)
            delta = timedelta(days=30 * n)
            label = f"최근 {n}개월"

        end = now.replace(hour=23, minute=59, second=59, microsecond=0)
        start = (end - delta).replace(hour=0, minute=0, second=0, microsecond=0)
        return _mk(label, start, end)

    return None


def _filter_docs_by_datetime_range(
    documents: List[Dict[str, Any]],
    start_dt: datetime,
    end_dt: datetime,
) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for doc in documents:
        dt = _extract_doc_datetime(doc)
        if not dt:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("Asia/Seoul"))
        dt = dt.astimezone(ZoneInfo("Asia/Seoul"))
        if start_dt <= dt < end_dt:
            filtered.append(doc)
    return filtered


def rerank_rag_documents(documents: List[Dict[str, Any]], prefer_recent: bool = False) -> List[Dict[str, Any]]:
    if not documents:
        return []
    merged: Dict[str, Dict[str, Any]] = {}
    for doc in documents:
        key = str(
            doc.get("doc_id")
            or doc.get("id")
            or doc.get("confluence_mail_page_url")
            or doc.get("url")
            or f"{doc.get('title','')}|{doc.get('_index','')}"
        )
        raw_score = float(doc.get("_score") or 0.0)
        if key not in merged:
            item = dict(doc)
            item["_query_hits"] = 1
            item["_vector_score"] = raw_score
            merged[key] = item
        else:
            merged[key]["_query_hits"] += 1
            if raw_score > float(merged[key].get("_vector_score") or 0.0):
                keep_hits = merged[key]["_query_hits"]
                item = dict(doc)
                item["_query_hits"] = keep_hits
                item["_vector_score"] = raw_score
                merged[key] = item
    docs = list(merged.values())
    max_vec = max([float(d.get("_vector_score") or 0.0) for d in docs] or [1.0])
    now = datetime.now(ZoneInfo("Asia/Seoul"))
    for d in docs:
        vec = float(d.get("_vector_score") or 0.0)
        vec_norm = vec / max_vec if max_vec > 0 else 0.0
        dt = _extract_doc_datetime(d)
        if dt:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=ZoneInfo("Asia/Seoul"))
            dt_local = dt.astimezone(ZoneInfo("Asia/Seoul"))
            age_days = max((now - dt_local).total_seconds() / 86400.0, 0.0)
            recency_score = max(
                RAG_MIN_RECENCY_SCORE,
                math.exp(-math.log(2) * age_days / max(RAG_RECENCY_HALF_LIFE_DAYS, 1.0))
            )
            d["_doc_date"] = dt_local.strftime("%Y-%m-%d %H:%M")
            d["_doc_ts"] = dt_local.timestamp()
        else:
            recency_score = RAG_MIN_RECENCY_SCORE
            d["_doc_date"] = "날짜 정보 없음"
            d["_doc_ts"] = 0.0
        query_hit_bonus = min(max(int(d.get("_query_hits") or 1) - 1, 0), 3) * 0.03
        combined_score = ((1 - RAG_RECENCY_WEIGHT) * vec_norm) + (RAG_RECENCY_WEIGHT * recency_score) + query_hit_bonus
        d["_vector_norm"] = round(vec_norm, 4)
        d["_recency_score"] = round(recency_score, 4)
        d["_combined_score"] = round(combined_score, 4)
    if prefer_recent:
        docs.sort(
            key=lambda x: (
                float(x.get("_doc_ts", 0.0)),
                float(x.get("_combined_score", 0.0)),
                float(x.get("_vector_score", 0.0)),
            ),
            reverse=True,
        )
    else:
        docs.sort(
            key=lambda x: (
                float(x.get("_combined_score", 0.0)),
                float(x.get("_vector_score", 0.0))
            ),
            reverse=True
        )
    return docs
RAG_MIN_COMBINED_SCORE = float(os.getenv("RAG_MIN_COMBINED_SCORE", str(RAG_SIMILARITY_THRESHOLD)))
RAG_MIN_KEYWORD_HITS = int(os.getenv("RAG_MIN_KEYWORD_HITS", "1"))


BUSINESS_SPLIT_KEYWORDS = [
    "주간", "이슈", "정리", "요약", "현황", "리스크", "대응", "이번주", "저번주", "지난주"
]
MAIL_STRONG_INTENT_KEYWORDS = [
    "이슈", "정리", "요약", "현황", "주간", "이번주", "저번주", "지난주", "최신", "최근", "업데이트"
]
GLOSSARY_INTENT_KEYWORDS = ["뭐야", "뜻", "의미", "정의", "약자", "약어", "용어", "무슨 뜻"]
RECENT_PRIORITY_KEYWORDS = [
    "최신", "최근", "요즘", "근래", "업데이트", "이번주", "금주", "최근이슈", "최신이슈", "주요이슈", "이슈정리"
]
ISSUE_SUMMARY_KEYWORDS = ["이슈", "정리", "요약", "현황", "동향", "업데이트", "주요"]


def normalize_query_for_search(question: str) -> str:
    """RAG 검색 직전에만 사용하는 질의 정규화"""
    q = (question or "").strip()
    if not q:
        return ""

    # 영문/숫자와 한글 경계 공백 삽입
    q = re.sub(r"([A-Za-z0-9])([가-힣])", r"\1 \2", q)
    q = re.sub(r"([가-힣])([A-Za-z0-9])", r"\1 \2", q)

    # 붙어 있는 업무 키워드 분리
    for kw in sorted(BUSINESS_SPLIT_KEYWORDS, key=len, reverse=True):
        q = re.sub(rf"\s*{re.escape(kw)}\s*", f" {kw} ", q)

    # 연속 공백 정리
    return re.sub(r"\s+", " ", q).strip()


def has_strong_mail_intent(question: str) -> bool:
    q_compact = re.sub(r"\s+", "", question or "")
    return any(kw in q_compact for kw in MAIL_STRONG_INTENT_KEYWORDS)


def is_issue_summary_intent(question: str) -> bool:
    q_compact = re.sub(r"\s+", "", question or "")
    hits = sum(1 for kw in ISSUE_SUMMARY_KEYWORDS if kw in q_compact)
    return hits >= 2 or ("이슈" in q_compact and any(k in q_compact for k in ("정리", "요약", "현황")))

GENERAL_QUESTION_HINTS = [
    "날씨", "기온", "비와", "눈와", "환율", "주가", "뉴스", "시간", "몇시",
    "today", "weather", "temperature", "stock", "news", "time"
]

def _normalize_text_for_match(s: str) -> str:
    s = (s or "").lower().strip()
    for ch in [" ", "\n", "\t", ",", ".", ":", ";", "/", "\\", "(", ")", "[", "]", "{", "}", "-", "_", "?", "!"]:
        s = s.replace(ch, " ")
    return " ".join(s.split())

def _extract_query_keywords(question: str) -> List[str]:
    q = _normalize_text_for_match(normalize_query_for_search(question))
    toks = [t for t in q.split() if len(t) >= 2]
    stopwords = {
        "오늘", "어때", "뭐야", "알려줘", "조회", "관련", "대한", "해줘", "설명",
        "the", "is", "are",
        "what", "when", "how", "why", "please"
    }
    return [t for t in toks if t not in stopwords]

def should_prefer_general_llm(question: str) -> bool:
    q = (question or "").lower()
    return any(h in q for h in GENERAL_QUESTION_HINTS)

def is_rag_result_relevant(question: str, top_docs: List[Dict[str, Any]]) -> bool:
    if not top_docs:
        return False

    top1 = top_docs[0]
    top_score = float(top1.get("_combined_score") or 0.0)

    title = str(top1.get("title") or "")
    content = str(top1.get("content") or top1.get("merge_title_content") or "")
    haystack = _normalize_text_for_match(title + " " + content)

    keywords = _extract_query_keywords(question)
    keyword_hits = sum(1 for kw in keywords if kw in haystack)

    # FW:/RE: 같은 전달메일성 제목은 약간 보수적으로
    noisy_title = title.strip().upper().startswith(("FW:", "RE:"))

    effective_threshold = max(RAG_SIMILARITY_THRESHOLD, RAG_MIN_COMBINED_SCORE)
    if top_score < effective_threshold:
        return False
    if keyword_hits < RAG_MIN_KEYWORD_HITS and noisy_title:
        return False
    if keywords and keyword_hits == 0:
        return False

    return True

# =========================
# Glossary RAG Helper Functions
# =========================
def is_glossary_doc(doc: Dict[str, Any]) -> bool:
    """문서가 glossary 인덱스에서 온 것인지 확인"""
    return doc.get("_index", "") == GLOSSARY_INDEX_NAME

def is_glossary_intent(question: str) -> bool:
    """
    용어형 질문인지 판별
    - 키워드: 뭐야, 뜻, 의미, 정의, 약자, 용어, 무슨 뜻
    - 영문 대문자 약어 패턴 (예: RTF, HBM, TSV)
    - 단, 메일성 의도가 강하면 False
    """
    q = (question or "").strip()
    if not q:
        return False

    if has_strong_mail_intent(q):
        return False

    q_norm = _normalize_text_for_match(q)
    if any(kw in q_norm for kw in GLOSSARY_INTENT_KEYWORDS):
        return True

    if re.search(r"\b[A-Z]{2,8}\b", q):
        return True

    return False



def is_force_glossary_query(question: str) -> bool:
    """'용어'를 명시한 질문은 glossary 우선 경로를 강제"""
    q_norm = _normalize_text_for_match(question)
    if not q_norm:
        return False

    compact = q_norm.replace(" ", "")
    force_patterns = [
        "용어검색", "용어알려", "용어설명", "용어뜻", "약어검색", "약어설명"
    ]
    return any(p in compact for p in force_patterns)


def should_prioritize_recent_docs(question: str) -> bool:
    q_norm = _normalize_text_for_match(question)
    if not q_norm:
        return False
    compact = q_norm.replace(" ", "")
    return any(k in compact for k in RECENT_PRIORITY_KEYWORDS)


def is_glossary_result_relevant(
    question: str,
    docs: List[Dict[str, Any]],
    *,
    topk: int = 3,
    min_score: float = 0.35
) -> bool:
    """
    glossary 문서들에 대한 완화된 관련성 판정
    - glossary 전용 낮은 threshold 사용
    - topK 중 하나라도 질문 키워드/약어가 매칭되면 True
    """
    if not docs:
        return False

    gdocs = [d for d in docs if is_glossary_doc(d)]
    if not gdocs:
        return False

    target_docs = gdocs[:max(1, topk)]
    keywords = _extract_query_keywords(question)
    abbreviations = re.findall(r"\b[A-Z]{2,8}\b", question or "")

    for doc in target_docs:
        combined_score = float(doc.get("_combined_score") or 0.0)
        if combined_score < min_score:
            continue

        title = str(doc.get("title") or "")
        content = str(doc.get("content") or doc.get("merge_title_content") or "")
        haystack = _normalize_text_for_match(title + " " + content)

        if any(abbr.lower() in haystack for abbr in abbreviations):
            return True
        if any(kw in haystack for kw in keywords):
            return True

    return False

def format_rag_context(documents: List[Dict[str, Any]], max_docs: int = 3) -> str:
    if not documents:
        return ""
    context_parts = []
    for i, doc in enumerate(documents[:max_docs], 1):
        title = doc.get("title", doc.get("doc_id", "")) or "제목 없음"
        content = doc.get("content", "") or doc.get("merge_title_content", "") or ""
        index = doc.get("_index", "")
        doc_date = doc.get("_doc_date", "날짜 정보 없음")
        combined = doc.get("_combined_score", doc.get("_score", 0))
        url = doc.get("confluence_mail_page_url", "") or doc.get("url", "")
        context_parts.append(
            f"[문서 {i}]\n"
            f"제목: {title}\n"
            f"문서일시: {doc_date}\n"
            f"종합점수: {combined}\n"
            f"인덱스: {index}\n"
            f"내용: {_truncate_text(content, 2200)}\n"
            f"출처: {url}"
        )
    return "\n\n".join(context_parts)


def retrieve_rag_documents_parallel(queries: List[str], *, top_k: int, indexes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    query_list = [q.strip() for q in queries if q and q.strip()]
    if not query_list:
        return []

    all_documents: List[Dict[str, Any]] = []
    max_workers = min(len(query_list), MAX_RAG_QUERIES, 2)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(search_rag_documents, query, indexes=indexes, top_k=top_k, mode=RAG_RETRIEVE_MODE): query
            for query in query_list
        }
        for future in as_completed(future_map):
            query = future_map[future]
            try:
                docs = future.result()
                print(f"[RAG] 병렬 검색 완료: query={query} docs={len(docs)}")
                all_documents.extend(docs)
            except Exception as e:
                print(f"[RAG] 병렬 검색 실패: query={query} err={e}")

    return all_documents


LLM_BUSY_MESSAGE = "지금 답변 생성 중입니다. 완료 후 다시 질문해주세요."
LLM_QUEUE_FULL_MESSAGE = "요청이 많아 잠시 후 다시 시도해주세요."
llm_job_queue: "queue.Queue[dict]" = queue.Queue(maxsize=LLM_JOB_QUEUE_MAX)
llm_task_state_lock = threading.Lock()
llm_job_state_lock = threading.Lock()
llm_job_state: Dict[str, str] = {}  # queued|running|done|failed
llm_notice_lock = threading.Lock()
llm_notice_state: Dict[str, List[Tuple[int, int]]] = {}
inflight: Dict[str, bool] = {}
inflight_lock = threading.Lock()
llm_sem = threading.Semaphore(LLM_MAX_CONCURRENT)
llm_workers_started = False
job_metrics_lock = threading.Lock()
job_metrics = {
    "minute": "",
    "count": 0,
}


def _memory_db_path() -> str:
    if MEMORY_DB_PATH:
        return MEMORY_DB_PATH
    store_db_path = getattr(store, "DB_PATH", "")
    if store_db_path:
        return store_db_path
    return os.path.join(os.getcwd(), "gocllm.db")


def init_conversation_memory_db():
    if not (ENABLE_CONVERSATION_MEMORY or ENABLE_CONVERSATION_STATE):
        return
    with sqlite3.connect(_memory_db_path()) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope_id TEXT NOT NULL,
                room_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_chat_memory_scope_id_id ON chat_memory(scope_id, id)")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_context_state (
                scope_id TEXT PRIMARY KEY,
                topic TEXT,
                time_label TEXT,
                last_query TEXT,
                updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
            )
            """
        )
        conn.commit()


def _memory_enabled_for_chat(chat_type: str) -> bool:
    if not ENABLE_CONVERSATION_MEMORY:
        return False
    if MEMORY_ONLY_SINGLE and (chat_type or "").upper() != "SINGLE":
        return False
    return True


def _is_context_dependent_question(question: str) -> bool:
    q = (question or "").strip().lower()
    if not q:
        return False
    patterns = ["그거", "아까", "이전", "방금", "담당자는", "왜", "언제", "뭐야", "그게", "그건", "그 내용"]
    return any(p in q for p in patterns) or len(q) <= 8


def _trim_memory_content(role: str, content: str) -> str:
    text = re.sub(r"\s+", " ", (content or "")).strip()
    if role == "assistant" and MEMORY_SUMMARIZE_ASSISTANT:
        text = text[: MEMORY_MAX_CHARS_PER_MESSAGE * 2]
    if len(text) <= MEMORY_MAX_CHARS_PER_MESSAGE:
        return text
    return text[:MEMORY_MAX_CHARS_PER_MESSAGE] + " ..."


def save_conversation_memory(*, scope_id: str, room_id: str, user_id: str, role: str, content: str, chat_type: str):
    if not _memory_enabled_for_chat(chat_type):
        return
    if role not in ("user", "assistant"):
        return
    trimmed = _trim_memory_content(role, content)
    if not trimmed:
        return
    with sqlite3.connect(_memory_db_path()) as conn:
        conn.execute(
            "INSERT INTO chat_memory(scope_id, room_id, user_id, role, content) VALUES (?, ?, ?, ?, ?)",
            (str(scope_id), str(room_id), str(user_id or ""), role, trimmed),
        )
        conn.execute(
            """
            DELETE FROM chat_memory
            WHERE scope_id = ? AND id NOT IN (
                SELECT id FROM chat_memory WHERE scope_id = ? ORDER BY id DESC LIMIT ?
            )
            """,
            (str(scope_id), str(scope_id), MEMORY_MAX_TURNS),
        )
        conn.commit()


def load_conversation_memory(*, scope_id: str, chat_type: str) -> List[Dict[str, str]]:
    if not _memory_enabled_for_chat(chat_type):
        return []
    with sqlite3.connect(_memory_db_path()) as conn:
        rows = conn.execute(
            "SELECT role, content FROM chat_memory WHERE scope_id = ? ORDER BY id DESC LIMIT ?",
            (str(scope_id), MEMORY_MAX_TURNS),
        ).fetchall()
    rows = list(reversed(rows))
    return [{"role": r[0], "content": r[1]} for r in rows]


def clear_conversation_memory(scope_id: str):
    if not ENABLE_CONVERSATION_MEMORY:
        return
    with sqlite3.connect(_memory_db_path()) as conn:
        conn.execute("DELETE FROM chat_memory WHERE scope_id = ?", (str(scope_id),))
        conn.commit()


def load_conversation_state(scope_id: str) -> Dict[str, str]:
    if not ENABLE_CONVERSATION_STATE:
        return {}
    with sqlite3.connect(_memory_db_path()) as conn:
        row = conn.execute(
            "SELECT topic, time_label, last_query FROM chat_context_state WHERE scope_id = ?",
            (str(scope_id),),
        ).fetchone()
    if not row:
        return {}
    return {
        "topic": (row[0] or "").strip(),
        "time_label": (row[1] or "").strip(),
        "last_query": (row[2] or "").strip(),
    }


def save_conversation_state(scope_id: str, *, topic: str = "", time_label: str = "", last_query: str = ""):
    if not ENABLE_CONVERSATION_STATE:
        return
    with sqlite3.connect(_memory_db_path()) as conn:
        conn.execute(
            """
            INSERT INTO chat_context_state(scope_id, topic, time_label, last_query, updated_at)
            VALUES (?, ?, ?, ?, datetime('now', 'localtime'))
            ON CONFLICT(scope_id) DO UPDATE SET
                topic=excluded.topic,
                time_label=excluded.time_label,
                last_query=excluded.last_query,
                updated_at=datetime('now', 'localtime')
            """,
            (str(scope_id), (topic or "").strip(), (time_label or "").strip(), (last_query or "").strip()),
        )
        conn.commit()


def _extract_topic_from_question(question: str) -> str:
    q = normalize_query_for_search(question)
    if not q:
        return ""
    stop = {
        "이슈", "정리", "요약", "현황", "최근", "최신", "이번주", "지난주", "저번주", "사항", "알려줘", "해줘", "뭐야", "그거", "저기", "그중"
    }
    toks = [t for t in q.split() if len(t) >= 2 and t not in stop]
    if not toks:
        return ""
    # 영어 대문자 토큰(HBM/FLASH 등)을 우선
    for t in toks:
        if re.fullmatch(r"[A-Z0-9_\-]{2,20}", t):
            return t
    return toks[0]


def _extract_time_label_from_question(question: str, time_range: Optional[Dict[str, Any]]) -> str:
    if time_range and time_range.get("label"):
        return str(time_range.get("label")).split("(")[0].strip()
    q = re.sub(r"\s+", "", question or "")
    if any(k in q for k in ("이번주", "금주")):
        return "이번주"
    if any(k in q for k in ("지난주", "저번주", "전주")):
        return "지난주"
    if any(k in q for k in ("최근", "최신", "요즘", "근래")):
        return "최근"
    return ""


def _build_effective_question(question: str, *, scope_id: str, time_range: Optional[Dict[str, Any]]) -> Tuple[str, Dict[str, str]]:
    state = load_conversation_state(scope_id)
    q = (question or "").strip()
    topic_now = _extract_topic_from_question(q)
    time_now = _extract_time_label_from_question(q, time_range)

    use_state = _is_context_dependent_question(q) or any(x in q for x in ("그거", "저기", "그중", "방금", "아까"))
    topic_eff = topic_now or (state.get("topic", "") if use_state else "")
    time_eff = time_now or (state.get("time_label", "") if use_state else "")

    prefix_parts = []
    if topic_eff and not topic_now:
        prefix_parts.append(f"주제={topic_eff}")
    if time_eff and not time_now:
        prefix_parts.append(f"기간={time_eff}")

    effective = q if not prefix_parts else f"[{', '.join(prefix_parts)}] {q}"
    return effective, {"topic": topic_eff, "time_label": time_eff}


def build_memory_text(memory_messages: List[Dict[str, str]]) -> str:
    if not memory_messages:
        return ""
    lines = []
    total_chars = 0
    hard_limit = MEMORY_MAX_TURNS * MEMORY_MAX_CHARS_PER_MESSAGE
    for m in memory_messages:
        role = "사용자" if m.get("role") == "user" else "어시스턴트"
        line = f"- {role}: {(m.get('content') or '').strip()}"
        if total_chars + len(line) > hard_limit:
            break
        lines.append(line)
        total_chars += len(line)
    return "\n".join(lines)


def _mark_job_counter():
    now_minute = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M")
    with job_metrics_lock:
        if job_metrics["minute"] != now_minute:
            job_metrics["minute"] = now_minute
            job_metrics["count"] = 0
        job_metrics["count"] += 1
        return job_metrics["minute"], job_metrics["count"]


def _register_llm_notice(req_id: str, resp: Any):
    if not req_id:
        return
    try:
        mid, st = extract_msgid_senttime(resp if isinstance(resp, dict) else {})
        if mid is None or st is None:
            return
        with llm_notice_lock:
            llm_notice_state.setdefault(req_id, []).append((int(mid), int(st)))
    except Exception as e:
        print(f"[LLM][{req_id}] notice register failed: {e}")


def _recall_llm_notices(chatroom_id: int, req_id: str):
    if not ENABLE_RECALL or not req_id or chatBot is None:
        return
    with llm_notice_lock:
        notices = llm_notice_state.pop(req_id, [])
    for mid, st in notices:
        try:
            chatBot.recall_message(chatroom_id, int(mid), int(st))
        except Exception as e:
            print(f"[LLM][{req_id}] notice recall failed: {e}")


def enqueue_llm_job(job: Dict[str, Any]) -> bool:
    try:
        llm_job_queue.put_nowait(job)
        qsize = llm_job_queue.qsize()
        req_id = str(job.get("request_id") or "")
        if req_id:
            with llm_job_state_lock:
                llm_job_state[req_id] = "queued"
        print(f"[LLM][{job.get('request_id')}] enqueue ok qsize={qsize}")
        return True
    except queue.Full:
        print(f"[LLM][{job.get('request_id')}] enqueue failed queue full")
        return False


def _build_user_key(task: Dict[str, Any]) -> str:
    sender_knox = (task.get("sender_knox") or "").strip()
    sender_name = (task.get("sender_name") or "").strip()
    if sender_knox:
        return sender_knox
    if sender_name:
        return sender_name
    return str(task.get("chatroom_id"))


def schedule_long_wait_notice(task: Dict[str, Any], delay_sec: float = 6.0):
    req_id = str(task.get("request_id") or "")
    chatroom_id = task.get("chatroom_id")
    if not req_id or not chatroom_id:
        return

    def _notify_if_still_running():
        try:
            time.sleep(delay_sec)
            with llm_job_state_lock:
                state = llm_job_state.get(req_id)
            if state in ("queued", "running"):
                resp = chatBot.send_text(chatroom_id, "⏳ 아직 분석 중입니다. 문서 확인 후 정리해서 보내드리겠습니다.")
                _register_llm_notice(req_id, resp)
        except Exception as e:
            print(f"[LLM][{req_id}] long-wait notice failed: {e}")

    threading.Thread(target=_notify_if_still_running, daemon=True).start()


def llm_worker_loop(worker_name: str):
    while True:
        task = llm_job_queue.get()
        request_id = task.get("request_id")
        requested_at = float(task.get("requested_at") or time.time())
        dequeued_at = time.time()
        user_key = _build_user_key(task)

        with inflight_lock:
            if inflight.get(user_key):
                try:
                    chatBot.send_text(task["chatroom_id"], LLM_BUSY_MESSAGE)
                except Exception as send_err:
                    print(f"[{worker_name}][{request_id}] busy msg failed: {send_err}")
                print(f"[{worker_name}][{request_id}] dropped by inflight user_key={user_key}")
                llm_job_queue.task_done()
                continue
            inflight[user_key] = True

        if request_id:
            with llm_job_state_lock:
                llm_job_state[str(request_id)] = "running"

        rag_calls = 0
        llm_calls = 0
        used_rag = False
        fallback_reason = ""
        memory_hit = False
        memory_message_count = 0
        memory_prompt_chars = 0
        rewrite_used_memory = False
        try:
            with llm_sem:
                minute, minute_count = _mark_job_counter()
                stats = process_llm_chat_background(task)
                rag_calls = int(stats.get("rag_calls", 0))
                llm_calls = int(stats.get("llm_calls", 0))
                used_rag = bool(stats.get("used_rag", False))
                fallback_reason = str(stats.get("fallback_reason", ""))
                memory_hit = bool(stats.get("memory_hit", False))
                memory_message_count = int(stats.get("memory_message_count", 0))
                memory_prompt_chars = int(stats.get("memory_prompt_chars", 0))
                rewrite_used_memory = bool(stats.get("rewrite_used_memory", False))
                total_latency = time.time() - requested_at
                queue_wait = dequeued_at - requested_at
                print(
                    f"[{worker_name}][{request_id}] done queue_wait={queue_wait:.2f}s total={total_latency:.2f}s "
                    f"rag_calls={rag_calls} llm_calls={llm_calls} used_rag={used_rag} "
                    f"memory_hit={memory_hit} memory_message_count={memory_message_count} memory_prompt_chars={memory_prompt_chars} "
                    f"rewrite_used_memory={rewrite_used_memory} fallback_reason={fallback_reason} rpm={minute_count}@{minute}"
                )
        except Exception as e:
            print(f"[{worker_name}][{request_id}] unexpected worker error: {e}")
            if request_id:
                with llm_job_state_lock:
                    llm_job_state[str(request_id)] = "failed"
        finally:
            if request_id:
                with llm_job_state_lock:
                    if llm_job_state.get(str(request_id)) != "failed":
                        llm_job_state[str(request_id)] = "done"
                try:
                    _recall_llm_notices(int(task.get("chatroom_id")), str(request_id))
                except Exception as e:
                    print(f"[{worker_name}][{request_id}] recall notices failed: {e}")
            with inflight_lock:
                inflight[user_key] = False
            llm_job_queue.task_done()


def start_llm_workers():
    global llm_workers_started
    if llm_workers_started:
