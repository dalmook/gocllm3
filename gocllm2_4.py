### PART 4/5
    try:
        with open(TERM_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        data = _clean_xa0(data)
    except Exception:
        # 파일 못 읽으면 안내 카드
        return ui.build_term_not_found_card(q)

    qn = q.lower().replace(" ", "")

    exact = []
    starts = []
    scored = []

    for item in (data or []):
        term = (item.get("title") or "").strip()
        if not term:
            continue
        tn = term.lower().replace(" ", "")
        sim = _sim(tn, qn)

        rec = {
            "subject": (item.get("subject") or "").strip(),
            "term": term,
            "content": (item.get("content") or "").strip(),
            "link": (item.get("link") or "").strip(),
        }

        if term == q:
            exact.append((sim, rec))
        elif term.startswith(q):
            starts.append((sim, rec))
        elif qn in tn:
            scored.append((sim, rec))
        else:
            # 완전 불일치일 때도 유사도 높은 것 일부 포함(너무 낮으면 제외)
            if sim >= 0.70:
                scored.append((sim, rec))

    # 정렬/컷
    exact = sorted(exact, key=lambda x: x[0], reverse=True)[:5]
    starts = sorted(starts, key=lambda x: x[0], reverse=True)[:5]
    scored = sorted(scored, key=lambda x: x[0], reverse=True)[:9]

    merged = [r for _, r in (exact + starts + scored)]

    # 중복 제거(term+link 기준)
    seen = set()
    uniq = []
    for r in merged:
        key = (r.get("term",""), r.get("link",""))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)

    if not uniq:
        return ui.build_term_not_found_card(q)

    return ui.build_term_search_results_card(q, uniq)

def run_ps_query(params: dict) -> pd.DataFrame:
    """
    PS 파트조회 공용 러너
    - gubun: pscomp01 / psmodule01 / psmultichip01
    - conv : psfab02 / pseds03 / psasy04 / pstst05 / psmod06(모듈전용)
    - q    : 검색값
    """
    gubun = (params.get("gubun") or params.get("psgubun01") or "").strip()   # 구분
    conv  = (params.get("conv")  or params.get("psconv01")  or "").strip()   # 조회기준
    qraw  = (params.get("q")     or params.get("result")    or "").strip()   # 검색어

    if not qraw:
        return pd.DataFrame([{"Result": "코드 입력값이 비었습니다."}])
    if len(qraw.strip()) < 3:
        return pd.DataFrame([{"Result": "코드는 3자 이상 입력하세요."}])

    # ✅ MOD_CODE는 MODULE에서만 허용
    if conv == "psmod06" and gubun != "psmodule01":
        return pd.DataFrame([{"Result": "MOD_CODE(psmod06)는 MODULE에서만 조회 가능합니다."}])

    # ✅ 입력 normalize + like 처리 (바인딩)
    q = _likeify2(qraw.upper().replace(" ", ""))

    # ✅ gubun별 SQL 선택 + where 컬럼 맵(예전 코드와 동일한 컬럼들)
    if gubun == "pscomp01":
        sql = getattr(ui, "SQL_PS_COMP_BASE", "") or ""
        where_map = {
            "psfab02": "A.FOUT_CODE LIKE :q",
            "pseds03": "A.EFU_CODE  LIKE :q",
            "psasy04": "A.ABD_CODE  LIKE :q",
            "pstst05": "A.TFN_CODE  LIKE :q",
        }
        default_conv = "pseds03"

    elif gubun == "psmodule01":
        sql = getattr(ui, "SQL_PS_MODULE_BASE", "") or ""
        where_map = {
            "psfab02": "B.FAB_CODE  LIKE :q",
            "pseds03": "B.EFU_CODE  LIKE :q",
            "psasy04": "B.ABD_CODE  LIKE :q",
            "pstst05": "A.COMPCODE  LIKE :q",
            "psmod06": "A.PRODCODE  LIKE :q",
        }
        default_conv = "pseds03"

    elif gubun == "psmultichip01":
        sql = getattr(ui, "SQL_PS_MCP_BASE", "") or ""
        where_map = {
            "psfab02": "B.FOUT_CODE LIKE :q",
            "pseds03": "A.CHIPCODE  LIKE :q",
            "psasy04": "A.PRODCODE  LIKE :q",
            "pstst05": "C.TFN_CODE  LIKE :q",
        }
        default_conv = "pseds03"

    else:
        return pd.DataFrame([{"Result": f"알 수 없는 gubun: {gubun}"}])

    if not (sql or "").strip():
        return pd.DataFrame([{"Result": f"PS SQL이 비어있음: ui SQL 정의 확인 (gubun={gubun})"}])

    # ✅ conv가 이상하면 기본값으로
    where_clause = where_map.get(conv) or where_map.get(default_conv) or ""

    # ✅ SQL에 {where_clause}가 있으면 채워줌(없으면 그대로 실행)
    if "{where_clause}" in sql:
        sql = sql.format(where_clause=where_clause)

    return run_oracle_query(sql, params={"q": q})


# 기존 RUNNERS가 있으면 아래만 추가, 없으면 RUNNERS 선언 후 추가


RUNNERS: Dict[str, Any] = {}
RUNNERS["TERM_SEARCH"] = run_term_search
RUNNERS["ONEVIEW_SHIP"] = run_oneview_ship
RUNNERS["PKGCODE"] = run_pkgcode
RUNNERS["PS_QUERY"] = run_ps_query

llm_allowed_users_cache_lock = threading.Lock()
llm_allowed_users_cache: set[str] = set()
llm_allowed_users_cache_expire_at = 0.0


def _normalize_sender_knox_id(sender_knox: str) -> str:
    return (sender_knox or "").strip().lower()


def _fetch_llm_allowed_users() -> set[str]:
    if not (LLM_ALLOWED_USERS_SQL or "").strip():
        return set()

    df = run_oracle_query(LLM_ALLOWED_USERS_SQL)
    if df is None or df.empty:
        return set()

    target_col = None
    for col in df.columns:
        if str(col).lower() in ("senderknoxid", "sso_id", "ssoid"):
            target_col = col
            break
    if target_col is None:
        target_col = df.columns[0]

    allowed_users = set()
    for value in df[target_col].dropna().tolist():
        normalized = _normalize_sender_knox_id(str(value))
        if normalized:
            allowed_users.add(normalized)
    return allowed_users


def is_llm_allowed_user(sender_knox: str) -> bool:
    global llm_allowed_users_cache_expire_at

    normalized = _normalize_sender_knox_id(sender_knox)
    if not normalized:
        return False

    now_ts = time.time()
    with llm_allowed_users_cache_lock:
        if now_ts < llm_allowed_users_cache_expire_at:
            return normalized in llm_allowed_users_cache

    try:
        allowed_users = _fetch_llm_allowed_users()
    except Exception as e:
        print(f"[LLM allowlist load failed] {e}")
        return False

    expire_at = now_ts + LLM_ALLOWED_USERS_CACHE_TTL_SEC
    with llm_allowed_users_cache_lock:
        llm_allowed_users_cache.clear()
        llm_allowed_users_cache.update(allowed_users)
        llm_allowed_users_cache_expire_at = expire_at
        return normalized in llm_allowed_users_cache

def run_rightperson(params: dict) -> pd.DataFrame:
    q = (params.get("q") or "").strip()
    if not q:
        return pd.DataFrame([{"Result": "검색어를 입력하세요."}])

    # 1) Oracle
    df_oracle = run_oracle_query(ui.SQL_RIGHTPERSON_ORACLE)

    # 2) JSON (옵션)
    df_json = pd.DataFrame()
    if RIGHTPERSON_JSON_URL:
        try:
            r = requests.get(RIGHTPERSON_JSON_URL, timeout=5)
            r.raise_for_status()
            df_json = pd.DataFrame(r.json())
        except Exception:
            df_json = pd.DataFrame()

    cols = ["부서","담당제품","팀장","PL","TL","실무담당자","비고"]
    for df in (df_oracle, df_json):
        for c in cols:
            if c not in df.columns:
                df[c] = ""

    combined = pd.concat([df_json[cols], df_oracle[cols]], ignore_index=True)

    mask = (
        combined["부서"].astype(str).str.contains(q, case=False, na=False) |
        combined["담당제품"].astype(str).str.contains(q, case=False, na=False) |
        combined["팀장"].astype(str).str.contains(q, case=False, na=False) |
        combined["PL"].astype(str).str.contains(q, case=False, na=False) |
        combined["TL"].astype(str).str.contains(q, case=False, na=False) |
        combined["실무담당자"].astype(str).str.contains(q, case=False, na=False) |
        combined["비고"].astype(str).str.contains(q, case=False, na=False)
    )

    out = combined[mask].drop_duplicates().reset_index(drop=True)
    return out if not out.empty else pd.DataFrame([{"Result": f"검색 결과 없음: {q}"}])

RUNNERS["RIGHTPERSON"] = run_rightperson
# =========================
# 6) Sender userID / DM room
# =========================
def get_sender_user_id(info: dict) -> str | None:
    for k in ("senderUserId", "senderUserID", "senderUid", "senderId"):
        v = info.get(k)
        if v:
            return str(v)

    sk = (info.get("senderKnoxId") or "").strip()
    if sk.isdigit():
        return sk

    if sk:
        try:
            if chatBot is not None:
                ids = chatBot.resolve_user_ids_from_loginids([sk])
                if ids:
                    return str(ids[0])
        except:
            pass
    return None


def get_or_create_dm_room_for_user(
    sender_user_id: str,
    sender_name: str = "",
    *,
    chat_type: str | None = None,
    current_room_id: int | None = None,
) -> int | None:
    # ✅ 안전장치: SINGLE 컨텍스트면 "새로 만들지 말고" 현재 방을 DM으로 바인딩
    ct = (chat_type or "").upper()
    if ct == "SINGLE" and current_room_id:
        try:
            store.dm_set_room(sender_user_id, str(current_room_id))
        except Exception as e:
            print("[DM bind failed]", e)
        return int(current_room_id)

    cached = store.dm_get_room(sender_user_id)
    if cached:
        return int(cached)

    try:
        if chatBot is None:
            return None
        title = f"공급망봇 · {sender_name}".strip() if sender_name else None
        rid = chatBot.room_create([str(sender_user_id)], chatType=1, chatroom_title=title)
        store.dm_set_room(sender_user_id, str(rid))
        return int(rid)
    except Exception as e:
        print("[DM create failed]", e)
        return None


# ✅ (추가) 단체방에서 눌러도 UI/결과는 DM으로 보내는 라우터
def route_ui_room(chatroom_id: int, info: dict, sender_name: str = "") -> int:
    sender_user_id = get_sender_user_id(info)
    try:
        if (info.get("chatType") or "").upper() == "SINGLE" and sender_user_id:
            store.dm_set_room(str(sender_user_id), str(chatroom_id))  # ← 네 store 함수명에 맞춰 조정
    except Exception as e:
        print("[dm_room bind failed]", e)
    # ✅ SINGLE(1:1)은 원래 방에서 바로 응답
    chat_type = (info.get("chatType") or "").upper()
    if chat_type == "SINGLE":
        return chatroom_id

    # ✅ 단체방에서만 DM 라우팅
    if chat_type != "GROUP":
        return chatroom_id
    if not sender_user_id:
        return chatroom_id

    dm_room = get_or_create_dm_room_for_user(
    sender_user_id,
    sender_name,
    chat_type=chat_type,
    current_room_id=chatroom_id,
)

    return int(dm_room) if dm_room else chatroom_id



# =========================
# 7) Scheduler Jobs
# =========================
def job_issue_deadline_reminder_daily():
    today = datetime.now().date()
    issues = store.issue_list_open_all()
    if not issues:
        return

    to_send: Dict[str, List[Tuple[int, dict, str]]] = {}

    for it in issues:
        td = store._parse_ymd(it.get("target_date", ""))
        if not td:
            continue
        d = (td - today).days
        if d not in store.REMIND_DAYS:
            continue

        memo = f"D-{d}|{today.isoformat()}"
        if store.issue_event_exists(int(it["issue_id"]), "REMIND", memo):
            continue

        room = str(it.get("chatroom_id", "")).strip()
        if not room:
            continue

        to_send.setdefault(room, []).append((d, it, memo))

    if not to_send:
        return

    for room, items in to_send.items():
        try:
            if chatBot is None:
                print("[job_issue_deadline_reminder_daily] KNOX 연결 안됨, 건너뜀")
                continue
            items.sort(key=lambda x: (x[0], int(x[1]["issue_id"])))
            today_str = today.strftime("%Y-%m-%d")
            card = ui.build_issue_deadline_reminder_card([(d, it) for d, it, _memo in items], today_str)
            chatBot.send_adaptive_card(int(room), card)

            for _d, it, _memo in items:
                store.issue_event_add(int(it["issue_id"]), "REMIND", actor="BOT", memo=_memo)

        except Exception as e:
            print("job_issue_deadline_reminder_daily error:", e)


def job_warning_daily():
    rooms = store.get_watch_rooms()
    if not rooms:
        return

    try:
        if chatBot is None:
            print("[job_warning_daily] KNOX 연결 안됨, 건너뜀")
            return
        df = run_oracle_query(ui.SQL_WARN)
        msg = "⚠️ [워닝 테스트]\n" + ui.format_df_brief(df, 5)
        for rid in rooms:
            chatBot.send_text(int(rid), msg)
    except Exception as e:
        print("job_warning_daily error:", e)


# (바로 위 코드)
KR_HOLIDAYS = holidays.KR()  # 대한민국 공휴일(대체공휴일 포함)

def job_issue_summary_daily():
    now = datetime.now(ZoneInfo("Asia/Seoul"))
    today = now.date()

    # ✅ 토/일 + 공휴일 스킵
    if today.weekday() >= 5 or today in KR_HOLIDAYS:  # 5=토, 6=일
        return

    rooms = store.get_watch_rooms()
    if not rooms:
        return

    try:
        if chatBot is None:
            print("[job_issue_summary_daily] KNOX 연결 안됨, 건너뜀")
            return
        today_str = now.strftime("%Y-%m-%d")

        for rid in rooms:
            issues = store.issue_list_open(str(rid))
            if not issues:
                continue

            for it in issues:
                it["d_day"] = store._dday(it.get("target_date", ""))

            issues.sort(key=lambda x: (
                999999 if x.get("d_day") is None else x.get("d_day"),
                int(x.get("issue_id", 0))
            ))

            card = ui.build_issue_summary_card(issues, today_str=today_str, max_items=15)
            chatBot.send_adaptive_card(int(rid), card)

    except Exception as e:
        print("job_issue_summary_daily error:", e)

def run_warning_once_to_chatroom(chatroom_id: int):
    if chatBot is None:
        print("[run_warning_once_to_chatroom] KNOX 연결 안됨")
        return
    df = run_oracle_query(ui.SQL_WARN)
    if df is None or df.empty:
        chatBot.send_text(chatroom_id, "워닝 조건: 현재 0건 ✅")
    else:
        chatBot.send_text(chatroom_id, "⚠️ 워닝 결과\n" + ui.format_df_brief(df, 10))


# =========================
# 8) FastAPI App
# =========================
app = FastAPI()
scheduler = BackgroundScheduler(timezone="Asia/Seoul")
chatBot: KnoxMessenger  # startup에서 초기화

@app.get("/api/dashboard/rooms")
def api_dashboard_rooms(token: str | None = Query(default=None)):
    _require_dashboard_token(token)
    return {"rooms": store.list_watch_rooms()}

@app.get("/api/dashboard/summary")
def api_dashboard_summary(
    token: str | None = Query(default=None),
### PART 3/3
    room_id: str | None = Query(default=None),
):
    _require_dashboard_token(token)

    today = store._today()
    open_issues = store.issue_list_open_all()
    closed_recent = store.issue_list_closed_recent(days=60)

    if room_id:
        open_issues = [x for x in open_issues if str(x.get("chatroom_id","")) == str(room_id)]

    last_map = store.get_last_activity_map([int(x["issue_id"]) for x in open_issues])

    overdue = 0
    due_7 = 0
    due_3 = 0
    no_target = 0
    long_open_14 = 0
    owner_cnt = defaultdict(int)

    urgent_list = []
    old_list = []
    stale_list = []

    for it in open_issues:
        d = store._dday(it.get("target_date",""))
        age = store._age_days(it.get("created_at",""))
        owner = (it.get("owner") or "").strip() or "(미지정)"
        owner_cnt[owner] += 1

        if d is None:
            no_target += 1
        else:
            if d < 0:
                overdue += 1
            if 0 <= d <= 7:
                due_7 += 1
            if 0 <= d <= 3:
                due_3 += 1

        if age >= 14:
            long_open_14 += 1

        urgent_list.append({
            "issue_id": it["issue_id"],
            "title": it.get("title",""),
            "owner": it.get("owner",""),
            "target_date": it.get("target_date",""),
            "d_day": d,
            "url": it.get("url",""),
        })

        old_list.append({
            "issue_id": it["issue_id"],
            "title": it.get("title",""),
            "owner": it.get("owner",""),
            "created_at": it.get("created_at",""),
            "age_days": age,
            "url": it.get("url",""),
        })

        last_evt = last_map.get(int(it["issue_id"]), "") or it.get("created_at","")
        last_dt = store._parse_dt(last_evt)
        if last_dt:
            stale_days = (datetime.now().date() - last_dt.date()).days
            stale_list.append({
                "issue_id": it["issue_id"],
                "title": it.get("title",""),
                "owner": it.get("owner",""),
                "last_event_at": last_evt,
                "stale_days": stale_days,
                "url": it.get("url",""),
            })

    urgent_list.sort(key=lambda x: (999999 if x["d_day"] is None else x["d_day"], int(x["issue_id"])))
    old_list.sort(key=lambda x: (-x["age_days"], int(x["issue_id"])))
    stale_list.sort(key=lambda x: (-x["stale_days"], int(x["issue_id"])))

    owner_top = sorted(owner_cnt.items(), key=lambda kv: kv[1], reverse=True)[:8]
    owner_top = [{"owner": k, "open_cnt": v} for k, v in owner_top]

    series = store.build_week_series(
        created_rows=store.issue_list_all_any("OPEN") + closed_recent,
        closed_rows=closed_recent,
        weeks=8
    )

    cycle_days = []
    for it in closed_recent:
        c = store._parse_dt(it.get("created_at",""))
        e = store._parse_dt(it.get("closed_at",""))
        if c and e:
            cycle_days.append((e.date() - c.date()).days)
    avg_cycle = round(sum(cycle_days)/len(cycle_days), 1) if cycle_days else None

    kpi = {
        "open_total": len(open_issues),
        "overdue": overdue,
        "due_7": due_7,
        "due_3": due_3,
        "no_target": no_target,
        "long_open_14": long_open_14,
        "red_alert": overdue + due_3,
        "avg_cycle_days_60d": avg_cycle,
        "today": today.isoformat(),
    }

    return {
        "kpi": kpi,
        "owner_top": owner_top,
        "series": series,
        "urgent_top10": urgent_list[:10],
        "old_top10": old_list[:10],
        "stale_top10": stale_list[:10],
    }

@app.get("/api/dashboard/issues")
def api_dashboard_issues(
    token: str | None = Query(default=None),
    room_id: str | None = Query(default=None),
    status: str = Query(default="OPEN"),
    owner: str | None = Query(default=None),
    q: str | None = Query(default=None),
    page: int = Query(default=0),
    size: int = Query(default=50),
):
    _require_dashboard_token(token)

    rows = store.issue_list_all_any(None if status == "ALL" else status)

    if room_id:
        rows = [r for r in rows if str(r.get("chatroom_id","")) == str(room_id)]
    if owner:
        rows = [r for r in rows if owner.lower() in (r.get("owner","") or "").lower()]
    if q:
        qq = q.lower()
        rows = [r for r in rows if qq in (r.get("title","") or "").lower() or qq in (r.get("content","") or "").lower()]

    for r in rows:
        r["d_day"] = store._dday(r.get("target_date",""))
        r["age_days"] = store._age_days(r.get("created_at",""))

    if status == "OPEN":
        rows.sort(key=lambda x: (999999 if x["d_day"] is None else x["d_day"], -x["age_days"], int(x["issue_id"])))
    else:
        rows.sort(key=lambda x: int(x["issue_id"]), reverse=True)

    total = len(rows)
    start = page * size
    end = start + size
    return {"total": total, "page": page, "size": size, "items": rows[start:end]}

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(token: str | None = Query(default=None)):
    if not DASHBOARD_TOKEN:
        t = token or ""
        return HTMLResponse(ui.DASHBOARD_HTML.replace("__DASHBOARD_TITLE__", DASHBOARD_TITLE).replace("__TOKEN__", t))
    if (token or "") == DASHBOARD_TOKEN:
        return HTMLResponse(ui.DASHBOARD_HTML.replace("__DASHBOARD_TITLE__", DASHBOARD_TITLE).replace("__TOKEN__", token or ""))

    return HTMLResponse(ui.DASHBOARD_LOGIN_HTML.replace("__DASHBOARD_TITLE__", DASHBOARD_TITLE))

# KNOX 재연결 함수
def job_knox_reconnect():
    global chatBot
    # 이미 연결되어 있으면 재시도 안함
    if chatBot is not None:
        return
    
    print("[knox_reconnect] KNOX 연결 시도...")
    try:
        new_chatBot = KnoxMessenger(host=KNOX_HOST, systemId=KNOX_SYSTEM_ID, token=KNOX_TOKEN)
        new_chatBot.device_regist()
        new_chatBot.getKeys()
        chatBot = new_chatBot
        print("[knox_reconnect] KNOX 연결 성공! chatBot 객체 재설정 완료")
    except Exception as e:
        print(f"[knox_reconnect] KNOX 연결 실패: {e}")


@app.on_event("startup")
def on_startup():
    global chatBot
    store.init_db()
    init_conversation_memory_db()
    start_llm_workers()
    print(f"[startup] LLM workers started: workers={LLM_WORKERS}, max_concurrent={LLM_MAX_CONCURRENT}, queue_max={LLM_JOB_QUEUE_MAX}")

    # KNOX 연결 - 실패해도 앱은 계속 실행
    try:
        chatBot = KnoxMessenger(host=KNOX_HOST, systemId=KNOX_SYSTEM_ID, token=KNOX_TOKEN)
        chatBot.device_regist()
        chatBot.getKeys()
        print("[startup] KNOX 연결 성공")
    except Exception as e:
        print(f"[startup] KNOX 연결 실패: {e}")
        print("[startup] KNOX 기능은 사용할 수 없지만 앱은 계속 실행됩니다.")
        print("[startup] 5분마다 자동 재연결을 시도합니다.")
        chatBot = None  # KNOX 기능 비활성화

    # 스케줄러 시작
    try:
        scheduler.add_job(job_issue_summary_daily, CronTrigger(hour=8, minute=00), id="issue_summary", replace_existing=True)
        # scheduler.add_job(job_issue_deadline_reminder_daily, CronTrigger(hour=8, minute=35), id="issue_deadline_remind", replace_existing=True)
        # KNOX 재연결 작업: 5분마다 실행
        scheduler.add_job(job_knox_reconnect, CronTrigger(minute="*/5"), id="knox_reconnect", replace_existing=True)
        scheduler.start()
        print("[startup] 스케줄러 시작 성공 (KNOX 재연결: 5분마다)")
    except Exception as e:
        print(f"[startup] 스케줄러 시작 실패: {e}")

    print("[BOOT] main =", __file__)
    print("[BOOT] store=", store.__file__)
    print("[BOOT] ui   =", ui.__file__)

    print("[startup] ready")


@app.post("/message")
async def post_message(request: Request):
    # KNOX 연결 안 된 경우
    if chatBot is None:
        return {"ok": False, "error": "KNOX 연결 안됨"}
    
    body = await request.body()
    info = json.loads(AESCipher(chatBot.key).decrypt(body))
    print(info)

    chatroom_id = int(info["chatroomId"])
    sender_name = info.get("senderName", "") or ""
    sender_knox = info.get("senderKnoxId", "") or ""
    sender = sender_name if sender_name else sender_knox

    action, payload = parse_action_payload(info)

    if action == "NOOP":
        return {"ok": True}

    try:
        if action == "OPEN_URL":
            url = (payload.get("url") or "").strip()
            title = (payload.get("title") or "🔗 바로가기").strip()
            if url:
                chatBot.send_adaptive_card(chatroom_id, ui.build_quicklink_card(title, url))
            else:
                chatBot.send_text(chatroom_id, "링크가 비어있어요.")
            return {"ok": True}

        elif action in ("HOME", "INTRO"):
            chatBot.send_adaptive_card(chatroom_id, ui.build_home_card(dashboard_url=DASHBOARD_URL, infocenter_url=INFOCENTER_URL))


        elif action == "WARN_RUN":
            run_warning_once_to_chatroom(chatroom_id)

        elif action == "QUICK_LINKS":
            ui_room = route_ui_room(chatroom_id, info, sender_name)  # ✅ GROUP이면 DM, SINGLE이면 그대로
            chatBot.send_adaptive_card(ui_room, ui.build_quick_links_card(QUICK_LINK_ALIASES))
            return {"ok": True}
        
        # ---------- LLM Chatbot ----------
        elif action == "LLM_CHAT":
            if not is_llm_allowed_user(sender_knox):
                chatBot.send_adaptive_card(chatroom_id, ui.build_home_card(dashboard_url=DASHBOARD_URL, infocenter_url=INFOCENTER_URL))
                return {"ok": True}

            question = (payload.get("question") or "").strip()
            if not question:
                chatBot.send_text(chatroom_id, "질문 내용이 비어있습니다. /ask 질문내용 또는 질문:내용 형식으로 입력해주세요.")
                return {"ok": True}

            chat_type = (info.get("chatType") or "").upper()
            scope_id = str(chatroom_id)
            if question in MEMORY_RESET_COMMANDS:
                clear_conversation_memory(scope_id)
                chatBot.send_text(chatroom_id, "🧹 해당 1:1 대화 메모리를 초기화했습니다.")
                return {"ok": True}

            skip_memory_save = {
                "INTRO", "HOME", "", "/home", "홈"
            }
            if question not in skip_memory_save:
                save_conversation_memory(
                    scope_id=scope_id,
                    room_id=str(chatroom_id),
                    user_id=sender_knox,
                    role="user",
                    content=question,
                    chat_type=chat_type,
                )

            try:
                request_id = str(uuid.uuid4())
                job = {
                    "chatroom_id": chatroom_id,
                    "sender_knox": sender_knox,
                    "sender_name": sender_name,
                    "chat_type": chat_type,
                    "scope_id": scope_id,
                    "question": question,
                    "requested_at": time.time(),
                    "request_id": request_id,
                }

                try:
                    resp = chatBot.send_text(chatroom_id, "🤔 검색 중입니다. 잠시만 기다려주세요...")
                    _register_llm_notice(request_id, resp)
                except Exception as send_err:
                    print("[send thinking message failed]", send_err)

                # 지연이 길어질 때만 추가 진행 메시지 전송
                schedule_long_wait_notice(job, delay_sec=6.0)

                if not enqueue_llm_job(job):
                    chatBot.send_text(chatroom_id, LLM_QUEUE_FULL_MESSAGE)
                    return {"ok": True}

                return {"ok": True}
            except Exception as e:
                print(f"[LLM Dispatch Error] {e}")
                import traceback
                traceback.print_exc()
                try:
                    chatBot.send_text(chatroom_id, f"LLM 요청 처리 오류: {e}")
                except Exception:
                    pass
                return {"ok": True}

        
        # ---------- Generic Query Router ----------
        elif action in ui.ACTION_TO_QUERY:
            ui_room = route_ui_room(chatroom_id, info, sender_name)  # ✅ DM 우선

            mode, qkey = ui.ACTION_TO_QUERY[action]
            spec = ui.QUERY_REGISTRY[qkey]

            if mode == "FORM":
                chatBot.send_adaptive_card(ui_room, ui.build_query_form_card(spec))
                return {"ok": True}


            for f in spec.get("fields", []):
