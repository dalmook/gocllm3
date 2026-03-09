### PART 3/3
        ],
    }


def build_term_search_results_card(query: str, results: List[dict]) -> dict:
    body = [
        {"type":"TextBlock","text":"📚 용어검색 결과", "size":"Large", "weight":"Bolder"},
        {"type":"TextBlock","text":f"검색어: {query}", "wrap":True, "spacing":"Small", "isSubtle": True},
        {"type":"TextBlock","text":"", "separator": True, "spacing":"Medium"},
    ]

    for it in results[:10]:
        term = it.get("term","")
        subject = it.get("subject","")
        content = (it.get("content","") or "").strip()
        _raw_link = (it.get("link","") or "").strip()
        link = "" if _raw_link.lower() in ("none", "null", "nan", "-") else _raw_link

        # 너무 길면 살짝 자르기
        if len(content) > 280:
            content = content[:280] + "…"

        block = {
            "type":"Container",
            "style":"emphasis",
            "items":[
                {"type":"TextBlock","text":term, "weight":"Bolder", "wrap":True, "color":"Accent"},
                *([{"type":"TextBlock","text":f"분류: {subject}", "wrap":True, "spacing":"Small", "isSubtle": True}] if subject else []),
                *([{"type":"TextBlock","text":content, "wrap":True, "spacing":"Small"}] if content else []),
                *([{"type":"ActionSet","actions":[{"type":"Action.OpenUrl","title":"🔗 링크 열기","url": link}]}] if link else []),
            ]
        }
        body.append(block)

    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
        "actions":[
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
        ],
    }

def build_term_not_found_card(query: str) -> dict:
    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body":[
            {"type":"TextBlock","text":"📚 용어검색", "size":"Large", "weight":"Bolder"},
            {"type":"TextBlock","text":f"[ {query} ]에 대한 검색결과가 없습니다.", "wrap":True, "spacing":"Small"},
            {"type":"TextBlock","text":"(필요하면 담당자에게 반영 요청할 수 있어요)", "wrap":True, "spacing":"Small", "isSubtle":True},
            {"type":"Input.Text","id":"memo","placeholder":"추가 설명(선택) 예: 어디서 봤는지/의미 추정 등", "isMultiline":True, "maxLength":400},
        ],
        "actions":[
            {"type":"Action.Submit","title":"📩 반영 요청", "style":"positive",
             "data":{"action":"TERM_UNKNOWN_SUBMIT", "findword": query}},
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
        ],
    }

def df_to_table_card(df: pd.DataFrame, title: str = "조회 결과", max_rows: int = 10, max_cols: int = 6) -> dict:
    if df is None or df.empty:
        return {
            "type":"AdaptiveCard",
            "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
            "version":"1.3",
            "body":[
                {"type":"TextBlock","text":f"🔎 {title}","size":"Large","weight":"Bolder"},
                {"type":"TextBlock","text":"조회 결과: 0건", "wrap":True}
            ],
            "actions":[{"type":"Action.Submit","title":"홈","data":{"action":"HOME"}}]
        }

    view = df.copy()
    if view.shape[1] > max_cols:
        view = view.iloc[:, :max_cols]
    truncated_rows = view.shape[0] > max_rows
    view = view.head(max_rows)

    def cell_str(x, limit=40):
        s = "" if x is None else str(x)
        s = s.replace("\n", " ")
        return s if len(s) <= limit else (s[:limit] + "…")

    columns = list(view.columns)

    body = [
        {"type":"TextBlock","text":f"🔎 {title}", "size":"Large", "weight":"Bolder"},
        {"type":"TextBlock","text":f"Rows: {df.shape[0]} / Cols: {df.shape[1]} (표시는 {min(df.shape[0],max_rows)}행, {min(df.shape[1],max_cols)}열)", "wrap":True, "spacing":"Small"},
        {"type":"TextBlock","text":"", "separator": True}
    ]

    body.append({
        "type":"ColumnSet",
        "columns":[{"type":"Column","width":"stretch","items":[{"type":"TextBlock","text":str(col), "weight":"Bolder","wrap":True}]} for col in columns],
        "spacing":"Small"
    })

    for _, row in view.iterrows():
        body.append({
            "type":"ColumnSet",
            "columns":[{"type":"Column","width":"stretch","items":[{"type":"TextBlock","text":cell_str(row[col]), "wrap":True,"spacing":"None"}]} for col in columns],
            "spacing":"Small"
        })

    if truncated_rows:
        body.append({"type":"TextBlock","text":"※ 결과가 많아 일부만 표시했습니다.", "wrap":True, "spacing":"Small"})

    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
        "actions":[
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
            {"type":"Action.Submit","title":"🔎 조회 1","data":{"action":"QUERY_1"}},
            {"type":"Action.Submit","title":"🔎 조회 2","data":{"action":"QUERY_2"}},
        ]
    }

def build_open_url_card(title: str, url: str) -> dict:
    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body":[
            {"type":"TextBlock","text":title or "바로 열기", "size":"Large", "weight":"Bolder", "wrap":True},
            {"type":"TextBlock","text":url or "", "wrap":True, "isSubtle":True, "spacing":"Small"},
        ],
        "actions":[
            {"type":"Action.OpenUrl","title":"🚀 열기","url": url},
            {"type":"Action.Submit","title":"🏠 홈","data":{"action":"HOME"}},
        ],
    }


def build_quick_links_card(quick_links: list[tuple[list[str], str, str]]) -> dict:
    # 카테고리 분류(원하면 여기 룰만 바꾸면 됨)
    def norm(s: str) -> str:
        return (s or "").replace(" ", "").upper().strip()

    def cat(aliases: list[str], title: str, url: str) -> str:
        a = {norm(x) for x in (aliases or [])}
        t = norm(title)
        if a & {"GSCM","NSCM","O9"}: return "SCM"
        if a & {"SMDM","MDM","EDM","MSTR"}: return "마스터/시스템"
        if a & {"POWERBI","POWERBI","BI","PB","파워","파워BI"}: return "리포트/분석"
        if a & {"GITHUB","GIT","깃허브","깃헙"}: return "개발"
        if a & {"CONF","CONFLUENCE","컨플","컨플루언스","DSDN","DSASSISTANT","GPT","정보센터","정보"}: return "지식/협업"
        if "VOC" in t or any("VOC" in norm(x) for x in aliases): return "VOC/요청"
        if a & {"밥","식단","점심","아침","저녁","버스","출퇴근","통근","근태","근무시간","패밀리","패밀리넷","패넷","FAMILYNET","싱글","녹스","메일"}: return "생활"
        return "기타"

    def btn(title: str, url: str) -> dict:
        return {"type":"Action.OpenUrl","title":title,"url":url}

    def col_button(title: str | None, url: str | None) -> dict:
        if not title or not url:
            return {"type":"Column","width":"stretch","items":[]}

        # ✅ 균일 타일(박스) + 클릭(selectAction)
        return {
            "type":"Column",
            "width":"stretch",
            "items":[
                {
                    "type":"Container",
                    "style":"emphasis",
                    "minHeight":"44px",
                    "verticalContentAlignment":"Center",
                    "selectAction":{"type":"Action.OpenUrl","url":url},
                    "items":[
                        {
                            "type":"TextBlock",
                            "text": title,
                            "wrap": False,
                            "maxLines": 1,
                            "horizontalAlignment":"Center",
                            "weight":"Bolder",
                            "spacing":"None"
                        }
                    ]
                }
            ]
        }


    def row2(c1, c2) -> dict:
        return {"type":"ColumnSet","columns":[c1,c2],"spacing":"Small"}

    # 그룹핑
    buckets: dict[str, list[tuple[str,str]]] = {}
    for aliases, title, url in quick_links:
        buckets.setdefault(cat(aliases, title, url), []).append((title, url))

    order = ["SCM","마스터/시스템","리포트/분석","개발","지식/협업","VOC/요청","생활","기타"]
    body: list[dict] = [
        {"type":"TextBlock","text":"🧭 바로가기 모음", "size":"Large", "weight":"Bolder"},
        {"type":"TextBlock","text":"키워드만 쳐도 열리고, ‘바로가기’면 전체 목록이 떠요.", "wrap":True, "spacing":"Small", "isSubtle":True},
        {"type":"TextBlock","text":"", "separator": True, "spacing":"Medium"},
    ]

    for c in order:
        items = buckets.get(c) or []
        if not items:
            continue

        body.append({"type":"TextBlock","text":f"📌 {c}", "weight":"Bolder", "wrap":True, "spacing":"Medium"})

        # 2열 버튼 레이아웃
        for i in range(0, len(items), 2):
            t1,u1 = items[i]
            t2,u2 = items[i+1] if i+1 < len(items) else (None, None)
            body.append(row2(col_button(t1,u1), col_button(t2,u2)))

    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
        "actions":[
            {"type":"Action.Submit","title":"🏠 홈","data":{"action":"HOME"}},
        ],
    }
