### PART 2/3
# =========================
DASHBOARD_LOGIN_HTML = r"""
<!doctype html><html lang="ko"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>__DASHBOARD_TITLE__</title>
<style>
body{font-family:system-ui,Segoe UI,Apple SD Gothic Neo,Malgun Gothic,sans-serif;margin:0;background:#0b1020;color:#e8ecff}
.wrap{max-width:520px;margin:0 auto;padding:28px}
.card{margin-top:64px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.10);border-radius:16px;padding:18px}
.title{font-size:20px;font-weight:900;margin-bottom:8px}
.small{font-size:12px;opacity:.85}
input{width:100%;margin-top:12px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.14);color:#e8ecff;border-radius:12px;padding:12px 12px;font-size:14px}
button{margin-top:12px;width:100%;background:#4aa3ff;border:0;color:#061024;border-radius:12px;padding:12px 12px;font-weight:900;cursor:pointer}
.err{margin-top:10px;color:#ffb3b4;font-size:13px;min-height:18px}
</style></head><body>
<div class="wrap"><div class="card">
<div class="title">🔐 대시보드 접근 코드 _ 조성묵에게 문의</div>
<div class="small">코드를 입력해야 대시보드로 이동합니다.</div>
<input id="tok" type="password" placeholder="접근 코드 입력" autocomplete="off"/>
<button onclick="go()">접속</button>
<div class="err" id="err"></div>
<div class="small" style="margin-top:10px;opacity:.7;">* 코드가 틀리면 대시보드에서 401이 나며 다시 이 화면으로 돌아옵니다.</div>
</div></div>
<script>
function go(){const t=(document.getElementById("tok").value||"").trim();
if(!t){document.getElementById("err").innerText="코드를 입력해 주세요.";return;}
location.href="/dashboard?token="+encodeURIComponent(t);}
document.getElementById("tok").addEventListener("keydown",(e)=>{if(e.key==="Enter") go();});
</script></body></html>
"""

DASHBOARD_HTML = r"""
<!doctype html><html lang="ko"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>__DASHBOARD_TITLE__</title>
<style>
body{font-family:system-ui,Segoe UI,Apple SD Gothic Neo,Malgun Gothic,sans-serif;margin:0;background:#0b1020;color:#e8ecff}
header{padding:18px 20px;border-bottom:1px solid rgba(255,255,255,.08);position:sticky;top:0;background:#0b1020;z-index:10}
.wrap{max-width:1200px;margin:0 auto;padding:18px}
.kpis{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:10px}
.card{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08);border-radius:14px;padding:12px}
.label{opacity:.8;font-size:12px}
.value{font-size:22px;font-weight:800;margin-top:6px}
.value.red{color:#ff4d4f}
.value.yellow{color:#ffcc00}
.value.blue{color:#4aa3ff}
.grid{display:grid;grid-template-columns:1.2fr .8fr;gap:12px;margin-top:12px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}
.barrow{display:flex;align-items:center;gap:10px;margin:8px 0}
.bar{flex:1;height:10px;background:rgba(255,255,255,.08);border-radius:999px;overflow:hidden}
.bar>div{height:100%;background:#4aa3ff}
.small{font-size:12px;opacity:.85}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:10px;border-bottom:1px solid rgba(255,255,255,.08);vertical-align:top}
th{opacity:.85;text-align:left;font-weight:700}
a{color:#8fd3ff;text-decoration:none}
.pill{display:inline-block;padding:2px 8px;border-radius:999px;font-size:12px;border:1px solid rgba(255,255,255,.14);opacity:.9}
.pill.red{border-color:rgba(255,77,79,.6);color:#ffb3b4}
.pill.gray{opacity:.65}
.controls{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px}
input,select{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.12);color:#e8ecff;border-radius:10px;padding:8px 10px}
select option{color:#0b1020;background:#e8ecff}
button{background:#4aa3ff;border:0;color:#061024;border-radius:10px;padding:8px 12px;font-weight:800;cursor:pointer}
button.secondary{background:rgba(255,255,255,.08);color:#e8ecff}
@media (max-width:1100px){.kpis{grid-template-columns:repeat(3,minmax(0,1fr))}.grid{grid-template-columns:1fr}}
</style></head><body>
<header><div class="wrap" style="padding:0 18px;"><div style="display:flex;justify-content:space-between;align-items:center;gap:10px;">
<div><div style="font-size:18px;font-weight:900;">__DASHBOARD_TITLE__</div><div class="small" id="sub"></div></div>
<div class="small" id="health"></div></div></div></header>

<div class="wrap">
<div class="card" style="margin-bottom:12px;"><div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">
<div style="font-weight:900;">🏷️ 공지방(대화방) 선택</div><select id="f_room" onchange="onRoomChange()"></select>
<div class="small" id="roominfo"></div></div></div>

<div class="kpis">
<div class="card"><div class="label">🔥 Red Alert (Overdue + D-3)</div><div class="value red" id="k_red">-</div></div>
<div class="card"><div class="label">⏰ Overdue</div><div class="value red" id="k_overdue">-</div></div>
<div class="card"><div class="label">📅 D-7 이내</div><div class="value yellow" id="k_due7">-</div></div>
<div class="card"><div class="label">📌 OPEN 총계</div><div class="value blue" id="k_open">-</div></div>
<div class="card"><div class="label">🧊 14일+ 장기</div><div class="value" id="k_long">-</div></div>
<div class="card"><div class="label">🕳 목표일 미입력</div><div class="value" id="k_notarget">-</div></div>
</div>

<div class="grid">
<div class="card">
<div style="display:flex;justify-content:space-between;align-items:center;">
<div style="font-weight:900;">🚀 최근 8주 신규/완료 추이</div>
<div class="small">평균 처리기간(60d Closed): <b id="k_cycle">-</b>일</div></div>
<div class="small" style="margin-top:8px;opacity:.9;">(간단 막대 형태로 표시)</div><div id="trend"></div></div>

<div class="card"><div style="font-weight:900;">👤 담당자별 OPEN 상위</div><div id="owners"></div></div>
</div>

<div class="grid2">
<div class="card"><div style="font-weight:900;">🔥 가장 급한 TOP 10 (D-day)</div><div id="urgent"></div></div>
<div class="card"><div style="font-weight:900;">🧊 가장 오래된 TOP 10 (Aging)</div><div id="old"></div></div>
</div>

<div class="card" style="margin-top:12px;">
<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;">
<div style="font-weight:900;">📋 이슈 목록</div><div class="small">필터 후 새로고침</div></div>
<div class="controls">
<select id="f_status"><option value="OPEN">OPEN</option><option value="CLOSED">CLOSED</option><option value="ALL">ALL</option></select>
<input id="f_owner" placeholder="담당자(owner) 검색"/>
<input id="f_q" placeholder="제목/내용 검색" style="min-width:260px;"/>
<button onclick="loadIssues(0)">조회</button>
<button class="secondary" onclick="loadIssues(curPage-1)">이전</button>
<button class="secondary" onclick="loadIssues(curPage+1)">다음</button>
<span class="small" id="pageinfo"></span>
</div>

<div style="overflow:auto;margin-top:10px;">
<table><thead><tr>
<th style="width:70px;">ID</th><th>제목</th><th style="width:120px;">담당</th><th style="width:110px;">목표일</th>
<th style="width:70px;">D-day</th><th style="width:70px;">Aging</th><th style="width:70px;">링크</th>
</tr></thead><tbody id="tbody"></tbody></table></div></div></div>

<script>
const TOKEN="__TOKEN__";let curPage=0;
function esc(s){s=(s??"").toString();return s.replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;").replaceAll("'","&#39;");}
async function loadRooms(){
  const url=`/api/dashboard/rooms?token=${encodeURIComponent(TOKEN)}`;const r=await fetch(url);if(!r.ok) return;
  const data=await r.json();const rooms=data.rooms||[];
  const sel=document.getElementById("f_room");
  sel.innerHTML=`<option value="">ALL(전체)</option>`+rooms.map(x=>`<option value="${esc(x.room_id)}">${esc(x.title)}</option>`).join("");
  document.getElementById("roominfo").innerText=`총 ${rooms.length}개 방`;
}
function onRoomChange(){loadSummary();loadIssues(0);}
function pillD(d){
  if(d===null||d===undefined) return `<span class="pill gray">-</span>`;
  if(d<0) return `<span class="pill red">D${d}</span>`;
  if(d<=3) return `<span class="pill red">D-${d}</span>`;
  if(d<=7) return `<span class="pill">D-${d}</span>`;
  return `<span class="pill gray">D-${d}</span>`;
}
function barRow(name,val,max){
  const w=max?Math.round((val/max)*100):0;
  return `<div class="barrow"><div style="width:130px;font-weight:700;">${esc(name)}</div>
    <div class="bar"><div style="width:${w}%"></div></div>
    <div style="width:44px;text-align:right;font-weight:800;">${val}</div></div>`;
}
async function loadSummary(){
  const room_id=(document.getElementById("f_room")?.value||"").trim();
  const url=`/api/dashboard/summary?token=${encodeURIComponent(TOKEN)}${room_id?`&room_id=${encodeURIComponent(room_id)}`:""}`;
  const r=await fetch(url);
  if(!r.ok){document.getElementById("health").innerText="접근 불가(토큰 확인)";return;}
  const data=await r.json();const k=data.kpi;
  document.getElementById("sub").innerText=`기준일: ${k.today}${room_id?` · room_id: ${room_id}`:""}`;
  document.getElementById("k_red").innerText=k.red_alert;
  document.getElementById("k_overdue").innerText=k.overdue;
  document.getElementById("k_due7").innerText=k.due_7;
  document.getElementById("k_open").innerText=k.open_total;
  document.getElementById("k_long").innerText=k.long_open_14;
  document.getElementById("k_notarget").innerText=k.no_target;
  document.getElementById("k_cycle").innerText=(k.avg_cycle_days_60d ?? "-");

  const labels=data.series.labels;const created=data.series.created;const closed=data.series.closed;
  const maxv=Math.max(...created,...closed,1);
  let html=`<div class="small">주차</div>`;
  for(let i=0;i<labels.length;i++){
    html+=`<div class="barrow"><div style="width:90px;" class="small">${labels[i]}</div>
      <div class="bar"><div style="width:${Math.round(created[i]/maxv*100)}%"></div></div>
      <div style="width:48px;" class="small">신규 ${created[i]}</div>
      <div class="bar"><div style="width:${Math.round(closed[i]/maxv*100)}%;background:#ffcc00"></div></div>
      <div style="width:48px;" class="small">완료 ${closed[i]}</div></div>`;
  }
  document.getElementById("trend").innerHTML=html;

  const owners=data.owner_top||[];const omax=Math.max(...owners.map(x=>x.open_cnt),1);
  document.getElementById("owners").innerHTML=owners.map(x=>barRow(x.owner,x.open_cnt,omax)).join("")||`<div class="small">데이터 없음</div>`;

  const urg=data.urgent_top10||[];
  document.getElementById("urgent").innerHTML=
    `<table><thead><tr><th>ID</th><th>제목</th><th>D</th><th>담당</th><th>링크</th></tr></thead><tbody>`+
    urg.map(x=>`<tr><td>#${x.issue_id}</td><td>${esc(x.title)}</td><td>${pillD(x.d_day)}</td><td>${esc(x.owner||"")}</td><td>${x.url?`<a href="${esc(x.url)}" target="_blank">열기</a>`:"-"}</td></tr>`).join("")+
    `</tbody></table>`;

  const old=data.old_top10||[];
  document.getElementById("old").innerHTML=
    `<table><thead><tr><th>ID</th><th>제목</th><th>Aging</th><th>담당</th><th>링크</th></tr></thead><tbody>`+
    old.map(x=>`<tr><td>#${x.issue_id}</td><td>${esc(x.title)}</td><td><span class="pill">${x.age_days}d</span></td><td>${esc(x.owner||"")}</td><td>${x.url?`<a href="${esc(x.url)}" target="_blank">열기</a>`:"-"}</td></tr>`).join("")+
    `</tbody></table>`;

  document.getElementById("health").innerText="● LIVE";
}
async function loadIssues(page){
  if(page<0) page=0;
  const status=document.getElementById("f_status").value;
  const owner=document.getElementById("f_owner").value.trim();
  const q=document.getElementById("f_q").value.trim();
  const room_id=(document.getElementById("f_room")?.value||"").trim();
  const size=50;
  const url=`/api/dashboard/issues?token=${encodeURIComponent(TOKEN)}&status=${encodeURIComponent(status)}&owner=${encodeURIComponent(owner)}&q=${encodeURIComponent(q)}&page=${page}&size=${size}`+(room_id?`&room_id=${encodeURIComponent(room_id)}`:"");
  const r=await fetch(url);if(!r.ok) return;
  const data=await r.json();curPage=data.page;
  document.getElementById("pageinfo").innerText=`page ${data.page+1} / total ${data.total}`;
  const rows=data.items||[];
  document.getElementById("tbody").innerHTML=rows.map(x=>`
    <tr><td>#${x.issue_id}</td>
    <td>${esc(x.title)}<div class="small">${esc(x.content||"")}</div></td>
    <td>${esc(x.owner||"")}</td><td>${esc(x.target_date||"")}</td>
    <td>${pillD(x.d_day)}</td><td><span class="pill gray">${x.age_days}d</span></td>
    <td>${x.url?`<a href="${esc(x.url)}" target="_blank">열기</a>`:"-"}</td></tr>`).join("");
}
(async()=>{await loadRooms();loadSummary();loadIssues(0);setInterval(loadSummary,30000);})();
</script></body></html>
"""

# =========================
# Adaptive Cards
# =========================
def build_issue_deadline_reminder_card(items: List[tuple], today_str: str):
    items = sorted(items, key=lambda x: (x[0], int(x[1]["issue_id"])))
    body = [
        {"type":"TextBlock","text":"🚨 목표일 임박 이슈 알림", "size":"Large", "weight":"Bolder", "color":"Attention", "wrap":True},
        {"type":"TextBlock","text":f"기준: {today_str} 09:00", "wrap":True, "spacing":"Small", "isSubtle": True},
        {"type":"TextBlock","text":"", "separator": True, "spacing":"Medium"},
    ]
    for d, it in items[:10]:
        title = it.get("title","")
        owner = it.get("owner","")
        td    = it.get("target_date","")
        content = it.get("content","")
        url   = (it.get("url") or "").strip()

        body.append({
            "type":"Container",
            "style":"emphasis",
            "items":[
                {"type":"ColumnSet","columns":[
                    {"type":"Column","width":"stretch","items":[{"type":"TextBlock","text":f"#{it['issue_id']} {title}","weight":"Bolder","wrap":True}]},
                    {"type":"Column","width":"auto","items":[{"type":"TextBlock","text":f"D-{d}","weight":"Bolder","color":"Attention","wrap":True,"horizontalAlignment":"Right"}]}
                ]},
                {"type":"TextBlock","text":f"목표일: {td} / 담당: {owner}", "wrap":True, "spacing":"Small"},
                *([{"type":"TextBlock","text":f"내용: {content}", "wrap":True, "spacing":"Small"}] if content else []),
                *([
                    {"type":"TextBlock","text":f"LINK: {url}", "wrap":True, "spacing":"Small", "isSubtle": True},
                    {"type":"ActionSet","actions":[{"type":"Action.OpenUrl","title":"🔗 링크 열기","url": url}], "spacing":"Small"}
                ] if url else []),
            ]
        })
    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
        "actions":[
            {"type":"Action.Submit","title":"📋 현재 이슈", "data":{"action":"ISSUE_LIST"}},
            {"type":"Action.Submit","title":"🕓 이슈 이력", "data":{"action":"ISSUE_HISTORY", "page": 0}},
            {"type":"Action.Submit","title":"홈", "data":{"action":"HOME"}},
        ]
    }

def build_home_card(dashboard_url: str, infocenter_url: str):
    def btn(title: str, action: str | None = None, style: str | None = None, url: str | None = None) -> dict:
        if url:
            return {"type":"Action.OpenUrl","title":title,"url":url}
        a = {"type":"Action.Submit","title":title,"data":{"action":action}}
        if style:
            a["style"] = style
        return a

    def col_button(title: str | None, action: str | None = None, style: str | None = None, url: str | None = None) -> dict:
        if not title:
            return {"type":"Column","width":"stretch","items":[]}
        if url:
            return {"type":"Column","width":"stretch","items":[{"type":"ActionSet","actions":[btn(title,None,style,url=url)]}]}
        if not action:
            return {"type":"Column","width":"stretch","items":[]}
        return {"type":"Column","width":"110px","items":[{"type":"ActionSet","actions":[btn(title,action,style)]}]}

    def row2(c1,c2)->dict: return {"type":"ColumnSet","columns":[c1,c2],"spacing":"Small"}
    def section(title: str)->dict: return {"type":"TextBlock","text":title,"weight":"Bolder","wrap":True,"spacing":"Medium"}
    def divider()->dict: return {"type":"TextBlock","text":"","separator":True,"spacing":"Medium"}

    def _rows2_from_buttons(btns):
        rows=[]
        for i in range(0,len(btns),2):
            chunk=btns[i:i+2]
            t,a,s,u = chunk[0] if len(chunk)>=1 else (None,None,None,None)
            c1=col_button(t,a,style=s,url=u)
            t,a,s,u = chunk[1] if len(chunk)>=2 else (None,None,None,None)
            c2=col_button(t,a,style=s,url=u)
            rows.append(row2(c1,c2))
        return rows

    ISSUE_BUTTONS = [
        ("📝 이슈발의","ISSUE_FORM","positive",None),
        ("📋 현재이슈","ISSUE_LIST",None,None),
        ("🕓 이슈이력","ISSUE_HISTORY",None,None),
        ("🔗 대시보드",None,None,dashboard_url),
    ]
    HOME_QUERY_BUTTONS = [
        ("🔎 코드조회","CODE_FINDER_FORM",None,None),
        ("🔎 PKG코드","PKGCODE_FORM",None,None), 
        ("🔎 담당조회","RIGHTPERSON_FORM",None,None),        
        ("🧩 PS  조회","PS_FORM",None,None),        
        ("📚 용어검색","TERM_FORM",None,None),
        ("📈 입고계획","IPGO_FORM",None,None),
        # ("🔗 정보센터",None,None,infocenter_url),
        ("🚚 출하조회","SHIP_FORM",None,None),  
        
    ]
    WATCHROOM_BUTTONS = [
        ("📣 이슈방+","WATCHROOM_FORM",None,None),
        ("🧭 바로가기","QUICK_LINKS",None,None),
    ]

    body = [
        {"type": "TextBlock","text": "🤖 공급망 챗봇 ","size": "Large","weight": "Bolder"},
        {"type": "TextBlock","text": "Built by 조성묵","size": "Small","isSubtle": True,"spacing": "None"},
        {"type":"TextBlock","text":"[@공급망 챗봇]으로 호출 하세요.","wrap":True,"spacing":"Small"},
        divider(), section("이슈"), *_rows2_from_buttons(ISSUE_BUTTONS),
        divider(), section("조회"), *_rows2_from_buttons(HOME_QUERY_BUTTONS),
        divider(), section("공지방"), *_rows2_from_buttons(WATCHROOM_BUTTONS),
    ]

    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
    }

def build_quicklink_card(title: str, url: str) -> dict:
    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body":[
            {"type":"TextBlock","text":title or "🔗 바로가기", "size":"Large", "weight":"Bolder"},
            {"type":"TextBlock","text":url, "wrap":True, "isSubtle":True, "spacing":"Small"},
        ],
        "actions":[
            {"type":"Action.OpenUrl","title":"🌐 열기", "url": url},
            {"type":"Action.Submit","title":"홈", "data":{"action":"HOME"}},
        ],
    }


def build_issue_form_card(sender_hint: str = "", room_id: str = ""):
    today = datetime.now().strftime("%Y-%m-%d")
    target_default = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body":[
            {"type":"TextBlock","text":"📝 이슈 발의","size":"Large","weight":"Bolder"},
            {"type":"TextBlock","text":"(이 화면은 개인메신저에서 작성되며, 등록은 원래 대화방에 반영됩니다.)", "wrap":True, "spacing":"Small", "isSubtle":True},
            {"type":"Input.Text","id":"title","placeholder":"제목", "maxLength":60},
            {"type":"Input.Text","id":"content","placeholder":"내용", "isMultiline":True, "maxLength":2000},
            {"type":"Input.Text","id":"url","placeholder":"LINK(선택) 예: https://... 또는 gpmtsp.samsungds.net", "maxLength":200},
            {"type":"Input.Date","id":"occur_date","title":"발생일", "value":today},
            {"type":"Input.Date","id":"target_date","title":"목표일", "value":target_default},
            {"type":"Input.Text","id":"owner","placeholder":"담당자(SSO or 이름)", "value":sender_hint},
        ],
        "actions":[
            {"type":"Action.Submit","title":"등록","style":"positive","data":{"action":"ISSUE_CREATE", "room_id": room_id}},
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
        ],
    }

def build_issue_list_card(issues: List[dict], room_id: str = ""):
    body = [{"type":"TextBlock","text":"📋 현재 OPEN 이슈","size":"Large","weight":"Bolder"}]
    if not issues:
        body.append({"type":"TextBlock","text":"현재 등록된 OPEN 이슈가 없습니다."})
    else:
        # ✅ UI에서도 한번 더 정렬(안전빵): d_day 오름차순(오버듀/임박 우선), None은 맨 뒤
        items = []
        for it in issues:
            dd = store._dday(it.get("target_date", ""))
            it["_dday"] = dd
            items.append(it)

        items.sort(key=lambda x: (999999 if x.get("_dday") is None else x.get("_dday"), int(x.get("issue_id", 0))))

        for it in issues[:30]:
            # ✅ store.py에서 계산된 d_day 우선 사용 (없으면 fallback)
            dd = it.get("d_day", None)
            if dd is None:
                dd = store._dday(it.get("target_date", ""))

            # ✅ [D-X] 태그 + 색(주황/빨강) + 지날수록 강도 이모지
            tag_text = ""
            tag_color = "Default"
            if dd is not None:
                if dd < 0:
                    mark = "🔥" if dd <= -3 else "🚨"
                    tag_text = f"{mark}[D+{abs(dd)}]"
                    tag_color = "Attention"
                elif dd == 0:
                    tag_text = "🚨[D-0]"
                    tag_color = "Attention"
                else:
                    tag_text = f"[D-{dd}]"
                    tag_color = "Warning" if dd <= 3 else "Default"

            url = (it.get("url") or "").strip()

            body.append({
                "type":"Container",
                "style":"emphasis",
                "items":[
                    # ✅ 태그(색) + 제목(고정 색) 분리
                    {"type":"ColumnSet","columns":[
                        {"type":"Column","width":"auto","items":[
                            {"type":"TextBlock","text":tag_text,"weight":"Bolder","wrap":True,
                             "color":tag_color,"isVisible": True if tag_text else False}
                        ]},
                        {"type":"Column","width":"stretch","items":[
                            {"type":"TextBlock","text":f"#{it['issue_id']} {it['title']}",
                             "weight":"Bolder","wrap":True,"color":"Accent"}
                        ]},
                    ]},
                    {"type":"TextBlock","text":f"- 내용: {it.get('content','')}", "wrap":True, "spacing":"Small"},                                        
                    {"type":"TextBlock","text":f"📅 발생일: {it.get('occur_date','-')} · 목표일: {it.get('target_date','-')}", "wrap":True, "spacing":"None", "isSubtle": True},
                    {"type":"TextBlock","text":f"👤 담당: {it.get('owner','-')}", "wrap":True, "spacing":"Small", "weight":"Bolder", "color":"Dark"},


                    *([
                        {"type":"TextBlock","text":f"- LINK: {url}","wrap":True,"spacing":"Small"},
                        {"type":"ActionSet","actions":[{"type":"Action.OpenUrl","title":"🔗 링크 열기","url": url}],"spacing":"Small"}
                    ] if url else []),
                    {"type":"ActionSet","actions":[
                        {"type":"Action.Submit","title":"✅ 완료","style":"positive","data":{"action":"ISSUE_CLEAR","issue_id":it["issue_id"], "room_id": room_id}},
                        {"type":"Action.Submit","title":"✏️ 수정","data":{"action":"ISSUE_EDIT_FORM","issue_id":it["issue_id"], "room_id": room_id}},
                    ]}
                ]
            })


    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
        "actions":[
            {"type":"Action.Submit","title":"📝 이슈 발의","data":{"action":"ISSUE_FORM", "room_id": room_id}},
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
        ],
    }

def build_issue_edit_form_card(issue: dict, room_id: str = ""):
    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body":[
            {"type":"TextBlock","text":f"✏️ 이슈 수정 #{issue.get('issue_id')}", "size":"Large", "weight":"Bolder"},
            {"type":"Input.Text","id":"title","placeholder":"제목(짧게)", "maxLength":60, "value":issue.get("title","")},
            {"type":"Input.Text","id":"content","placeholder":"내용(초간단 한줄)", "isMultiline":True, "maxLength":200, "value":issue.get("content","") or ""},
            {"type":"Input.Text","id":"url","placeholder":"LINK(선택)", "maxLength":200, "value":issue.get("url","") or ""},
            {"type":"Input.Date","id":"occur_date","title":"발생일", "value": (issue.get("occur_date") or "")},
            {"type":"Input.Date","id":"target_date","title":"목표일", "value": (issue.get("target_date") or "")},
            {"type":"Input.Text","id":"owner","placeholder":"담당자(SSO or 이름)", "value":issue.get("owner","") or ""},
        ],
        "actions":[
            {"type":"Action.Submit","title":"저장","style":"positive","data":{"action":"ISSUE_UPDATE","issue_id":issue.get("issue_id"), "room_id": room_id}},
            {"type":"Action.Submit","title":"현재 이슈","data":{"action":"ISSUE_LIST", "room_id": room_id}},
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
        ],
    }


def build_issue_summary_card(issues: List[dict], today_str: str, max_items: int = 15) -> dict:
    """
    매일 아침 OPEN 이슈 요약용 AdaptiveCard 1장
    issues: store.issue_list_open(...) 결과 + (선택) it["d_day"] 가 들어있으면 사용
    """

    def dlabel(d: Optional[int]) -> str:
        if d is None:
            return ""
        if d > 0:
            return f"D-{d}"
        if d == 0:
            return "🚨 D-DAY"
        return f"D+{abs(d)}"   # overdue

    body = [
        {"type":"TextBlock", "text":"📌 [OPEN 이슈 요약]", "size":"Medium", "weight":"Bolder"},
        {"type":"TextBlock", "text":f"기준: {today_str} · 총 {len(issues)}건", "wrap":True, "spacing":"Small", "isSubtle":True},
        {"type":"TextBlock", "text":"", "separator": True, "spacing":"Medium"},
    ]

    if not issues:
        body.append({"type":"TextBlock", "text":"OPEN 이슈가 없습니다 ✅", "wrap":True})
    else:
        for it in issues[:max_items]:
            _id = it.get("issue_id", "")
            title = (it.get("title") or "").strip()
            owner = (it.get("owner") or "").strip()
            target_date = (it.get("target_date") or "").strip()

            dd = it.get("d_day", None)
            if dd is None:
                dd = store._dday(it.get("target_date", ""))

            tag_text = ""
            tag_color = "Default"
            if dd is not None:
                if dd < 0:
                    mark = "🔥" if dd <= -3 else "🚨"
                    tag_text = f"{mark}[D+{abs(dd)}]"
                    tag_color = "Attention"
                elif dd == 0:
                    tag_text = "🚨[D-0]"
                    tag_color = "Attention"
                else:
                    tag_text = f"[D-{dd}]"
                    tag_color = "Warning" if dd <= 3 else "Default"

            body.append({
                "type":"Container",
                "spacing":"Small",
                "items":[
                    {"type":"ColumnSet","columns":[
                        {"type":"Column","width":"auto","items":[
                            {"type":"TextBlock","text":tag_text,"wrap":True,"weight":"Bolder",
                             "color":tag_color,"isVisible": True if tag_text else False}
                        ]},
                        {"type":"Column","width":"stretch","items":[
                            {"type":"TextBlock","text":f"#{_id} {title}",
                             "wrap":True, "color":"Accent", "weight":"Bolder"}
                        ]},
                    ]},
                    {"type":"TextBlock",
                     "text":f"담당: {owner or '-'} · 목표: {target_date or '-'}",
                     "wrap":True, "spacing":"None", "isSubtle":True}
                ]
            })


        if len(issues) > max_items:
            body.append({"type":"TextBlock", "text":f"…외 {len(issues)-max_items}건", "wrap":True, "spacing":"Small", "isSubtle":True})

    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
        "actions":[
            {"type":"Action.Submit","title":"📋 현재 이슈","data":{"action":"ISSUE_LIST"}},
            {"type":"Action.Submit","title":"🏠 홈","data":{"action":"HOME"}},
        ],
    }


def build_issue_history_card(issues: List[dict], page: int = 0, total: int = 0, page_size: int = store.HISTORY_PAGE_SIZE, room_id: str = ""):
    total_pages = max(1, (int(total) + int(page_size) - 1) // int(page_size))
    page = max(0, min(int(page), total_pages - 1))

    def nav_col(title: str, p: int, enabled: bool):
        if not enabled:
            return {"type":"Column","width":"stretch","items":[]}
        return {"type":"Column","width":"stretch","items":[{"type":"ActionSet","actions":[
            {"type":"Action.Submit","title":title,"data":{"action":"ISSUE_HISTORY","page":p, "room_id": room_id}}
        ]}]} 

    body = [
        {"type":"TextBlock","text":"🕓 이슈 이력(OPEN/CLOSED)","size":"Large","weight":"Bolder"},
        {"type":"TextBlock","text":f"페이지 {page+1}/{total_pages} · 총 {total}건 · {page_size}개씩", "wrap":True, "spacing":"Small"},
        {"type":"ColumnSet","spacing":"Medium","columns":[
            nav_col("⬅️ 이전", page-1, enabled=(page > 0)),
            nav_col("➡️ 다음", page+1, enabled=(page < total_pages-1)),
        ]},
        {"type":"TextBlock","text":"", "separator": True, "spacing":"Medium"},
    ]

    if not issues:
        body.append({"type":"TextBlock","text":"이슈 이력이 없습니다."})
    else:
        for it in issues[:page_size]:
            status = (it.get("status") or "").upper()
            is_closed = (status == "CLOSED")

            dlabel = ""
            td = store._parse_ymd(it.get("target_date", ""))
            if td:
                d = (td - datetime.now().date()).days
                if d > 0: dlabel = f"D-{d}"
                elif d == 0: dlabel = "🚨 D-DAY"
                else: dlabel = f"D+{abs(d)}"

            actions = []
            if is_closed:
                actions.append({"type":"Action.Submit","title":"🗑️ 삭제","style":"destructive",
                                "data":{"action":"ISSUE_DELETE","issue_id":it["issue_id"], "page": page, "room_id": room_id}})

            url = (it.get("url") or "").strip()

            body.append({
                "type":"Container","style":"emphasis",
                "items":[
                    {"type":"ColumnSet","columns":[
                        {"type":"Column","width":"stretch","items":[{"type":"TextBlock","text":f"[{status}] #{it['issue_id']} {it['title']}",
                            "weight":"Bolder","wrap":True,"color":("Accent" if not is_closed else "Default"),"isSubtle":is_closed}]},
                        {"type":"Column","width":"auto","items":[{"type":"TextBlock","text":dlabel,"weight":"Bolder","wrap":True,
                            "color":"Attention","horizontalAlignment":"Right","isVisible": True if dlabel else False,"isSubtle":is_closed}]},
                    ]},
                    {"type":"TextBlock","text":f"- 내용: {it.get('content','')}", "wrap":True, "spacing":"Small", "isSubtle": is_closed},
                    {"type":"TextBlock","text":f"- 발생일: {it.get('occur_date','')} / 목표일: {it.get('target_date','')} / 담당: {it.get('owner','')}",
                     "wrap":True, "spacing":"Small", "isSubtle": is_closed},
                    *([
                        {"type":"TextBlock","text":f"- LINK: {url}", "wrap":True, "spacing":"Small", "isSubtle": is_closed},
                        {"type":"ActionSet","actions":[{"type":"Action.OpenUrl","title":"🔗 링크 열기","url": url}],"spacing":"Small"}
                    ] if url else []),
                    *([{"type":"ActionSet","actions": actions, "spacing":"Small"}] if actions else [])
                ]
            })

    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
        "actions":[
            {"type":"Action.Submit","title":"📋 현재 이슈","data":{"action":"ISSUE_LIST", "room_id": room_id}},
            {"type":"Action.Submit","title":"📝 이슈 발의","data":{"action":"ISSUE_FORM", "room_id": room_id}},
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
        ],
    }

def build_watchroom_form_card():
    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body":[
            {"type":"TextBlock","text":"📣 공지방 생성(푸시 대상)","size":"Large","weight":"Bolder"},
            {"type":"TextBlock","text":"봇이 직접 만든 방만 스케줄 워닝/이슈요약 푸시가 가능합니다.", "wrap":True},
            {"type":"Input.Text","id":"room_title","placeholder":"대화방 이름(선택) 예: 운영 워닝/이슈 공지방", "maxLength":128},
            {"type":"Input.Text","id":"members","placeholder":"참여자 SSO(콤마구분) 예: sungmook.cho,cc.choi", "isMultiline":True},
            {"type":"Input.Text","id":"note","placeholder":"방 설명(선택, DB에만 저장됨) 예: 운영 워닝/이슈방", "maxLength":80},
        ],
        "actions":[
            {"type":"Action.Submit","title":"공지방 생성 & 등록","style":"positive","data":{"action":"WATCHROOM_CREATE"}},
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
        ],
    }

def build_query_form_card(spec: Dict[str, Any]) -> dict:
    if spec.get("form_type") == "ONEVIEW_SHIP":
        return build_oneview_ship_form_card(spec)
    if spec.get("form_type") == "PKGCODE":
        return build_pkgcode_form_card(spec)    
    if spec.get("form_type") == "PS":
        return build_ps_form_card(spec)    
        
    body = [
        {"type":"TextBlock","text":f"🔎 {spec.get('title','조회')}", "size":"Large", "weight":"Bolder"},
        {"type":"TextBlock","text":"필수값을 입력 후 조회를 누르세요.", "wrap":True, "spacing":"Small"},
    ]

    for f in spec.get("fields", []):
        label = f.get("label", f.get("id"))
        ph = f.get("placeholder", "")
        required = f.get("required", False)
        body.append({"type":"TextBlock","text":f"{label}{' *' if required else ''}", "wrap":True, "spacing":"Small"})
        body.append({"type":"Input.Text", "id": f["id"], "placeholder": ph, "maxLength": 80})

    return {
        "type":"AdaptiveCard",
        "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
        "version":"1.3",
        "body": body,
        "actions":[
            *([{"type":"Action.OpenUrl","title":"🛠 수정","url": spec["edit_url"]}] if spec.get("edit_url") else []),
            {"type":"Action.Submit","title":"조회", "style":"positive", "data":{"action": spec["run_action"]}},
            {"type":"Action.Submit","title":"홈", "data":{"action":"HOME"}},
        ],
    }

# (추가 코드 - 추가용)  ※ build_query_form_card 아래에 붙이면 됨
def build_oneview_ship_form_card(spec: Dict[str, Any]) -> dict:
    ym = datetime.now().strftime("%Y%m")
    return {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.3",
        "body": [
            {"type":"Container","items":[
                {"type":"TextBlock","text":"🚚 출하이력 조회","size":"Large","weight":"Bolder"},
                {"type":"TextBlock","text":"NERP 원뷰 출하 조회 입니다.","spacing":"Small","weight":"Bolder","wrap":True},
            ]},
            {"type":"Container","spacing":"Padding","items":[
                {"type":"TextBlock","text":"※ 조회버튼은 한번만!","weight":"Bolder","wrap":True},
                {"type":"Container","style":"emphasis","spacing":"Padding","items":[
                    {"type":"ColumnSet","spacing":"Padding","columns":[
                        {"type":"Column","width":"72px","items":[{"type":"TextBlock","text":"설명","color":"Dark"}]},
                        {"type":"Column","width":"stretch","items":[
                            {"type":"TextBlock","text":"NERP 원뷰 테이블(출하) 기준 출하 조회 입니다.\n\nDO, ITEM, VERSION, 거래선으로 조회 가능합니다.","wrap":True}
                        ]},
                    ]}
                ]},

                {"type":"TextBlock","text":"조회월(YYYYMM)","weight":"Bolder","spacing":"Medium"},
                {"type":"ColumnSet","columns":[
                    {"type":"Column","width":"stretch","items":[{"type":"Input.Text","id":"hastartmon01","value": ym}]},
                    {"type":"Column","width":"stretch","items":[{"type":"Input.Text","id":"haendmon01","value": ym}]},
                ]},

                {"type":"TextBlock","text":"조회기준","weight":"Bolder","spacing":"Medium"},
                {"type":"Input.ChoiceSet",
                    "id":"hachoiceset01",
                    "style":"expanded",
                    "value":"deliverynum01",
                    "choices":[
                        {"title":"DO No.","value":"deliverynum01"},
                        {"title":"ITEM","value":"haitem01"},
                        {"title":"VERSION","value":"haversion01"},
                        {"title":"거래선","value":"hagc01"},
                    ]
                },

                {"type":"TextBlock","text":"검색값","weight":"Bolder","spacing":"Medium"},
                {"type":"Input.Text","id":"hainputtext01","placeholder":"ex) DO no. / ITEM / VERSION / 거래선", "maxLength": 80},
            ]},
        ],
        "actions":[
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
            {"type":"Action.Submit","title":"조회","style":"positive","data":{"action": spec["run_action"]}},
        ],
    }

# (추가 코드 - 추가용)  ※ build_query_form_card 아래쪽에 추가
def build_pkgcode_form_card(spec: Dict[str, Any]) -> dict:
    return {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.3",
        "body": [
            {"type":"Container","items":[
                {"type":"TextBlock","text":"📦 PKGCODE 조회","size":"Large","weight":"Bolder"},
                {"type":"TextBlock","text":"VER, PKG, PCBCODE를 입력해 주세요", "wrap":True, "spacing":"Small", "weight":"Bolder"},
                {"type":"TextBlock","text":"※ 조회버튼은 한번만!", "wrap":True, "spacing":"Small", "weight":"Bolder"},
                {"type":"Container","style":"emphasis","spacing":"Padding","items":[
                    {"type":"ColumnSet","spacing":"Padding","columns":[
                        {"type":"Column","width":"72px","items":[{"type":"TextBlock","text":"설명","color":"Dark"}]},
                        {"type":"Column","width":"stretch","items":[
                            {"type":"TextBlock","text":"PCB정보를 조회하는 용도이며,\n\nVER, PKG, PCBCODE로 조회 가능하고,\n\nPCB 수량 정보도 포함합니다.", "wrap":True}
                        ]},
                    ]}
                ]},
            ]},
            {"type":"Container","spacing":"Padding","items":[
                {"type":"Input.Text","id":"pkgcode3341","placeholder":"ex) WL / 9N7 / LA41-12223A", "maxLength": 80},
            ]},
        ],
        "actions":[
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
            {"type":"Action.Submit","title":"조회","style":"positive","data":{"action": spec["run_action"]}},
        ],
    }

def build_ps_form_card(spec: Dict[str, Any]) -> dict:
    return {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.3",
        "body": [
            {"type":"Container","items":[
                {"type":"TextBlock","text":"🧩 PS 조회","size":"Large","weight":"Bolder"},
                {"type":"TextBlock","text":"코드구분/조회기준 선택 후 코드 입력해서 조회하세요.", "wrap":True, "spacing":"Small", "weight":"Bolder"},
                {"type":"TextBlock","text":"※ 조회버튼은 한번만!", "wrap":True, "spacing":"Small", "weight":"Bolder"},
            ]},

            {"type":"Container","spacing":"Padding","items":[
                {"type":"TextBlock","text":"코드구분", "weight":"Bolder", "spacing":"Small"},
                {"type":"Input.ChoiceSet",
                    "id":"psgubun01",
                    "style":"expanded",
                    "value":"pscomp01",
                    "choices":[
                        {"title":"COMP",   "value":"pscomp01"},
                        {"title":"MODULE", "value":"psmodule01"},
                        {"title":"MCP",    "value":"psmultichip01"},
                    ]
                },

                {"type":"TextBlock","text":"조회기준", "weight":"Bolder", "spacing":"Medium"},
                {"type":"Input.ChoiceSet",
                    "id":"psconv01",
                    "value":"pseds03",
                    "choices":[
                        {"title":"FAB_CODE", "value":"psfab02"},
                        {"title":"EDS_CODE", "value":"pseds03"},
                        {"title":"ASY_CODE", "value":"psasy04"},
                        {"title":"TST_CODE", "value":"pstst05"},
                        {"title":"MOD_CODE", "value":"psmod06"},
                    ]
                },

                {"type":"TextBlock","text":"코드입력", "weight":"Bolder", "spacing":"Medium"},
                {"type":"Input.Text","id":"psver01","placeholder":"ex) K3KL4.. / WL / 9N7 ...", "maxLength": 80},
            ]},
        ],
        "actions":[
            {"type":"Action.Submit","title":"홈","data":{"action":"HOME"}},
            {"type":"Action.Submit","title":"조회","style":"positive","data":{"action": spec["run_action"]}},
