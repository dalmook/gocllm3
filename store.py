# store.py
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional

HISTORY_PAGE_SIZE = 5
REMIND_DAYS = {7, 3, 0}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.getenv("DB_PATH", os.path.join(BASE_DIR, "chatbot.db"))

def db_connect():
    return sqlite3.connect(DB_PATH)

def _has_column(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    return col in cols

def _ensure_column(cur: sqlite3.Cursor, table: str, col: str, col_def: str):
    if not _has_column(cur, table, col):
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")

def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # ui_state
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ui_state (
        chatroom_id          TEXT PRIMARY KEY,
        history_msg_id       INTEGER,
        history_sent_time    INTEGER,
        issue_list_msg_id    INTEGER,
        issue_list_sent_time INTEGER,
        updated_at           TEXT
    )
    """)
    _ensure_column(cur, "ui_state", "issue_list_msg_id", "issue_list_msg_id INTEGER")
    _ensure_column(cur, "ui_state", "issue_list_sent_time", "issue_list_sent_time INTEGER")

    # watch_rooms
    cur.execute("""
    CREATE TABLE IF NOT EXISTS watch_rooms (
        room_id        TEXT PRIMARY KEY,
        created_at     TEXT,
        created_by     TEXT,
        note           TEXT,
        chatroom_title TEXT
    )
    """)
    _ensure_column(cur, "watch_rooms", "chatroom_title", "chatroom_title TEXT")

    # issues
    cur.execute("""
    CREATE TABLE IF NOT EXISTS issues (
        issue_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        chatroom_id  TEXT NOT NULL,
        title        TEXT NOT NULL,
        content      TEXT,
        url          TEXT,
        occur_date   TEXT,
        target_date  TEXT,
        owner        TEXT,
        created_by   TEXT,
        created_at   TEXT,
        status       TEXT DEFAULT 'OPEN',
        closed_by    TEXT,
        closed_at    TEXT
    )
    """)
    _ensure_column(cur, "issues", "target_date", "target_date TEXT")
    _ensure_column(cur, "issues", "url", "url TEXT")

    # issue_events
    cur.execute("""
    CREATE TABLE IF NOT EXISTS issue_events (
        event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id    INTEGER,
        event_type  TEXT,
        actor       TEXT,
        event_at    TEXT,
        memo        TEXT,
        FOREIGN KEY(issue_id) REFERENCES issues(issue_id)
    )
    """)

    # dm_rooms
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dm_rooms (
        user_id    TEXT PRIMARY KEY,
        room_id    TEXT NOT NULL,
        created_at TEXT
    )
    """)

    con.commit()
    con.close()

# =========================
# parsing / dday / aging
# =========================
def _parse_dt(s: str):
    if not s:
        return None
    s = str(s).strip()

    # 눈에 안 보이는 공백/특수공백 제거
    s = s.replace("\u200b", "").replace("\ufeff", "").replace("\xa0", " ").strip()

    # ISO / 흔한 구분자 보정
    s = s.replace("T", " ").replace("/", "-").strip()

    # 밀리초(예: 2026-01-20 00:00:00.000)만 제거
    if ":" in s and "." in s:
        s = s.split(".", 1)[0].strip()
    else:
        # 날짜 구분자가 점인 케이스(2026.01.20)는 '-'로 바꿔서 살림
        s = s.replace(".", "-")

    # 1) 고정 포맷 우선
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt)
        except:
            pass

    # 2) 마지막 보험: dateutil parser (설치돼 있으면)
    try:
        from dateutil import parser as dtparser  # python-dateutil
        dt = dtparser.parse(s, fuzzy=True)
        return dt.replace(tzinfo=None) if getattr(dt, "tzinfo", None) else dt
    except:
        return None



def _parse_ymd(s: str):
    dt = _parse_dt(s)
    return dt.date() if dt else None

def _today():
    return datetime.now().date()

def _dday(target_date: str):
    td = _parse_ymd(target_date)
    if not td:
        return None
    return (td - _today()).days

def _age_days(created_at: str) -> int:
    dt = _parse_dt(created_at) or datetime.now()
    return max(0, (datetime.now().date() - dt.date()).days)

# =========================
# ui_state
# =========================
def ui_get_history_state(chatroom_id: str):
    con = db_connect()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT * FROM ui_state WHERE chatroom_id=? LIMIT 1", (str(chatroom_id),))
    row = cur.fetchone()
    con.close()
    return dict(row) if row else None

def ui_set_history_state(chatroom_id: str, msg_id: int, sent_time: int):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    con = db_connect()
    cur = con.cursor()
    cur.execute("""
      INSERT INTO ui_state(chatroom_id, history_msg_id, history_sent_time, updated_at)
      VALUES(?,?,?,?)
      ON CONFLICT(chatroom_id) DO UPDATE SET
        history_msg_id=excluded.history_msg_id,
        history_sent_time=excluded.history_sent_time,
        updated_at=excluded.updated_at
    """, (str(chatroom_id), int(msg_id), int(sent_time), now))
    con.commit()
    con.close()

def ui_get_issue_list_state(chatroom_id: str):
    con = db_connect()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT * FROM ui_state WHERE chatroom_id=? LIMIT 1", (str(chatroom_id),))
    row = cur.fetchone()
    con.close()
    return dict(row) if row else None

def ui_set_issue_list_state(chatroom_id: str, msg_id: int, sent_time: int):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    con = db_connect()
    cur = con.cursor()
    cur.execute("""
      INSERT INTO ui_state(chatroom_id, issue_list_msg_id, issue_list_sent_time, updated_at)
      VALUES(?,?,?,?)
      ON CONFLICT(chatroom_id) DO UPDATE SET
        issue_list_msg_id=excluded.issue_list_msg_id,
        issue_list_sent_time=excluded.issue_list_sent_time,
        updated_at=excluded.updated_at
    """, (str(chatroom_id), int(msg_id), int(sent_time), now))
    con.commit()
    con.close()

# =========================
# dm_rooms
# =========================
def dm_get_room(user_id: str) -> str | None:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT room_id FROM dm_rooms WHERE user_id=? LIMIT 1", (str(user_id),))
    row = cur.fetchone()
    con.close()
    return str(row[0]) if row and row[0] else None

def dm_set_room(user_id: str, room_id: str):
    con = db_connect()
    cur = con.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("""
      INSERT INTO dm_rooms(user_id, room_id, created_at)
      VALUES(?,?,?)
      ON CONFLICT(user_id) DO UPDATE SET room_id=excluded.room_id, created_at=excluded.created_at
    """, (str(user_id), str(room_id), now))
    con.commit()
    con.close()
def dm_room_upsert(user_id: int, room_id: int):
    dm_set_room(str(user_id), str(room_id))

# =========================
# watch rooms
# =========================
def add_watch_room(room_id: str, created_by: str, note: str = "", chatroom_title: str = ""):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    con = db_connect()
    cur = con.cursor()
    cur.execute("""
      INSERT OR REPLACE INTO watch_rooms(room_id, created_at, created_by, note, chatroom_title)
      VALUES(?,?,?,?,?)
    """, (str(room_id), now, created_by, note, chatroom_title))
    con.commit()
    con.close()

def list_watch_rooms() -> List[dict]:
    con = db_connect()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("""
        SELECT room_id, created_at, created_by, note,
               COALESCE(NULLIF(chatroom_title,''), NULLIF(note,''), room_id) AS title
        FROM watch_rooms
        ORDER BY created_at DESC
    """)
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows

def get_watch_rooms() -> List[str]:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT room_id FROM watch_rooms ORDER BY created_at DESC")
    rooms = [r[0] for r in cur.fetchall()]
    con.close()
    return rooms

# =========================
# issues CRUD
# =========================
def normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if not (u.startswith("http://") or u.startswith("https://")):
        u = "https://" + u
    return u

def issue_create(chatroom_id, title, content, url, occur_date, target_date, owner, created_by):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    con = db_connect()
    cur = con.cursor()

    url = normalize_url(url)

    cur.execute("""
      INSERT INTO issues(chatroom_id,title,content,url,occur_date,target_date,owner,created_by,created_at,status)
      VALUES (?,?,?,?,?,?,?,?,?, 'OPEN')
    """, (str(chatroom_id), title, content, url, occur_date, target_date, owner, created_by, now))

    issue_id = cur.lastrowid
    cur.execute("""
      INSERT INTO issue_events(issue_id,event_type,actor,event_at,memo)
      VALUES (?,?,?,?,?)
    """, (issue_id, "CREATE", created_by, now, ""))
    con.commit()
    con.close()
    return issue_id

def issue_list_open(chatroom_id):
    con = db_connect()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("""
      SELECT * FROM issues
      WHERE chatroom_id=? AND status='OPEN'
    """, (str(chatroom_id),))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()

    # ✅ d_day 계산 + 정렬 (None은 맨 뒤)
    for r in rows:
        r["d_day"] = _dday(r.get("target_date", ""))

    rows.sort(key=lambda x: (
        999999 if x.get("d_day") is None else x.get("d_day"),
        -int(x.get("issue_id", 0))
    ))
    return rows






def issue_list_all(chatroom_id):
    con = db_connect()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("""
      SELECT * FROM issues
      WHERE chatroom_id=?
      ORDER BY
        COALESCE(occur_date, substr(created_at,1,10)) DESC,
        issue_id DESC
    """, (str(chatroom_id),))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows

def issue_count_all(chatroom_id: str) -> int:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM issues WHERE chatroom_id=?", (str(chatroom_id),))
    n = int(cur.fetchone()[0] or 0)
    con.close()
    return n

def issue_list_all_paged(chatroom_id: str, page: int, page_size: int = HISTORY_PAGE_SIZE) -> List[dict]:
    offset = max(0, int(page)) * int(page_size)
    con = db_connect()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("""
      SELECT * FROM issues
      WHERE chatroom_id=?
      ORDER BY
        COALESCE(occur_date, substr(created_at,1,10)) DESC,
        issue_id DESC
      LIMIT ? OFFSET ?
    """, (str(chatroom_id), int(page_size), int(offset)))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows

def issue_get(chatroom_id, issue_id):
    con = db_connect()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("""
      SELECT * FROM issues
      WHERE chatroom_id=? AND issue_id=?
      LIMIT 1
    """, (str(chatroom_id), int(issue_id)))
    row = cur.fetchone()
    con.close()
    return dict(row) if row else None

def issue_update(chatroom_id, issue_id, title, content, url, occur_date, target_date, owner, actor):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    con = db_connect()
    cur = con.cursor()

    url = normalize_url(url)

    cur.execute("""
      UPDATE issues
      SET title=?, content=?, url=?, occur_date=?, target_date=?, owner=?
      WHERE chatroom_id=? AND issue_id=? AND status='OPEN'
    """, (title, content, url, occur_date, target_date, owner, str(chatroom_id), int(issue_id)))

    cur.execute("""
      INSERT INTO issue_events(issue_id,event_type,actor,event_at,memo)
      VALUES (?,?,?,?,?)
    """, (int(issue_id), "UPDATE", actor, now, ""))

    con.commit()
    con.close()

def issue_clear(chatroom_id, issue_id, actor):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    con = db_connect()
    cur = con.cursor()
    cur.execute("""
      UPDATE issues
      SET status='CLOSED', closed_by=?, closed_at=?
      WHERE chatroom_id=? AND issue_id=? AND status='OPEN'
    """, (actor, now, str(chatroom_id), int(issue_id)))
    cur.execute("""
      INSERT INTO issue_events(issue_id,event_type,actor,event_at,memo)
      VALUES (?,?,?,?,?)
    """, (int(issue_id), "CLOSE", actor, now, ""))
    con.commit()
    con.close()

def issue_delete(chatroom_id, issue_id, actor):
    con = db_connect()
    cur = con.cursor()
    cur.execute("""
      SELECT status FROM issues
      WHERE chatroom_id=? AND issue_id=?
      LIMIT 1
    """, (str(chatroom_id), int(issue_id)))
    row = cur.fetchone()

    if not row:
        con.close()
        return False, "해당 이슈가 없습니다."

    status = (row[0] or "").upper()
    if status != "CLOSED":
        con.close()
        return False, "CLOSED 이슈만 삭제 가능합니다. (먼저 Clear 해주세요)"

    cur.execute("DELETE FROM issue_events WHERE issue_id=?", (int(issue_id),))
    cur.execute("DELETE FROM issues WHERE chatroom_id=? AND issue_id=?", (str(chatroom_id), int(issue_id)))

    con.commit()
    con.close()
    return True, "삭제 완료"

# =========================
# dashboard support
# =========================
def issue_list_all_any(status: str | None = None) -> List[dict]:
    con = db_connect()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    if status in ("OPEN", "CLOSED"):
        cur.execute("SELECT * FROM issues WHERE status=? ORDER BY issue_id DESC", (status,))
    else:
        cur.execute("SELECT * FROM issues ORDER BY issue_id DESC")
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows

def issue_list_open_all() -> List[dict]:
    return issue_list_all_any("OPEN")

def issue_list_closed_recent(days: int = 60) -> List[dict]:
    con = db_connect()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("""
        SELECT * FROM issues
        WHERE status='CLOSED'
          AND closed_at IS NOT NULL
          AND date(closed_at) >= date('now', ?)
        ORDER BY closed_at DESC
    """, (f"-{days} day",))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows

def issue_event_exists(issue_id: int, event_type: str, memo: str) -> bool:
    con = db_connect()
    cur = con.cursor()
    cur.execute("""
      SELECT 1 FROM issue_events
      WHERE issue_id=? AND event_type=? AND memo=?
      LIMIT 1
    """, (int(issue_id), event_type, memo))
    ok = cur.fetchone() is not None
    con.close()
    return ok

def issue_event_add(issue_id: int, event_type: str, actor: str, memo: str = ""):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    con = db_connect()
    cur = con.cursor()
    cur.execute("""
      INSERT INTO issue_events(issue_id,event_type,actor,event_at,memo)
      VALUES (?,?,?,?,?)
    """, (int(issue_id), event_type, actor, now, memo))
    con.commit()
    con.close()

def get_last_activity_map(issue_ids: List[int]) -> Dict[int, str]:
    if not issue_ids:
        return {}
    con = db_connect()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    q = ",".join(["?"] * len(issue_ids))
    cur.execute(f"""
        SELECT issue_id, MAX(event_at) AS last_event_at
        FROM issue_events
        WHERE issue_id IN ({q})
        GROUP BY issue_id
    """, tuple(issue_ids))
    mp = {int(r["issue_id"]): (r["last_event_at"] or "") for r in cur.fetchall()}
    con.close()
    return mp

def build_week_series(created_rows: List[dict], closed_rows: List[dict], weeks: int = 8):
    def wk(dt: datetime) -> str:
        return dt.strftime("%Y-%W")

    today = datetime.now()
    labels = []
    for i in range(weeks-1, -1, -1):
        d = today - timedelta(days=i*7)
        labels.append(wk(d))

    created_cnt = {k: 0 for k in labels}
    closed_cnt  = {k: 0 for k in labels}

    for r in created_rows:
        dt = _parse_dt(r.get("created_at",""))
        if not dt:
            continue
        k = wk(dt)
        if k in created_cnt:
            created_cnt[k] += 1

    for r in closed_rows:
        dt = _parse_dt(r.get("closed_at",""))
        if not dt:
            continue
        k = wk(dt)
        if k in closed_cnt:
            closed_cnt[k] += 1

    return {
        "labels": labels,
        "created": [created_cnt[k] for k in labels],
        "closed":  [closed_cnt[k] for k in labels],
    }

def scope_room_id(chatroom_id: int, payload: dict) -> str:
    rid = (payload.get("room_id") or "").strip()
    return rid if rid else str(chatroom_id)
