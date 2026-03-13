# pip install pycryptodomex fastapi uvicorn apscheduler requests pandas holidays langchain-openai oracledb
import os
import json
import base64
import time
import math
import uuid
import re
import sqlite3
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import defaultdict

import requests
import pandas as pd
import oracledb
import uvicorn
import urllib3

from Cryptodome.Cipher import AES
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import HTMLResponse
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import store
import ui
from app.oracle_db import init_oracle_thick_mode_once, run_oracle_query_compat, startup_initialize
from app.sql_registry import (
    build_execution_plan,
    build_match_for_query_id,
    build_sql_intent_prompt,
    configure_sql_intent_llm_classifier,
    find_best_sql_registry_match,
    get_last_sql_nlu_trace,
)
from app.query_intent import classify_query_intent
from app.hybrid_router import build_route_decision, execute_sql_match
from app.hybrid_answer import (
    summarize_sql_result,
    build_data_only_answer,
    build_hybrid_prompt,
    build_hybrid_fallback_answer,
)
from app.sql_answering import render_answer_rule_based, render_answer_with_llm
from app.feedback import normalize_feedback_type, normalize_reason_code
from app.search_improvement import (
    detect_weekly_issue_query,
    detect_topic as detect_weekly_topic,
    compute_target_week_label,
    build_weekly_search_query_variants,
    rerank_weekly_issue_docs,
    summarize_rerank_reason,
    extract_week_tokens_from_doc,
)
from app.doc_summary_postprocess import enrich_sparse_issue_lines

from zoneinfo import ZoneInfo
import holidays
from langchain_openai import ChatOpenAI

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# 0) ENV (여기만 채우면 동작)
# =========================
DASHBOARD_TOKEN = os.getenv("DASHBOARD_TOKEN", "goc")  # 토큰 비우면 오픈 모드
DASHBOARD_TITLE = os.getenv("DASHBOARD_TITLE", "GOC Issue Dashboard")

KNOX_HOST      = os.getenv("KNOX_HOST", "https://openapi.samsung.net")
KNOX_SYSTEM_ID = os.getenv("KNOX_SYSTEM_ID", "KCC10REST01591")
KNOX_TOKEN     = os.getenv("KNOX_TOKEN", "Bearer 0937decd-9394-38fe-bb5a-348d2d618c67")

ORACLE_HOST    = os.getenv("ORACLE_HOST", "gmgsdd09-vip.sec.samsung.net")
ORACLE_PORT    = int(os.getenv("ORACLE_PORT", "2541"))
ORACLE_SERVICE = os.getenv("ORACLE_SERVICE", "MEMSCM")
ORACLE_USER    = os.getenv("ORACLE_USER", "memscm")
ORACLE_PW      = os.getenv("ORACLE_PW", "mem01scm")

PROXY_HTTP  = os.getenv("PROXY_HTTP", "")
PROXY_HTTPS = os.getenv("PROXY_HTTPS", "")
VERIFY_SSL  = os.getenv("VERIFY_SSL", "false").lower() == "true"

BIND_HOST = os.getenv("BIND_HOST", "12.52.147.157")
BIND_PORT = int(os.getenv("BIND_PORT", "9500"))

DASHBOARD_URL = os.getenv("DASHBOARD_URL", f"http://{BIND_HOST}:{BIND_PORT}/dashboard")
INFOCENTER_URL = os.getenv(
    "INFOCENTER_URL",
    "https://assistant.samsungds.net/#/main?studio_id=1245feb9-7770-4bdc-99d0-871f40a87536"
)
RIGHTPERSON_JSON_URL = os.getenv("RIGHTPERSON_JSON_URL", "http://12.52.146.94:7000/json/%EB%8B%B4%EB%8B%B9%EC%9E%90.json")
TERM_JSON_PATH = os.getenv("TERM_JSON_PATH", r"F:\Workspace\output.json")
TERM_ADMIN_ROOM_IDS = os.getenv("TERM_ADMIN_ROOM_IDS", "")  # 예

# ✅ 카드 recall(회수) 기능: 기본 OFF 권장(통신 오류 시스템 메시지 유발 가능)

ENABLE_RECALL = os.getenv("ENABLE_RECALL", "true").lower() == "true"
# LLM 대화 기본 동작
# - "off": /ask 또는 "질문:"만 LLM
# - "single": 1:1(SINGLE)에서는 /ask 없이도 모든 일반 텍스트를 LLM
# - "mention": 단체방(GROUP)은 멘션/접두어 있을 때만 LLM
# - "all": 단체방도 일반 텍스트면 LLM (비추)
LLM_CHAT_DEFAULT_MODE = os.getenv("LLM_CHAT_DEFAULT_MODE", "single")
LLM_GROUP_MENTION_TEXT = os.getenv("LLM_GROUP_MENTION_TEXT", "@공급망 챗봇")
LLM_GROUP_PREFIXES = [x.strip() for x in os.getenv("LLM_GROUP_PREFIXES", "봇,챗봇").split(",") if x.strip()]

# Conversation memory (SINGLE 전용)
ENABLE_CONVERSATION_MEMORY = os.getenv("ENABLE_CONVERSATION_MEMORY", "true").lower() == "true"
MEMORY_MAX_TURNS = max(1, int(os.getenv("MEMORY_MAX_TURNS", "4")))
MEMORY_MAX_CHARS_PER_MESSAGE = max(50, int(os.getenv("MEMORY_MAX_CHARS_PER_MESSAGE", "300")))
MEMORY_SUMMARIZE_ASSISTANT = os.getenv("MEMORY_SUMMARIZE_ASSISTANT", "true").lower() == "true"
MEMORY_ONLY_SINGLE = os.getenv("MEMORY_ONLY_SINGLE", "true").lower() == "true"
MEMORY_RESET_COMMANDS = [x.strip() for x in os.getenv("MEMORY_RESET_COMMANDS", "/reset,기억초기화,대화초기화").split(",") if x.strip()]
MEMORY_DB_PATH = os.getenv("MEMORY_DB_PATH", "")
ENABLE_CONVERSATION_STATE = os.getenv("ENABLE_CONVERSATION_STATE", "true").lower() == "true"

# =========================
# LLM API Configuration (GaussO4)
# =========================
# 테스트 키 (운영 키로 변경하려면 아래 값만 수정)
LLM_API_KEY = os.getenv("LLM_API_KEY", "credential:TICKET-96f7bce0-efab-4516-8e62-5501b07ab43c:ST0000107488-PROD:CTXLCkSDRGWtI5HdVHkPAQgol2o-RyQiq2I1vCHHOgGw:-1:Q1RYTENrU0RSR1d0STVIZFZIa1BBUWdvbDJvLVJ5UWlxMkkxdkNISE9nR3c=:signature=eRa1UcfmWGfKTDBt-Xnz2wFhW0OvMX0WESZUpoNVgCA5uNVgpgax59LZ3osPOp8whnZwQay8s5TUvxJGtmsCD9iK-HpcsyUOcE5P58W0Weyg-YQ3KRTWFiA==")
LLM_API_URL = os.getenv("LLM_API_URL", "http://apigw.samsungds.net:8000/model-23/1/gausso4-instruct/v1")
LLM_MODEL_NAME = os.getenv("LLM_MODEL_NAME", "GaussO4-instruct")
LLM_SEND_SYSTEM_NAME = os.getenv("LLM_SEND_SYSTEM_NAME", "GOC_MAIL_RAG_PIPELINE")
LLM_USER_TYPE = os.getenv("LLM_USER_TYPE", "bot")

# =========================
# RAG API Configuration
# =========================
# RAG API 키 (DS API HUB 키)
RAG_DEP_TICKET = os.getenv("RAG_DEP_TICKET", "credential:TICKET-e09692e2-45e3-46e7-ab4d-e75c06ef2b47:ST0000106045-PROD:n591JsqkTh-51wynrJeZ3Qbk2a5Oo2TfGDc9P6pAkN9Q:-1:bjU5MUpzcWtUaC01MXd5bnJKZVozUWJrMmE1T28yVGZHRGM5UDZwQWtOOVE=:signature=x-Dh7diDnQqQAVyfObfHxQoqHxyH7zGC4irZ9vA0Wgfi9zNURR853sMEXG5QcMnYUHXCclma5dGSMwDWSOgGQBvesPSHRz3zvarPfkcFqovLv6OgNZw_X5A==")
# RAG Portal 키
RAG_API_KEY = os.getenv("RAG_API_KEY", "rag-laeeKyA.KazNAgzjr-d1iK9rUClS2vdqKLZ4oOOcsOhhuR3tJaAYa3h73BE7SdjgLjxQsEtJCN6Oc7B1mJYq1Pu_ruTKmcmeujAVpmDxms44OdjGCeHGBTisaSFHdqyepsbEa3nw")
RAG_BASE_URL = os.getenv("RAG_BASE_URL", "http://apigw.samsungds.net:8000/ds_llm_rag/2/dsllmrag/elastic/v2")
# RAG 인덱스 목록 (쉼표로 구분, 나중에 추가 가능)
RAG_INDEXES = os.getenv("RAG_INDEXES", "rp-gocinfo_mail_jsonl,glossary_m3_100chunk50")
RAG_PERMISSION_GROUPS = os.getenv("RAG_PERMISSION_GROUPS", "rag-public")
# RAG 후보는 top6까지만 가져오고, 최종 컨텍스트는 top3만 사용
RAG_NUM_RESULT_DOC = int(os.getenv("RAG_NUM_RESULT_DOC", "3"))   # vector search top_k (latency-tuned default)
RAG_CONTEXT_DOCS = int(os.getenv("RAG_CONTEXT_DOCS", "2"))       # rerank 후 최종 반영 top_k (latency-tuned default)
RAG_TEMPORAL_NUM_RESULT_DOC = int(os.getenv("RAG_TEMPORAL_NUM_RESULT_DOC", "20"))  # 시간조건 질문일 때 후보 확장
RAG_API_MAX_NUM_RESULT_DOC = int(os.getenv("RAG_API_MAX_NUM_RESULT_DOC", "100"))
RAG_REWRITE_QUERY_COUNT = max(1, int(os.getenv("RAG_REWRITE_QUERY_COUNT", "1")))
ENABLE_QUERY_REWRITE = os.getenv("ENABLE_QUERY_REWRITE", "true").lower() == "true"
MAX_RAG_QUERIES = max(1, int(os.getenv("MAX_RAG_QUERIES", "1")))
RAG_INCLUDE_ORIGINAL_QUERY = os.getenv("RAG_INCLUDE_ORIGINAL_QUERY", "true").lower() == "true"
RAG_RETRIEVE_MODE = os.getenv("RAG_RETRIEVE_MODE", "hybrid").lower()
RAG_BM25_BOOST = float(os.getenv("RAG_BM25_BOOST", "0.025"))
RAG_KNN_BOOST = float(os.getenv("RAG_KNN_BOOST", "7.98"))
RAG_SIMILARITY_THRESHOLD = float(os.getenv("RAG_SIMILARITY_THRESHOLD", "0.35"))
RAG_RECENCY_WEIGHT = float(os.getenv("RAG_RECENCY_WEIGHT", "0.28"))   # 최신성 가중치
RAG_RECENCY_HALF_LIFE_DAYS = float(os.getenv("RAG_RECENCY_HALF_LIFE_DAYS", "30"))  # 반감기(일)
RAG_MIN_RECENCY_SCORE = float(os.getenv("RAG_MIN_RECENCY_SCORE", "0.15"))  # 날짜 없을 때 최소점수

# =========================
# Glossary RAG Configuration
# =========================
GLOSSARY_RAG_ENABLE = os.getenv("GLOSSARY_RAG_ENABLE", "true").lower() == "true"
GLOSSARY_THRESHOLD = float(os.getenv("GLOSSARY_THRESHOLD", os.getenv("GLOSSARY_RELAXED_THRESHOLD", "0.35")))
GLOSSARY_TOPK_MATCH = int(os.getenv("GLOSSARY_TOPK_MATCH", "3"))
GLOSSARY_INDEX_NAME = os.getenv("GLOSSARY_INDEX_NAME", "glossary_m3_100chunk50")
MAIL_INDEX_NAME = os.getenv("MAIL_INDEX_NAME", "rp-gocinfo_mail_jsonl")

LLM_WORKERS = max(1, int(os.getenv("LLM_WORKERS", os.getenv("LLM_WORKER_COUNT", "4"))))
LLM_JOB_QUEUE_MAX = max(1, int(os.getenv("LLM_JOB_QUEUE_MAX", "200")))
LLM_MAX_CONCURRENT = max(1, int(os.getenv("LLM_MAX_CONCURRENT", "4")))
LLM_PROFILE_LOG = os.getenv("LLM_PROFILE_LOG", "true").lower() == "true"
ISSUE_SUMMARY_SPEED_MODE = os.getenv("ISSUE_SUMMARY_SPEED_MODE", "false").lower() == "true"
ENABLE_FEEDBACK_CARD = os.getenv("ENABLE_FEEDBACK_CARD", "false").lower() == "true"
LLM_ALLOWED_USERS_SQL = os.getenv(
    "LLM_ALLOWED_USERS_SQL",
    "SELECT SSO_ID FROM SCM_WP.T_T_FOR_MASTER A WHERE 1=1 and a.dept_name in ('공급망운영그룹(메모리)','SCM그룹(메모리)','운영전략그룹(메모리)','Global운영팀(메모리)') and A.DEPT_NAME LIKE '%메모리%' and a.POSITION_CODE is not null AND A.SSO_ID NOT IN ('SCM.RPA','SCM 봇','메모리STO2','메모리 STO','dalbong.chatbot01', 'dalbongbot01', 'dalbong.bot01', 'command.center', 'thatcoolguy')"
)
LLM_ALLOWED_USERS_CACHE_TTL_SEC = max(0, int(os.getenv("LLM_ALLOWED_USERS_CACHE_TTL_SEC", "1800")))
SQL_ANSWER_LLM_ENABLE = os.getenv("SQL_ANSWER_LLM_ENABLE", "true").lower() == "true"

# ✅ SINGLE(1:1) 단축키 → URL
# ✅ SINGLE(1:1) 단축키(별칭 묶음) → URL
QUICK_LINK_ALIASES = [
    (["GSCM"], "🧭 GSCM", "https://dsgscm.sec.samsung.net/"),
    (["조성묵"], "🧭 조성묵", "mysingleim://ids=sungmook.cho&msg=7ZmI"),
    (["NSCM", "O9"], "📦 NSCM", "https://nextscm.sec.samsung.net/Kibo2#/P-Mix%20Item/DRAM/P-Mix%20Item%20(DRAM)"),
    (["EDM"], "🗂️ EDM", "https://edm2.sec.samsung.net/cc/#/home/efss/recent"),
    (["컨플","컨플루언스","CONF","CONFLUENCE"], "📚 컨플루언스", "https://confluence.samsungds.net/spaces/GOCMEM/pages/182884004/Global%EC%9A%B4%EC%98%81%ED%8C%80+%EB%A9%94%EB%AA%A8%EB%A6%AC"),
    (["SMDM","MDM"], "🗃️ SMDM", "http://smdm.samsungds.net/irj/portal"),
    (["이미지"], "🖼️ 이미지 공유", "https://img/home"),
    (["파워","파워BI","PB","POWERBI","POWER BI","BI"], "📊 Power BI", "http://10.227.100.251/Reports/browse"),
    (["DSASSISTANT","GPT"], "🤖 DS Assistant", "https://assistant.samsungds.net/#/main"),
    (["GITHUB","GIT","깃허브","깃헙"], "🧑‍💻 GitHub", "https://github.samsungds.net/SCM-Group-MEM/SCM_DO"),
    (["밥","식단","점심","아침","저녁","배고파"], "🍱 식단", "https://vkghrap.sec.samsung.net:5999/cis/info/cafeteria/mealMenuInfo.do?_menuId=AWiWxU1cAAEZgNXQ&_menuF=true"),
    (["버스","출퇴근","통근"], "🚌 출퇴근버스", "http://samsung.u-vis.com:8080/portalm/VISMain.do?method=main&pickoffice=0000011"),
    (["패밀리","패밀리넷","패넷","FAMILYNET"], "🏡 패밀리넷", "https://familynet.samsung.com/"),
    (["DSDN"], "💬 DSDN", "https://dsdn.samsungds.net/questions/space/scooldspace:all/"),
    (["이모지"], "😀 이모지 모음", "https://emojidb.org/"),
    (["MSTR"], "📈 MSTR", "https://dsdapmstrsvc.samsungds.net/Mstr/servlet/mstrWeb"),
    (["싱글","녹스","메일"], "🛡️ 싱글/Knox", "https://www.samsung.net/"),
    (["정보센터","정보"], "🗞️ GOC 정보센터", "https://assistant.samsungds.net/#/main?studio_id=1245feb9-7770-4bdc-99d0-871f40a87536"),
    (["근태","근무시간"], "⏱️ 근태", "https://ghrp.kr.sec.samsung.net/shcm/main/openpage?encMenuId=4fd503562d7a42abe598ecae64b53e5cd67df78992b2e2425605d89582270ae8&"),
    (["MPVOC","MP VOC"], "📝 MP VOC", "https://service-hub--sh-servicehub-prod.kspprd.dks.cloud.samsungds.net/sh/svoc/voc/vocReg?sysMapMstId=SSTM01090_MDLE05840_DVSN00120_CMPS00040_2025030415334402679&vocClassCode=RQTP0002"),
    (["NSCMVOC","NSCM VOC"], "📝 NSCM VOC", "https://service-hub--sh-servicehub-prod.kspprd.dks.cloud.samsungds.net/sh/svoc/voc/vocReg?sysMapMstId=SSTM00130_MDLE00740_DVSN00120_CMPS00040_2025030415334301856&vocClassCode=RQTP0002"),
]

def resolve_quick_link(key: str):
    k = (key or "").strip().upper()
    for aliases, title, url in QUICK_LINK_ALIASES:
        if k in [a.upper() for a in aliases]:
            return title, url
    return None, None


# =========================
# Dashboard token guard
# =========================
def _require_dashboard_token(token: str | None):
    if DASHBOARD_TOKEN:
        if (token or "") != DASHBOARD_TOKEN:
            raise HTTPException(status_code=401, detail="Invalid token")

def _limit_utf8mb4_bytes(s: str, max_bytes: int = 128) -> str:
    if not s:
        return s
    b = s.encode("utf-8")
    if len(b) <= max_bytes:
        return s
    cut = max_bytes
    while cut > 0:
        try:
            return b[:cut].decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            cut -= 1
    return ""


def _answer_preview(text: str, max_len: int = 280) -> str:
    value = (text or "").replace("\n", " ").strip()
    if len(value) <= max_len:
        return value
    return value[: max_len - 3] + "..."


def _sanitize_glossary_answer(answer: str) -> str:
    text = answer or ""
    lines = text.splitlines()
    cleaned_lines: List[str] = []
    in_doc_section = False
    heading_pattern = re.compile(r"^\s*[📌📂💡⚠️🔗]")
    date_prefix_pattern = re.compile(r"^(\s*[-•]\s*)\((\d{4}-\d{2}-\d{2})(?:[^\)]*)\)\s*")

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("📂 문서 기반 답변"):
            in_doc_section = True
            cleaned_lines.append(line)
            continue
        if in_doc_section and heading_pattern.match(stripped) and not stripped.startswith("📂 문서 기반 답변"):
            in_doc_section = False

        if "기준일시" in stripped:
            continue

        if in_doc_section:
            line = date_prefix_pattern.sub(r"\1", line)

        cleaned_lines.append(line)

    return "\n".join(cleaned_lines).strip()

# =========================
# 1) AES Cipher (Knox key 기반)
# =========================
class AESCipher:
    def __init__(self, key_hex: str):
        self.BS = 16
        raw = bytes.fromhex(key_hex)
        self.key = raw[0:32]
        self.iv  = raw[32:48]

    def _pad(self, b: bytes) -> bytes:
        pad_len = self.BS - (len(b) % self.BS)
        return b + bytes([pad_len]) * pad_len

    def _unpad(self, b: bytes) -> bytes:
        pad_len = b[-1]
        return b[:-pad_len]

    def encrypt(self, data: str) -> str:
        pt = self._pad(data.encode("utf-8"))
        cipher = AES.new(self.key, AES.MODE_CBC, self.iv)
        ct = cipher.encrypt(pt)
        return base64.b64encode(ct).decode("utf-8")

    def decrypt(self, data_b64: bytes) -> str:
        ct = base64.b64decode(data_b64)
        cipher = AES.new(self.key, AES.MODE_CBC, self.iv)
        pt = self._unpad(cipher.decrypt(ct))
        return pt.decode("utf-8", errors="ignore")


# =========================
# 2) Knox Messenger Client
# =========================
class KnoxMessenger:
    def __init__(self, host: str, systemId: str, token: str):
        self.host = host
        self.systemId = systemId
        self.token = token

        self.userID = ""
        self.x_device_id = ""
        self.key = ""  # getKeys()에서 채움

        self.session = requests.Session()
        # if PROXY_HTTP or PROXY_HTTPS:
        #     self.session.proxies = {}
        #     if PROXY_HTTP:
        #         self.session.proxies["http"] = PROXY_HTTP
        #     if PROXY_HTTPS:
        #         self.session.proxies["https"] = PROXY_HTTPS

    def recall_message(self, chatroom_id: int, msg_id: int, sent_time: int):
        api = "/messenger/message/api/v1.0/message/recallMessageRequest"
        requestid = int(round(time.time() * 1000))
        body = {
            "requestId": requestid,
            "chatroomId": int(chatroom_id),
            "msgId": int(msg_id),
            "sentTime": int(sent_time),
        }
        return self._post_encrypted(api, body)

    def device_regist(self, max_retries: int = 3, retry_delay: int = 5):
        API = "/messenger/contact/api/v1.0/device/o1/reg"
        header = {"Authorization": self.token, "System-ID": self.systemId}
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(self.host + API, headers=header, verify=VERIFY_SSL)
                print(f"[device_regist] Attempt {attempt + 1}/{max_retries} - Status: {response.status_code}")
                
                if response.status_code >= 500:
                    # 서버 에러 (502, 503 등) - 재시도
                    if attempt < max_retries - 1:
                        print(f"[device_regist] Server error {response.status_code}, retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        raise ValueError(f"API 서버 오류 (Status: {response.status_code}). 서버 상태를 확인하세요.")
                
                if not response.text or response.text.strip() == "":
                    raise ValueError(f"API 응답이 비어있습니다. Status: {response.status_code}")
                
                # HTML 응답 체크 (에러 페이지)
                if response.text.strip().startswith("<!DOCTYPE") or response.text.strip().startswith("<html"):
                    raise ValueError(f"API가 HTML 에러 페이지를 반환했습니다. Status: {response.status_code}\n서버 상태를 확인하세요.")
                
                data = json.loads(response.text)
                self.userID = str(data["userID"])
                self.x_device_id = str(data["deviceServerID"])
                print(f"[device_regist] Success - userID: {self.userID}, deviceServerID: {self.x_device_id}")
                return
                
            except json.JSONDecodeError as e:
                if attempt < max_retries - 1:
                    print(f"[device_regist] JSON 파싱 실패, 재시도 중... ({e})")
                    time.sleep(retry_delay)
                    continue
                else:
                    raise ValueError(f"JSON 파싱 실패: {e}\n응답 내용: {response.text[:500]}")
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"[device_regist] 오류 발생, 재시도 중... ({e})")
                    time.sleep(retry_delay)
                    continue
                else:
                    raise

    def getKeys(self):
        API = "/messenger/msgctx/api/v1.0/key/getkeys"
        header = {"Authorization": self.token, "x-device-id": self.x_device_id}
        resp = self.session.get(self.host + API, headers=header, verify=VERIFY_SSL).text
        data = json.loads(resp)
        self.key = data["key"]

    def _post_encrypted(self, api: str, body_dict: dict, extra_headers: Optional[dict] = None) -> dict:
        header = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": self.token,
            "System-ID": self.systemId,
            "x-device-id": self.x_device_id,
            "x-device-type": "relation",
        }
        if extra_headers:
            header.update(extra_headers)

        requestid = int(round(time.time() * 1000))
        if "requestId" not in body_dict:
            body_dict["requestId"] = requestid

        cipher = AESCipher(self.key)
        enc_body = cipher.encrypt(json.dumps(body_dict, ensure_ascii=False))
        resp = self.session.post(self.host + api, headers=header, data=enc_body, verify=VERIFY_SSL).text

        rt = (resp or "").strip()
        dec = cipher.decrypt(rt.encode("utf-8"))
        return json.loads(dec)

    def send_text(self, chatroom_id: int, text: str):
        api = "/messenger/message/api/v1.0/message/chatRequest"
        requestid = int(round(time.time() * 1000))
        body = {
            "requestId": requestid,
            "chatroomId": int(chatroom_id),
            "chatMessageParams": [
                {"msgId": requestid, "msgType": 0, "chatMsg": text, "msgTtl": 3600}
            ],
        }
        resp = self._post_encrypted(api, body)
        if isinstance(resp, dict):
            resp["_request_msg_id"] = requestid
        return resp

    def send_adaptive_card(self, chatroom_id: int, card: dict):
        api = "/messenger/message/api/v1.0/message/chatRequest"
        requestid = int(round(time.time() * 1000))
        card_str = json.dumps(card, ensure_ascii=False)
        payload = {"adaptiveCards": card_str}

        body = {
            "requestId": requestid,
            "chatroomId": int(chatroom_id),
            "chatMessageParams": [
                {
                    "msgId": requestid,
                    "msgType": 19,
                    "chatMsg": json.dumps(payload, ensure_ascii=False),
                    "msgTtl": 3600,
                }
            ],
        }
        return self._post_encrypted(api, body)

    def send_table_csv_msg7(self, chatroom_id: int, df: pd.DataFrame, title: str = "조회 결과"):
        api = "/messenger/message/api/v1.0/message/chatRequest"
        requestid = int(round(time.time() * 1000))

        chat_msg = ui.df_to_knox_csv_msg7(df, title=title)
        body = {
            "requestId": requestid,
            "chatroomId": int(chatroom_id),
            "chatMessageParams": [
                {
                    "msgId": requestid,
                    "msgType": 7,
                    "chatMsg": chat_msg,
                    "msgTtl": 3600,
                }
            ],
        }
        return self._post_encrypted(api, body)

    def resolve_user_ids_from_loginids(self, login_ids: List[str]) -> List[str]:
        api = "/messenger/contact/api/v1.0/profile/o1/search/loginid"
        header = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": self.token,
            "System-ID": self.systemId,
            "x-device-id": self.x_device_id,
            "x-device-type": "relation",
        }
        body = {"singleIdList": [{"singleId": x} for x in login_ids if x]}

        resp = self.session.post(self.host + api, headers=header, data=json.dumps(body), verify=VERIFY_SSL).text
        data = json.loads(resp)

        out = []
        for item in data.get("userSearchResult", {}).get("searchResultList", []):
            out.append(str(item["userID"]))
        return out

    def room_create(
        self,
        receivers_userid: List[str],
        *,
        chatType: int = 1,
        chatroom_title: Optional[str] = None,
    ) -> int:
        api = "/messenger/message/api/v1.0/message/createChatroomRequest"
        requestid = int(round(time.time() * 1000))

        body = {
            "requestId": requestid,
            "chatType": int(chatType),
            "receivers": receivers_userid,
        }
        if chatroom_title:
            body["chatroomTitle"] = _limit_utf8mb4_bytes(chatroom_title, 128)

        resp = self._post_encrypted(api, body)
        return int(resp["chatroomId"])


# =========================
# LLM Chatbot (GaussO4)
# =========================
# LangChain ChatOpenAI에서 필요한 더미 키
os.environ["OPENAI_API_KEY"] = "api_key"

def create_llm_chatbot(user_id: str = "bot"):
    """GaussO4 LLM 챗봇 인스턴스 생성"""
    headers = {
        "x-dep-ticket": LLM_API_KEY,
        "Send-System-Name": LLM_SEND_SYSTEM_NAME,
        "User-Id": user_id,
        "User-Type": LLM_USER_TYPE,
        "Prompt-Msg-Id": str(uuid.uuid4()),
        "Completion-Msg-Id": str(uuid.uuid4()),
    }
    
    llm = ChatOpenAI(
        base_url=LLM_API_URL,
        model=LLM_MODEL_NAME,
        max_tokens=2000,
        temperature=0.3,
        default_headers=headers
    )
    
    return llm
def _is_retryable_llm_error(e: Exception) -> bool:
    s = str(e)
    return (
        "Error code: 502" in s
        or "Error code: 503" in s
        or "Error code: 504" in s
        or "upstream server" in s.lower()
        or "invalid response" in s.lower()
    )

def llm_invoke_with_retry(llm, payload, *, attempts: int = 3, base_delay: float = 1.5):
    """
    payload: messages(list) 또는 str 모두 지원
    """
    last_err = None
    for i in range(1, attempts + 1):
        try:
            return llm.invoke(payload)
        except Exception as e:
            last_err = e
            if not _is_retryable_llm_error(e) or i == attempts:
                raise
            delay = base_delay * i  # simple backoff
            print(f"[LLM retry] attempt={i}/{attempts} err={e} sleep={delay}s")
            time.sleep(delay)
    raise last_err  # pragma: no cover


def classify_sql_intent_with_llm(llm, question: str, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    from langchain_core.messages import HumanMessage, SystemMessage

    system_prompt = (
        "You classify Korean sales questions into registered intents only. "
        "Return strict JSON only with keys: intent, confidence, slots."
    )
    user_prompt = build_sql_intent_prompt(question, context)
    messages = [SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)]

    try:
        resp = llm_invoke_with_retry(llm, messages, attempts=2, base_delay=0.8)
        content = getattr(resp, "content", "") if resp is not None else ""
        text = content if isinstance(content, str) else str(content)
        m = re.search(r"\\{[\\s\\S]*\\}", text)
        if not m:
            return None
        obj = json.loads(m.group(0))
        if not isinstance(obj, dict):
            return None
        return obj
    except Exception as e:
        print(f"[SQL_NLU] llm classify failed: {e}")
        return None


def render_sql_answer_with_llm(llm, prompt: str) -> Optional[str]:
    from langchain_core.messages import HumanMessage, SystemMessage

    system_prompt = (
        "당신은 SQL 조회 결과를 사용자 친화적으로 설명하는 도우미입니다. "
        "SQL 생성/수정은 금지하며, 입력 구조를 근거로만 답변하세요."
    )
    try:
        resp = llm_invoke_with_retry(
            llm,
            [SystemMessage(content=system_prompt), HumanMessage(content=prompt)],
            attempts=2,
            base_delay=0.8,
        )
        content = getattr(resp, "content", "") if resp is not None else ""
        text = content if isinstance(content, str) else str(content)
        return text.strip() if text else None
    except Exception as e:
        print(f"[SQL_ANSWER] llm render failed: {e}")
        return None

# =========================
# RAG Client
# =========================
class RagClient:
    """RAG API 클라이언트"""

    def __init__(self, api_key: str, dep_ticket: str, base_url: str, timeout: Tuple[int, int] = (5, 60)):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.sess = requests.Session()
        self.sess.headers.update({
            "Content-Type": "application/json",
            "x-dep-ticket": dep_ticket,
            "api-key": api_key,
        })

    def retrieve(
        self,
        index_name: str,
        query_text: str,
        *,
        mode: str = "hybrid",
        num_result_doc: int = 3,
        permission_groups: Optional[List[str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        bm25_boost: Optional[float] = None,
        knn_boost: Optional[float] = None,
    ) -> Dict[str, Any]:
        """RAG 문서 검색: bm25 / knn / hybrid / weighted_hybrid"""
        endpoint_map = {
            "bm25": "/retrieve-bm25",
            "knn": "/retrieve-knn",
            "hybrid": "/retrieve-rrf",
            "weighted_hybrid": "/retrieve-weighted-hybrid",
        }
        selected_mode = mode if mode in endpoint_map else "hybrid"
        url = f"{self.base_url}{endpoint_map[selected_mode]}"
        payload: Dict[str, Any] = {
            "index_name": index_name,
            "permission_groups": permission_groups or ["rag-public"],
            "query_text": query_text,
            "num_result_doc": num_result_doc,
        }

        if filter:
            payload["filter"] = filter
        if selected_mode == "weighted_hybrid":
            if bm25_boost is not None:
                payload["bm25_boost"] = bm25_boost
            if knn_boost is not None:
                payload["knn_boost"] = knn_boost

        r = self.sess.post(url, data=json.dumps(payload, ensure_ascii=False), timeout=self.timeout)
        if 200 <= r.status_code < 300:
            return r.json()
        raise Exception(f"RAG API Error: {r.status_code} - {r.text}")

def create_rag_client() -> RagClient:
    """RAG 클라이언트 인스턴스 생성"""
    return RagClient(
        api_key=RAG_API_KEY,
        dep_ticket=RAG_DEP_TICKET,
        base_url=RAG_BASE_URL,
    )


def sanitize_query(query: str) -> str:
    cleaned = re.sub(r"[\x00-\x1F\x7F]", " ", query or "")
    cleaned = re.sub(r"[^\w\sㄱ-ㅎ가-힣]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _normalize_rag_indexes(indexes: Optional[List[str]]) -> List[str]:
    raw_indexes = indexes if indexes is not None else [x.strip() for x in RAG_INDEXES.split(",") if x.strip()]
    normalized: List[str] = []
    for item in raw_indexes:
        for index in str(item or "").split(","):
            cleaned = index.strip()
            if cleaned and cleaned not in normalized:
                normalized.append(cleaned)
    return normalized


def _compute_rag_fallback_top_k(top_k: int) -> int:
    capped_top_k = min(max(1, int(top_k)), RAG_API_MAX_NUM_RESULT_DOC)
    if capped_top_k <= 1:
        return 1
    return max(1, math.ceil(capped_top_k / 2))


def _log_rag_retrieve_event(
    *,
    stage: str,
    query: str,
    indexes: List[str],
    top_k: int,
    elapsed_sec: float,
    timeout_hit: bool,
    level: str = "INFO",
    detail: str = "",
):
    suffix = f" detail={detail}" if detail else ""
    print(
        f"[RAG Retrieve {level}] stage={stage} query={query!r} indexes={indexes} "
        f"top_k={top_k} elapsed_sec={elapsed_sec:.3f} timeout={timeout_hit}{suffix}"
    )


def search_rag_documents(
    query: str,
    indexes: Optional[List[str]] = None,
    *,
    top_k: Optional[int] = None,
    mode: Optional[str] = None,
    filter: Optional[Dict[str, Any]] = None,
    return_meta: bool = False,
) -> Any:
    """
    RAG 문서 검색 (다중 인덱스 지원)
    
    Args:
        query: 검색 쿼리
        indexes: 검색할 인덱스 목록 (None이면 기본 인덱스 사용)
    
    Returns:
        검색 결과 문서 목록
    """
    indexes = _normalize_rag_indexes(indexes)
    
    sanitized_query = sanitize_query(query)
    metadata: Dict[str, Any] = {
        "query": query,
        "sanitized_query": sanitized_query,
        "timeout_occurred": False,
        "fallback_used": False,
        "user_notice": "",
        "attempts": [],
        "effective_indexes": indexes[:],
        "effective_top_k": None,
    }
    if not sanitized_query:
        empty_result = {"documents": [], "metadata": metadata}
        return empty_result if return_meta else []

    print(f"[RAG Search] Query(raw): {query}")
    print(f"[RAG Search] Query(sanitized): {sanitized_query}")
    print(f"[RAG Search] Indexes: {indexes}")
    print(f"[RAG Search] Base URL: {RAG_BASE_URL}")
    requested_top_k = top_k or RAG_NUM_RESULT_DOC
    num_result_doc = min(max(1, int(requested_top_k)), RAG_API_MAX_NUM_RESULT_DOC)
    if num_result_doc != requested_top_k:
        print(f"[RAG Search] Num Result Doc capped: requested={requested_top_k} capped={num_result_doc} (api_max={RAG_API_MAX_NUM_RESULT_DOC})")
    else:
        print(f"[RAG Search] Num Result Doc: {num_result_doc}")
    
    rag_client = create_rag_client()
    all_results: List[Dict[str, Any]] = []
    primary_timeout_results: List[Dict[str, Any]] = []
    fallback_index = indexes[0] if indexes else ""
    fallback_top_k = _compute_rag_fallback_top_k(num_result_doc)
    attempts = [
        {"stage": "primary", "indexes": indexes[:], "top_k": num_result_doc},
    ]
    if fallback_index:
        attempts.append({"stage": "fallback", "indexes": [fallback_index], "top_k": fallback_top_k})

    for attempt in attempts:
        stage = attempt["stage"]
        stage_indexes = attempt["indexes"]
        stage_top_k = attempt["top_k"]
        stage_started_at = time.perf_counter()
        stage_results: List[Dict[str, Any]] = []
        stage_timeout = False

        for index in stage_indexes:
            request_started_at = time.perf_counter()
            try:
                print(f"[RAG Search] Searching index: {index} stage={stage}")
                result = rag_client.retrieve(
                    index_name=index,
                    query_text=sanitized_query,
                    mode=mode or RAG_RETRIEVE_MODE,
                    num_result_doc=stage_top_k,
                    permission_groups=[RAG_PERMISSION_GROUPS],
                    filter=filter,
                    bm25_boost=RAG_BM25_BOOST,
                    knn_boost=RAG_KNN_BOOST,
                )
                request_elapsed = time.perf_counter() - request_started_at
                _log_rag_retrieve_event(
                    stage=stage,
                    query=sanitized_query,
                    indexes=[index],
                    top_k=stage_top_k,
                    elapsed_sec=request_elapsed,
                    timeout_hit=False,
                )
                if "hits" in result and isinstance(result["hits"], dict):
                    hits = result["hits"].get("hits", [])
                    for hit in hits:
                        if "_source" in hit:
                            doc = hit["_source"]
                            doc["_index"] = index
                            doc["_score"] = hit.get("_score", 0)
                            stage_results.append(doc)
                    print(f"[RAG Search] Found {len(hits)} documents in {index}")
                else:
                    print(f"[RAG Search] No 'hits' field in response from {index}")
            except requests.exceptions.ReadTimeout as e:
                request_elapsed = time.perf_counter() - request_started_at
                stage_timeout = True
                metadata["timeout_occurred"] = True
                _log_rag_retrieve_event(
                    stage=stage,
                    query=sanitized_query,
                    indexes=[index],
                    top_k=stage_top_k,
                    elapsed_sec=request_elapsed,
                    timeout_hit=True,
                    level="WARN",
                    detail=str(e),
                )
                continue
            except Exception as e:
                request_elapsed = time.perf_counter() - request_started_at
                _log_rag_retrieve_event(
                    stage=stage,
                    query=sanitized_query,
                    indexes=[index],
                    top_k=stage_top_k,
                    elapsed_sec=request_elapsed,
                    timeout_hit=False,
                    level="ERROR",
                    detail=str(e),
                )
                import traceback
                traceback.print_exc()
                continue

        stage_elapsed = time.perf_counter() - stage_started_at
        metadata["attempts"].append({
            "stage": stage,
            "indexes": stage_indexes[:],
            "top_k": stage_top_k,
            "elapsed_sec": round(stage_elapsed, 3),
            "timeout_occurred": stage_timeout,
            "result_count": len(stage_results),
        })

        if stage == "primary" and stage_timeout and len(attempts) > 1:
            primary_timeout_results = stage_results[:]
            print(
                f"[RAG Search] ReadTimeout detected. retrying with narrowed scope: "
                f"indexes={[fallback_index]} top_k={fallback_top_k}"
            )
            metadata["fallback_used"] = True
            metadata["user_notice"] = "문서 검색 응답이 지연되어 범위를 줄여 재조회했습니다."
            continue

        if stage_results:
            all_results = stage_results
            metadata["effective_indexes"] = stage_indexes[:]
            metadata["effective_top_k"] = stage_top_k
            if stage == "fallback":
                metadata["fallback_used"] = True
                metadata["user_notice"] = "문서 검색 응답이 지연되어 범위를 줄여 재조회했습니다."
            break

        if not stage_timeout:
            metadata["effective_indexes"] = stage_indexes[:]
            metadata["effective_top_k"] = stage_top_k
            break

    if not all_results and primary_timeout_results:
        all_results = primary_timeout_results
        metadata["effective_indexes"] = indexes[:]
        metadata["effective_top_k"] = num_result_doc

    print(f"[RAG Search] Total results: {len(all_results)}")
    result_payload = {"documents": all_results, "metadata": metadata}
    return result_payload if return_meta else all_results


DATE_FIELD_CANDIDATES = [
    "created_time", "last_modified_time", "updated_time", "modified_time",
    "updated_at", "updated_date", "last_updated", "last_modified",
    "modified_at", "modified_date", "created_at", "created_date",
    "register_date", "reg_date", "date", "datetime", "timestamp",
    "mail_date", "page_updated_at", "page_created_at",
    "ingested_at", "ingest_at", "ingested_time", "indexed_at", "index_time",
]

INGESTED_DATE_FIELD_CANDIDATES = [
    "ingested_at", "ingest_at", "ingested_time", "indexed_at", "index_time",
    "inserted_at", "loaded_at", "etl_loaded_at",
]
def _truncate_text(s: str, max_chars: int = 2200) -> str:
    s = (s or "").strip()
    if len(s) <= max_chars:
        return s
    return s[:max_chars] + " ..."


def format_for_knox_text(text: str) -> str:
    """Convert markdown-ish output to messenger-friendly plain text."""
    t = (text or "").replace("\r\n", "\n")

    # Headers -> emoji section labels
    t = re.sub(r"(?m)^###\s*", "📌 ", t)
    t = re.sub(r"(?m)^##\s*", "📍 ", t)
    t = re.sub(r"(?m)^#\s*", "📍 ", t)

    # Bold/code markers removal
    t = t.replace("**", "")
    t = t.replace("__", "")
    t = t.replace("`", "")

    # Collapse excessive blank lines
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    return t
def _parse_doc_datetime_value(v: Any) -> Optional[datetime]:
    if v in (None, "", 0):
        return None
    if isinstance(v, (int, float)):
        try:
            ts = float(v)
            if ts > 1_000_000_000_000:  # ms
                ts = ts / 1000.0
            if ts > 0:
                return datetime.fromtimestamp(ts, tz=ZoneInfo("Asia/Seoul"))
        except Exception:
            pass
    s = str(v).strip()
    if not s:
        return None
    s_norm = s.replace("Z", "+00:00").replace("/", "-").replace(".", "-")
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y%m%d%H%M%S",
        "%Y%m%d%H%M",
        "%Y%m%d",
    ):
        try:
            dt = datetime.strptime(s_norm, fmt)
            return dt.replace(tzinfo=ZoneInfo("Asia/Seoul"))
        except Exception:
            pass
    try:
        dt = datetime.fromisoformat(s_norm)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("Asia/Seoul"))
        return dt
    except Exception:
        return None
def _extract_doc_datetime(doc: Dict[str, Any]) -> Optional[datetime]:
    if not isinstance(doc, dict):
        return None
    for key in DATE_FIELD_CANDIDATES:
        if key in doc:
            dt = _parse_doc_datetime_value(doc.get(key))
            if dt:
                return dt
    meta = doc.get("metadata")
    if isinstance(meta, dict):
        for key in DATE_FIELD_CANDIDATES:
            if key in meta:
                dt = _parse_doc_datetime_value(meta.get(key))
                if dt:
                    return dt
    src = doc.get("_source")
    if isinstance(src, dict):
        for key in DATE_FIELD_CANDIDATES:
            if key in src:
                dt = _parse_doc_datetime_value(src.get(key))
                if dt:
                    return dt
    for k, v in doc.items():
        lk = str(k).lower()
        if any(token in lk for token in ("date", "time", "updated", "modified", "created", "ts")):
            dt = _parse_doc_datetime_value(v)
            if dt:
                return dt
    return None


def _extract_doc_ingested_datetime(doc: Dict[str, Any]) -> Optional[datetime]:
    if not isinstance(doc, dict):
        return None
    for key in INGESTED_DATE_FIELD_CANDIDATES:
        if key in doc:
            dt = _parse_doc_datetime_value(doc.get(key))
            if dt:
                return dt
    meta = doc.get("metadata")
    if isinstance(meta, dict):
        for key in INGESTED_DATE_FIELD_CANDIDATES:
            if key in meta:
                dt = _parse_doc_datetime_value(meta.get(key))
                if dt:
                    return dt
    src = doc.get("_source")
    if isinstance(src, dict):
        for key in INGESTED_DATE_FIELD_CANDIDATES:
            if key in src:
                dt = _parse_doc_datetime_value(src.get(key))
                if dt:
                    return dt
    for k, v in doc.items():
        lk = str(k).lower()
        if any(token in lk for token in ("ingest", "index", "loaded", "etl")):
            dt = _parse_doc_datetime_value(v)
            if dt:
                return dt
    return _extract_doc_datetime(doc)


def _is_doc_nav_learning_query(question: str) -> bool:
    q = re.sub(r"\s+", "", (question or "").lower())
    return any(tok in q for tok in ("학습한문서", "학습문서", "금주에학습", "최근학습", "최신학습"))


def _is_doc_nav_title_only_query(question: str) -> bool:
    q = re.sub(r"\s+", "", (question or "").lower())
    return any(tok in q for tok in ("문서제목", "제목알려", "문서목록", "목록알려", "무슨문서", "어떤문서"))


def _get_week_range(base_dt: datetime, week_offset: int = 0) -> Tuple[datetime, datetime]:
    tz = ZoneInfo("Asia/Seoul")
    if base_dt.tzinfo is None:
        base_dt = base_dt.replace(tzinfo=tz)
    base_dt = base_dt.astimezone(tz)
    monday = (base_dt - timedelta(days=base_dt.weekday())).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    start = monday + timedelta(days=7 * week_offset)
    end = start + timedelta(days=7)
    return start, end


def _get_month_range(year: int, month: int) -> Optional[Tuple[datetime, datetime]]:
    if month < 1 or month > 12:
        return None
    tz = ZoneInfo("Asia/Seoul")
    start = datetime(year, month, 1, tzinfo=tz)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=tz)
    else:
        end = datetime(year, month + 1, 1, tzinfo=tz)
    return start, end


def _extract_time_range_from_question(question: str) -> Optional[Dict[str, Any]]:
    import re
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    q_raw = (question or "").strip()
    if not q_raw:
        return None

    # 공백 제거 버전(토큰 탐지용) / 원문(정규식용)
    q_compact = q_raw.replace(" ", "")
    now = datetime.now(ZoneInfo("Asia/Seoul"))

    def _mk(label: str, start: datetime, end: datetime) -> Dict[str, Any]:
        # end는 "포함"으로 쓰는 게 편하니, 23:59:59로 세팅(필요시 너희 RAG filter 형식에 맞춰 조정)
        if start.tzinfo is None:
            start = start.replace(tzinfo=ZoneInfo("Asia/Seoul"))
        if end.tzinfo is None:
            end = end.replace(tzinfo=ZoneInfo("Asia/Seoul"))
        return {
            "label": f"{label}({start.strftime('%Y-%m-%d')}~{end.strftime('%Y-%m-%d')})",
            "start": start,
            "end": end,
        }

    # 0) (옵션) YYYY-MM-DD ~ YYYY-MM-DD 같은 "명시 범위"가 있으면 최우선
    m = re.search(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})\s*[~\-]\s*(\d{4})[-./](\d{1,2})[-./](\d{1,2})", q_raw)
    if m:
        y1, mo1, d1, y2, mo2, d2 = map(int, m.groups())
        start = datetime(y1, mo1, d1, 0, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        end = datetime(y2, mo2, d2, 23, 59, 59, tzinfo=ZoneInfo("Asia/Seoul"))
        if start <= end:
            return _mk("지정기간", start, end)

    # 1) YYYY년 MM월 / 올해/작년 MM월 (최우선: 명시 월)
    m = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월", q_raw)
    if m:
        target_year = int(m.group(1))
        target_month = int(m.group(2))
        month_range = _get_month_range(target_year, target_month)
        if month_range:
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

    # 1-1) 연도 생략 월 표현: "3월", "3 월", "3월달", "3월 달"
    #      현재 연도 기준으로 보정
    m = re.search(r"(?<!\d)(\d{1,2})\s*월\s*(?:달)?(?!\d)", q_raw)
    if m:
        target_month = int(m.group(1))
        month_range = _get_month_range(now.year, target_month)
        if month_range:
            start, end = month_range
            return _mk(f"{now.year}년 {target_month}월", start, end)

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
    *,
    datetime_extractor: Optional[Callable[[Dict[str, Any]], Optional[datetime]]] = None,
) -> List[Dict[str, Any]]:
    extractor = datetime_extractor or _extract_doc_datetime
    filtered: List[Dict[str, Any]] = []
    for doc in documents:
        dt = extractor(doc)
        if not dt:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("Asia/Seoul"))
        dt = dt.astimezone(ZoneInfo("Asia/Seoul"))
        if start_dt <= dt < end_dt:
            filtered.append(doc)
    return filtered


def rerank_rag_documents(
    documents: List[Dict[str, Any]],
    prefer_recent: bool = False,
    *,
    datetime_extractor: Optional[Callable[[Dict[str, Any]], Optional[datetime]]] = None,
    recency_boost: float = 1.0,
) -> List[Dict[str, Any]]:
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
    extractor = datetime_extractor or _extract_doc_datetime
    for d in docs:
        vec = float(d.get("_vector_score") or 0.0)
        vec_norm = vec / max_vec if max_vec > 0 else 0.0
        dt = extractor(d)
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
        effective_recency_weight = max(0.0, min(0.9, RAG_RECENCY_WEIGHT * max(recency_boost, 0.1)))
        combined_score = ((1 - effective_recency_weight) * vec_norm) + (effective_recency_weight * recency_score) + query_hit_bonus
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

def get_dynamic_similarity_threshold(question: str, time_range: Optional[Dict[str, Any]]) -> float:
    threshold = max(RAG_SIMILARITY_THRESHOLD, RAG_MIN_COMBINED_SCORE)
    # 기간/최신 의도에서는 더 공격적으로 완화 (최저 0.20)
    if time_range or should_prioritize_recent_docs(question):
        threshold = max(0.20, threshold - 0.15)
    return threshold


def is_rag_result_relevant(
    question: str,
    top_docs: List[Dict[str, Any]],
    *,
    time_range: Optional[Dict[str, Any]] = None,
) -> bool:
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

    effective_threshold = get_dynamic_similarity_threshold(question, time_range)
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


def get_dynamic_recency_boost(question: str, time_range: Optional[Dict[str, Any]]) -> float:
    """모든 질의에 기본 recent boost를 적용하고, 시간/최신 의도에서는 추가 가중."""
    boost = 1.3  # 기본값: 기간 미지정 질의도 최신 문서를 우선
    if should_prioritize_recent_docs(question):
        boost += 0.35
    if time_range:
        boost += 0.25
    return min(boost, 1.9)


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


def build_doc_nav_answer(
    *,
    question: str,
    documents: List[Dict[str, Any]],
    max_docs: int = 10,
    learning_based: bool = False,
) -> str:
    docs = list(documents or [])[:max_docs]
    if not docs:
        return "\n".join(
            [
                "📌 한줄 요약",
                "- 조건에 맞는 문서를 찾지 못했습니다.",
                "",
                "📂 문서 목록(최신순)",
                "- 조회 결과가 없습니다.",
            ]
        )

    title_only = _is_doc_nav_title_only_query(question)
    date_label = "학습일시" if learning_based else "문서일시"
    lines: List[str] = []
    for idx, doc in enumerate(docs, 1):
        title = str(doc.get("title") or doc.get("doc_id") or "제목 없음").strip()
        doc_date = str(doc.get("_doc_date") or "날짜 정보 없음")
        url = str(doc.get("confluence_mail_page_url") or doc.get("url") or "").strip()
        line = f"- {idx}) {title} | {date_label}: {doc_date}"
        if url:
            line += f" | 링크: {url}"
        lines.append(line)

    summary_label = "학습 문서 제목" if learning_based else "문서 제목"
    answer = [
        "📌 한줄 요약",
        f"- 최신순 {summary_label} {len(docs)}건입니다.",
        "",
        "📂 문서 목록(최신순)",
        *lines,
    ]
    if not title_only:
        answer.extend(
            [
                "",
                "💡 참고",
                "- 제목 중심으로 정렬해 제공했습니다. 필요하면 특정 문서 내용을 이어서 요약해드릴 수 있습니다.",
            ]
        )
    return "\n".join(answer)


def retrieve_rag_documents_parallel(
    queries: List[str],
    *,
    top_k: int,
    indexes: Optional[List[str]] = None,
    return_meta: bool = False,
) -> Any:
    query_list = [q.strip() for q in queries if q and q.strip()]
    if not query_list:
        empty_payload = {"documents": [], "metadata": {"queries": [], "timeout_occurred": False, "fallback_used": False, "user_notice": ""}}
        return empty_payload if return_meta else []

    all_documents: List[Dict[str, Any]] = []
    aggregated_meta: Dict[str, Any] = {
        "queries": [],
        "timeout_occurred": False,
        "fallback_used": False,
        "user_notice": "",
    }
    max_workers = min(len(query_list), MAX_RAG_QUERIES, 2)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                search_rag_documents,
                query,
                indexes=indexes,
                top_k=top_k,
                mode=RAG_RETRIEVE_MODE,
                return_meta=True,
            ): query
            for query in query_list
        }
        for future in as_completed(future_map):
            query = future_map[future]
            try:
                result = future.result()
                docs = result.get("documents", [])
                metadata = result.get("metadata", {})
                print(f"[RAG] 병렬 검색 완료: query={query} docs={len(docs)}")
                all_documents.extend(docs)
                aggregated_meta["queries"].append(metadata)
                if metadata.get("timeout_occurred"):
                    aggregated_meta["timeout_occurred"] = True
                if metadata.get("fallback_used"):
                    aggregated_meta["fallback_used"] = True
                if metadata.get("user_notice"):
                    aggregated_meta["user_notice"] = str(metadata.get("user_notice"))
            except Exception as e:
                print(f"[RAG] 병렬 검색 실패: query={query} err={e}")

    payload = {"documents": all_documents, "metadata": aggregated_meta}
    return payload if return_meta else all_documents


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
        payload = resp if isinstance(resp, dict) else {}
        expected_msg_id = payload.get("_request_msg_id")
        mid, st = extract_msgid_senttime_for_expected(payload, expected_msg_id)
        if mid is None or st is None:
            return
        with llm_notice_lock:
            llm_notice_state.setdefault(req_id, []).append((int(mid), int(st)))
    except Exception as e:
        print(f"[LLM][{req_id}] notice register failed: {e}")


def extract_msgid_senttime_for_expected(resp: dict, expected_msg_id: Any):
    if expected_msg_id is None:
        return None, None

    def _match_entry(entry: Any):
        if not isinstance(entry, dict):
            return None, None
        mid = entry.get("msgId") or entry.get("messageId") or entry.get("msgID")
        st = entry.get("sentTime") or entry.get("sendTime") or entry.get("sent_time")
        if mid is None or st is None:
            return None, None
        try:
            if int(mid) != int(expected_msg_id):
                return None, None
            return int(mid), int(st)
        except Exception:
            if str(mid) != str(expected_msg_id):
                return None, None
            return mid, st

    if not isinstance(resp, dict):
        return None, None

    pme = resp.get("processedMessageEntries")
    if isinstance(pme, list):
        for entry in pme:
            mid, st = _match_entry(entry)
            if mid is not None and st is not None:
                return mid, st

    for k in ("chatReplyResultList", "chatReplyResults", "resultList", "data", "results"):
        v = resp.get(k)
        if isinstance(v, list):
            for entry in v:
                mid, st = _match_entry(entry)
                if mid is not None and st is not None:
                    return mid, st

    return _match_entry(resp)


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
        return

    with llm_task_state_lock:
        if llm_workers_started:
            return
        for idx in range(LLM_WORKERS):
            threading.Thread(
                target=llm_worker_loop,
                args=(f"llm-worker-{idx + 1}",),
                daemon=True,
                name=f"llm-worker-{idx + 1}",
            ).start()
        llm_workers_started = True



def _strip_time_tokens_for_search(question: str) -> str:
    q = (question or "")
    # 연/월/주/최근 표현 제거하여 주제 토큰 중심 검색 질의를 추가 생성
    q = re.sub(r"\d{4}\s*년", " ", q)
    q = re.sub(r"(?<!\d)\d{1,2}\s*월(?:\s*달)?", " ", q)
    q = re.sub(r"(이번주|금주|저번주|지난주|전주|이번달|저번달|지난달|전월|최근\s*\d*\s*(일|주|개월|달)?)", " ", q)
    q = re.sub(r"\s+", " ", q).strip()
    return q


def generate_deterministic_query_variants(question: str) -> List[str]:
    """LLM 호출 없이 붙여쓰기/표현 차이를 보정하는 검색 질의 변형"""
    base = normalize_query_for_search(question)
    if not base:
        return []

    variants: List[str] = []
    q = re.sub(r"\s+", " ", base).strip()

    # "EDP 주요 이슈 정리"처럼 조직/도메인 + 주요 이슈 패턴은
    # 문서에 자주 등장하는 "파트"를 보강한 질의를 함께 검색
    if "주요" in q and "이슈" in q and "정리" in q and "파트" not in q:
        parts = q.split()
        if parts:
            lead = parts[0]
            if re.fullmatch(r"[A-Za-z0-9]{2,10}", lead):
                variants.append(f"{lead} 파트 " + " ".join(parts[1:]))

    # 월 표현 확장: "3월" -> "3.4"/"3/4" 같은 제목 패턴과 결합되도록 월 숫자 토큰 보강
    month_match = re.search(r"(?<!\d)(\d{1,2})\s*월(?:\s*달)?(?!\d)", question or "")
    if month_match:
        m = int(month_match.group(1))
        if 1 <= m <= 12:
            variants.append(f"{m}. {q}")
            variants.append(f"{m}월 {q}")
            variants.append(f"{m}월달 {q}")

    return [v for v in variants if v and v != base]
def build_search_queries(question: str, llm: ChatOpenAI, *, memory_text: str = "", use_memory_for_rewrite: bool = False) -> List[str]:
    normalized = normalize_query_for_search(question)
    sanitized_original = sanitize_query(normalized)
    if not sanitized_original:
        return []

    # rewrite 비활성화 시 LLM 전처리 호출 없이 normalized 원문 1개만 사용
    if not ENABLE_QUERY_REWRITE:
        return [sanitized_original]

    queries: List[str] = []
    if RAG_INCLUDE_ORIGINAL_QUERY:
        queries.append(sanitized_original)

    # 기간 토큰 제거 변형(시간 필터와 검색 질의 분리)
    stripped_time_query = _strip_time_tokens_for_search(question)
    if stripped_time_query:
        sq = sanitize_query(normalize_query_for_search(stripped_time_query))
        if sq and sq != sanitized_original and sq not in queries:
            queries.append(sq)

    # LLM 호출 없이 deterministic 변형을 먼저 추가 (ex. "EDP" -> "EDP 파트")
    deterministic = generate_deterministic_query_variants(question)
    for item in deterministic:
        sq = sanitize_query(item)
        if sq and sq not in queries:
            queries.append(sq)

    # rewrite 비활성화 시 LLM 전처리 호출 없이 종료
    if not ENABLE_QUERY_REWRITE:
        return (queries or [sanitized_original])[:MAX_RAG_QUERIES]

    if len(sanitized_original) > 12 and len(queries) < MAX_RAG_QUERIES:
        rewritten = rewrite_search_queries(question, llm, memory_text=memory_text, use_memory=use_memory_for_rewrite)
        for item in rewritten:
            sq = sanitize_query(normalize_query_for_search(item))
            if sq and sq not in queries:
                queries.append(sq)
            if len(queries) >= MAX_RAG_QUERIES:
                break

    if not queries:
        queries = [sanitized_original]
    return queries[:MAX_RAG_QUERIES]


def _process_llm_chat_background_impl(task: Dict[str, Any]) -> Dict[str, Any]:
    chatroom_id = int(task["chatroom_id"])
    chat_type = (task.get("chat_type") or "").upper()
    scope_id = str(task.get("scope_id") or chatroom_id)
    question = (task.get("question") or "").strip()
    force_mode = (task.get("force_mode") or "").strip().lower()
    force_sql_mode = force_mode == "sql"
    force_glossary_mode = force_mode == "glossary"
    sender_knox = task.get("sender_knox") or ""
    sender_name = task.get("sender_name") or ""
    request_id = str(task.get("request_id") or "")
    stats = {
        "rag_calls": 0,
        "llm_calls": 0,
        "used_rag": False,
        "fallback_reason": "",
        "memory_hit": False,
        "memory_message_count": 0,
        "memory_prompt_chars": 0,
        "rewrite_used_memory": False,
    }
    perf = {"t0": time.perf_counter()}
    answer = ""
    success_flag = 0
    effective_question = question
    normalized_query = ""
    final_intent = ""
    sql_used = False
    sql_registry_id = ""
    rag_selected_domain = "none"
    selected_docs: List[Dict[str, Any]] = []
    weekly_debug: Dict[str, Any] = {}
    search_queries: List[str] = []
    rag_retrieval_meta: Dict[str, Any] = {}

    def _send_answer_with_feedback_card(answer_text: str):
        nonlocal success_flag
        chatBot.send_text(chatroom_id, f"🤖 {format_for_knox_text(answer_text)}")
        success_flag = 1
        if ENABLE_FEEDBACK_CARD and request_id:
            try:
                chatBot.send_adaptive_card(chatroom_id, ui.build_feedback_card(request_id))
            except Exception as feedback_err:
                print(f"[FEEDBACK] feedback card send failed request_id={request_id} err={feedback_err}")

    try:
        user_id = sender_knox if sender_knox else "bot"
        llm = create_llm_chatbot(user_id)
        perf["llm_init_ms"] = (time.perf_counter() - perf["t0"]) * 1000

        memory_messages = load_conversation_memory(scope_id=scope_id, chat_type=chat_type)
        memory_text = build_memory_text(memory_messages)
        memory_hit = bool(memory_messages)
        use_memory_for_rewrite = memory_hit and _is_context_dependent_question(question)
        memory_chars = len(memory_text)
        stats["memory_hit"] = memory_hit
        stats["memory_message_count"] = len(memory_messages)
        stats["memory_prompt_chars"] = memory_chars
        stats["rewrite_used_memory"] = use_memory_for_rewrite
        print(f"[MEMORY] hit={memory_hit} message_count={len(memory_messages)} prompt_chars={memory_chars} use_in_rewrite={use_memory_for_rewrite}")
        perf["memory_ms"] = (time.perf_counter() - perf["t0"]) * 1000

        prefer_general = should_prefer_general_llm(question) and not (force_sql_mode or force_glossary_mode)
        time_range = _extract_time_range_from_question(question)
        effective_question, ctx_state = _build_effective_question(question, scope_id=scope_id, time_range=time_range)
        issue_summary_intent = is_issue_summary_intent(effective_question)

        normalized_query = normalize_query_for_search(effective_question)
        glossary_intent = is_glossary_intent(effective_question) if force_glossary_mode else False
        force_glossary = force_glossary_mode
        configure_sql_intent_llm_classifier(
            lambda q, ctx: classify_sql_intent_with_llm(llm, q, ctx)
        )
        sql_match = find_best_sql_registry_match(effective_question) if force_sql_mode else None
        sql_nlu_trace = get_last_sql_nlu_trace() if force_sql_mode else {}
        if force_sql_mode and sql_nlu_trace:
            print(f"[SQL_NLU] original={sql_nlu_trace.get('original_question')!r}")
            print(f"[SQL_NLU] normalized={sql_nlu_trace.get('normalized_question')!r}")
            print(f"[SQL_NLU] slots={sql_nlu_trace.get('final_slots') or sql_nlu_trace.get('slots')}")
            print(f"[SQL_NLU] rule_intent={sql_nlu_trace.get('rule_intent')} final_intent={sql_nlu_trace.get('final_intent')}")
            if sql_nlu_trace.get("llm_used"):
                print(f"[SQL_NLU] llm_intent_result={sql_nlu_trace.get('llm_intent_result')}")
            print(f"[SQL_NLU] selected_query_id={sql_nlu_trace.get('selected_query_id')}")
            print(f"[SQL_NLU] period={sql_nlu_trace.get('resolved_period')}")
            print(f"[SQL_NLU] period_inferred={sql_nlu_trace.get('period_inferred')} reason={sql_nlu_trace.get('period_infer_reason')}")
            print(f"[SQL_NLU] fallback_used={sql_nlu_trace.get('fallback_used')}")
        if sql_match:
            sql_registry_id = str(sql_match.item.id)
            print(f"[SQL_REGISTRY] matched={sql_match.item.id} score={sql_match.score:.2f}")
        else:
            print("[SQL_REGISTRY] matched=None")
        if force_sql_mode:
            final_intent = "data_only"
        elif force_glossary_mode:
            final_intent = "rag_only"
        else:
            final_intent = classify_query_intent(
                question,
                effective_question,
                sql_match,
                prefer_general=prefer_general,
                issue_summary_intent=issue_summary_intent,
                glossary_intent=glossary_intent,
            )
        print(
            f"[INTENT] force_mode={force_mode or 'none'} "
            f"sql_match={(sql_match.item.id if sql_match else 'none')} final={final_intent}"
        )
        route = build_route_decision(final_intent, sql_match)
        sql_summary = None
        sql_summary_text = ""
        sql_used = False

        if force_sql_mode and not sql_match:
            answer = (
                "📌 한줄 요약\n"
                "- /sql 질문으로 처리했지만 실행 가능한 plan을 만들지 못했습니다.\n\n"
                "💡 참고\n"
                "- 현재 /sql은 metadata-driven planner/builder를 우선 사용하고, 필요 시 legacy queries로 fallback 합니다.\n"
                "- 예: /sql 2월 vh 판매 알려줘"
            )
            _send_answer_with_feedback_card(answer)
            save_conversation_memory(
                scope_id=scope_id,
                room_id=str(chatroom_id),
                user_id=sender_knox,
                role="assistant",
                content=answer,
                chat_type=chat_type,
            )
            return stats

        if route.use_sql and sql_match:
            if force_sql_mode:
                final_slots = (sql_nlu_trace.get("final_slots") or sql_match.slots or {}).copy()
                final_sql_intent = str(sql_nlu_trace.get("final_intent") or sql_match.intent or "sales_total")
                period_ctx = dict(sql_nlu_trace.get("resolved_period") or sql_match.period or {})
                selected_qid = str(sql_nlu_trace.get("selected_query_id") or sql_match.item.id)
                plan = build_execution_plan(effective_question, final_sql_intent, final_slots, selected_qid)
                print(f"[SQL_PLAN] steps={[{'query_id': s.query_id, 'role': s.role, 'reason': s.reason} for s in plan]}")

                execution_results: List[Dict[str, Any]] = []
                primary_error = None
                primary_missing: List[str] = []

                for step in plan:
                    step_match = build_match_for_query_id(
                        step.query_id,
                        slots=final_slots,
                        period=period_ctx,
                        intent=final_sql_intent,
                        llm_used=bool(sql_nlu_trace.get("llm_used")),
                        fallback_used=bool(sql_nlu_trace.get("fallback_used")),
                    )
                    if step_match is None:
                        print(f"[SQL_PLAN] skip missing_query_id={step.query_id}")
                        continue

                    step_exec = execute_sql_match(step_match, question=effective_question, run_oracle_query=run_oracle_query)
                    step_df = step_exec.get("df")
                    step_rows = len(step_df.index) if isinstance(step_df, pd.DataFrame) else 0
                    print(
                        f"[SQL_EXEC] query_id={step.query_id} role={step.role} rows={step_rows} "
                        f"elapsed_ms={step_exec.get('elapsed_ms', 0)} ok={step_exec.get('ok')}"
                    )

                    if step_exec.get("ok"):
                        execution_results.append(
                            {
                                "query_id": step.query_id,
                                "role": step.role,
                                "reason": step.reason,
                                "df": step_exec.get("df"),
                                "params": step_exec.get("params") or {},
                                "result_mode": step_exec.get("result_mode"),
                                "result_field": step_exec.get("result_field"),
                            }
                        )
                    else:
                        if step.role == "primary":
                            primary_error = step_exec.get("error")
                            primary_missing = step_exec.get("missing_params") or []
                            break
                        print(f"[SQL_PLAN] aux_failed query_id={step.query_id} error={step_exec.get('error')}")

                if primary_error and not execution_results:
                    if primary_missing:
                        missing_line = ", ".join(primary_missing)
                        answer = (
                            "📌 한줄 요약\n"
                            "- /sql 실행에 필요한 필수 파라미터가 부족합니다.\n\n"
                            "💡 참고\n"
                            f"- 누락 파라미터: {missing_line}\n"
                            "- 가능한 범위에서 기본값 보정을 시도했지만 해석이 어려웠습니다."
                        )
                    else:
                        answer = (
                            "📌 한줄 요약\n"
                            "- /sql 실행 중 오류가 발생했습니다.\n\n"
                            "💡 참고\n"
                            f"- 오류: {primary_error}"
                        )
                    _send_answer_with_feedback_card(answer)
                    save_conversation_memory(
                        scope_id=scope_id,
                        room_id=str(chatroom_id),
                        user_id=sender_knox,
                        role="assistant",
                        content=answer,
                        chat_type=chat_type,
                    )
                    return stats

                if final_intent == "data_only":
                    sql_used = True
                    rule_answer = render_answer_rule_based(
                        effective_question,
                        intent=final_sql_intent,
                        slots=final_slots,
                        period=period_ctx,
                        results=execution_results,
                        period_infer_reason=str(sql_nlu_trace.get("period_infer_reason") or ""),
                    )
                    answer = rule_answer
                    if SQL_ANSWER_LLM_ENABLE:
                        llm_answer = render_answer_with_llm(
                            llm_render_fn=lambda prompt: render_sql_answer_with_llm(llm, prompt),
                            question=effective_question,
                            intent=final_sql_intent,
                            slots=final_slots,
                            period=period_ctx,
                            results=execution_results,
                            period_infer_reason=str(sql_nlu_trace.get("period_infer_reason") or ""),
                        )
                        if llm_answer:
                            answer = llm_answer
                            print("[SQL_ANSWER] renderer=llm")
                        else:
                            print("[SQL_ANSWER] renderer=rule_fallback")
                    else:
                        print("[SQL_ANSWER] renderer=rule_only")

                    print("[HYBRID] sql_used=True rag_used=False")
                    print("[ANSWER] mode=data_only llm_used=False")
                    _send_answer_with_feedback_card(answer)
                    save_conversation_memory(
                        scope_id=scope_id,
                        room_id=str(chatroom_id),
                        user_id=sender_knox,
                        role="assistant",
                        content=answer,
                        chat_type=chat_type,
                    )
                    return stats
            else:
                sql_exec = execute_sql_match(sql_match, question=effective_question, run_oracle_query=run_oracle_query)
                sql_df = sql_exec.get("df")
                sql_rows = len(sql_df.index) if isinstance(sql_df, pd.DataFrame) else 0
                print(
                    f"[SQL_EXEC] runner={sql_exec.get('runner')} rows={sql_rows} "
                    f"elapsed_ms={sql_exec.get('elapsed_ms', 0)} ok={sql_exec.get('ok')}"
                )
                if sql_exec.get("ok"):
                    sql_used = True
                    sql_summary = summarize_sql_result(
                        sql_exec["df"],
                        result_mode=str(sql_exec.get("result_mode") or "table"),
                        result_field=str(sql_exec.get("result_field") or ""),
                        empty_message=str(sql_exec.get("empty_message") or "조회 결과가 없습니다."),
                    )
                    sql_summary_text = "\n".join(sql_summary.get("bullets") or [])
                    print(f"[SQL_RESULT] summary_chars={len(sql_summary_text)}")
                else:
                    print(f"[SQL_EXEC] failed runner={sql_exec.get('runner')} error={sql_exec.get('error')}")

        if final_intent == "general_llm":
            from langchain_core.messages import SystemMessage, HumanMessage

            fallback_system_prompt = """
당신은 GOC 업무 지원 챗봇입니다.
이번 질문은 일반 지식/실시간 성격의 질문으로 판단하여 문서 검색 없이 일반 LLM 답변으로 안내합니다.
과도한 추측은 피하고, 불확실한 내용은 단정하지 마세요.
"""
            messages = [SystemMessage(content=fallback_system_prompt)]
            if memory_text:
                messages.append(HumanMessage(content=f"[최근 대화 메모리]\n{memory_text}"))
            messages.append(HumanMessage(content=question))
            t_llm = time.perf_counter()
            response = llm_invoke_with_retry(llm, messages, attempts=1, base_delay=1.5)
            perf["llm_ms"] = (time.perf_counter() - t_llm) * 1000
            stats["llm_calls"] += 1
            stats["fallback_reason"] = "prefer_general"
            answer = "📋 문서 기반 답변 미적용\n- 일반 지식/실시간 성격의 질문으로 판단했습니다.\n- 아래는 일반 LLM 답변입니다.\n\n" + response.content.strip()
            print("[ANSWER] mode=general_llm llm_used=True")
            _send_answer_with_feedback_card(answer)
            save_conversation_memory(
                scope_id=scope_id,
                room_id=str(chatroom_id),
                user_id=sender_knox,
                role="assistant",
                content=answer,
                chat_type=chat_type,
            )
            return stats

        print(f"[RAG] original question={question}")
        print(f"[RAG] effective question={effective_question}")
        print(f"[RAG] normalized query={normalized_query}")
        print(f"[RAG] glossary_intent={glossary_intent}")
        print(f"[RAG] force_glossary={force_glossary}")
        doc_nav_mode = final_intent == "doc_nav"
        doc_summary_mode = final_intent == "doc_summary"
        doc_nav_learning_mode = doc_nav_mode and _is_doc_nav_learning_query(effective_question)

        t_rewrite = time.perf_counter()
        search_queries = build_search_queries(effective_question, llm, memory_text=memory_text, use_memory_for_rewrite=use_memory_for_rewrite)
        weekly_issue_query = detect_weekly_issue_query(effective_question)
        weekly_topic = detect_weekly_topic(effective_question)
        week_label = ""
        if weekly_issue_query:
            week_label = compute_target_week_label(datetime.now(ZoneInfo("Asia/Seoul")), effective_question)
            weekly_queries = build_weekly_search_query_variants(effective_question, week_label, weekly_topic)
            for wq in weekly_queries:
                sq = sanitize_query(normalize_query_for_search(wq))
                if sq and sq not in search_queries:
                    search_queries.append(sq)
            search_queries = search_queries[: max(MAX_RAG_QUERIES, 4)]
            print(f"[WEEKLY_RAG] target_week={week_label} topic={weekly_topic or 'none'}")
        weekly_debug = {
            "weekly_issue_query": weekly_issue_query,
            "detected_topic": weekly_topic,
            "week_token": week_label,
            "search_queries": search_queries[:8],
            "extracted_period_label": (time_range or {}).get("label") if time_range else "",
        }
        perf["rewrite_ms"] = (time.perf_counter() - t_rewrite) * 1000
        print(f"[RAG] search queries: {search_queries}")
        strong_mail_intent = has_strong_mail_intent(effective_question)
        prefer_recent_docs = bool(time_range) or should_prioritize_recent_docs(effective_question) or issue_summary_intent
        retrieve_top_k = RAG_NUM_RESULT_DOC
        if time_range:
            retrieve_top_k = max(RAG_NUM_RESULT_DOC, RAG_TEMPORAL_NUM_RESULT_DOC)
        elif issue_summary_intent:
            retrieve_top_k = max(RAG_NUM_RESULT_DOC, RAG_API_MAX_NUM_RESULT_DOC)

        target_indexes = None
        if force_glossary:
            target_indexes = [GLOSSARY_INDEX_NAME]
        elif strong_mail_intent or issue_summary_intent:
            # 이슈/요약류 질문은 glossary 잡음을 줄이기 위해 메일 인덱스 우선
            target_indexes = [MAIL_INDEX_NAME]

        print(
            f"[RAG] prefer_recent_docs={prefer_recent_docs} issue_summary_intent={issue_summary_intent} "
            f"strong_mail_intent={strong_mail_intent} time_range={(time_range or {}).get('label') if time_range else 'none'} "
            f"top_k={retrieve_top_k} indexes={target_indexes or 'default'}"
        )

        t_rag = time.perf_counter()
        rag_retrieval = retrieve_rag_documents_parallel(
            search_queries,
            top_k=retrieve_top_k,
            indexes=target_indexes,
            return_meta=True,
        )
        all_rag_documents = rag_retrieval.get("documents", [])
        rag_retrieval_meta = rag_retrieval.get("metadata", {})
        perf["rag_fetch_ms"] = (time.perf_counter() - t_rag) * 1000
        stats["rag_calls"] = len(search_queries)
        if rag_retrieval_meta.get("fallback_used"):
            stats["fallback_reason"] = "rag_read_timeout_fallback"

        all_mail_docs = [d for d in all_rag_documents if d.get("_index") == MAIL_INDEX_NAME]
        all_glossary_docs = [d for d in all_rag_documents if d.get("_index") == GLOSSARY_INDEX_NAME]
        if force_glossary:
            all_mail_docs = []

        if time_range and all_mail_docs:
            time_extractor = _extract_doc_ingested_datetime if doc_nav_learning_mode else _extract_doc_datetime
            ranged_mail_docs = _filter_docs_by_datetime_range(
                all_mail_docs,
                time_range["start"],
                time_range["end"],
                datetime_extractor=time_extractor,
            )
            if ranged_mail_docs:
                print(
                    f"[RAG] 메일 기간 필터 적용: {time_range['label']} "
                    f"{time_range['start']}~{time_range['end']} docs={len(ranged_mail_docs)} "
                    f"date_basis={'ingested_at' if doc_nav_learning_mode else 'doc_date'}"
                )
                all_mail_docs = ranged_mail_docs
            else:
                # 시간의도 질문(이번주/최근/최신)에서는 원본으로 되돌리지 않고,
                # 우선 확장 기간(직전 14일)으로 1회 재시도 후 없으면 빈 결과를 유지.
                expanded_start = time_range["start"] - timedelta(days=14)
                expanded_mail_docs = _filter_docs_by_datetime_range(
                    all_mail_docs,
                    expanded_start,
                    time_range["end"],
                    datetime_extractor=time_extractor,
                )
                if expanded_mail_docs:
                    print(
                        f"[RAG] 메일 기간 확장 필터 적용: {expanded_start}~{time_range['end']} "
                        f"docs={len(expanded_mail_docs)}"
                    )
                    all_mail_docs = expanded_mail_docs
                else:
                    print(
                        f"[RAG] 메일 기간 필터 결과 없음(확장 포함). 원본 fallback 비활성: "
                        f"{time_range['label']} {time_range['start']}~{time_range['end']}"
                    )
                    all_mail_docs = []

        # 시간 필터 적용 후에는 mail/glossary 분리본 기준으로 전체 문서 후보도 재구성
        # (기간의도 질의에서 원본 all_rag_documents가 다시 섞이며 과거 문서가 역전되는 현상 방지)
        all_rag_documents = [*all_mail_docs, *all_glossary_docs]

        t_rerank = time.perf_counter()
        mail_datetime_extractor = _extract_doc_ingested_datetime if doc_nav_learning_mode else _extract_doc_datetime
        dynamic_recency_boost = get_dynamic_recency_boost(effective_question, time_range)
        reranked_mail_docs = rerank_rag_documents(
            all_mail_docs,
            prefer_recent=prefer_recent_docs,
            datetime_extractor=mail_datetime_extractor,
            recency_boost=dynamic_recency_boost,
        )[:RAG_NUM_RESULT_DOC]
        reranked_glossary_docs = rerank_rag_documents(
            all_glossary_docs,
            prefer_recent=prefer_recent_docs,
            recency_boost=dynamic_recency_boost,
        )[:RAG_NUM_RESULT_DOC]
        if weekly_debug.get("weekly_issue_query"):
            reranked_mail_docs = rerank_weekly_issue_docs(
                effective_question,
                reranked_mail_docs,
                str(weekly_debug.get("week_token") or ""),
                str(weekly_debug.get("detected_topic") or ""),
            )
            target_week = str(weekly_debug.get("week_token") or "")
            exact_week_docs = 0
            for doc in reranked_mail_docs:
                if target_week and (doc.get("_weekly_exact_week_match") or target_week in extract_week_tokens_from_doc(doc)):
                    exact_week_docs += 1
            recent_fallback_used = False
            if target_week and exact_week_docs == 0:
                target_num = int(target_week.replace("W", ""))
                fallback_docs = []
                for doc in reranked_mail_docs:
                    tokens = extract_week_tokens_from_doc(doc)
                    if not tokens:
                        continue
                    for token in tokens:
                        try:
                            token_num = int(token.replace("W", ""))
                        except Exception:
                            continue
                        if abs(token_num - target_num) <= 2:
                            fallback_docs.append(doc)
                            break
                if fallback_docs:
                    reranked_mail_docs = fallback_docs + [d for d in reranked_mail_docs if d not in fallback_docs]
                    recent_fallback_used = True
            weekly_debug["exact_week_match_count"] = exact_week_docs
            weekly_debug["recent_fallback_used"] = recent_fallback_used
            weekly_debug["rerank_reason_summary"] = [
                summarize_rerank_reason(d) for d in reranked_mail_docs[:3]
            ]
            print(
                f"[WEEKLY_RAG] target_week={target_week or 'none'} "
                f"exact_week_docs={exact_week_docs} fallback_recent_docs={1 if recent_fallback_used else 0}"
            )
        perf["rerank_ms"] = (time.perf_counter() - t_rerank) * 1000
        mail_docs = reranked_mail_docs[:RAG_CONTEXT_DOCS]
        glossary_docs = reranked_glossary_docs[:max(RAG_CONTEXT_DOCS, GLOSSARY_TOPK_MATCH)]

        selected_rag_domain = "none"
        glossary_match = False
        mail_match = False
        skip_rag = False
        rag_relevant = False
        rag_context = ""
        selected_docs: List[Dict[str, Any]] = []

        if GLOSSARY_RAG_ENABLE and force_glossary and glossary_docs:
            glossary_match = is_glossary_result_relevant(
                effective_question,
                glossary_docs,
                topk=GLOSSARY_TOPK_MATCH,
                min_score=GLOSSARY_THRESHOLD,
            )
            if glossary_match:
                selected_rag_domain = "glossary"
                rag_relevant = True
                selected_docs = glossary_docs[:RAG_CONTEXT_DOCS]
                rag_context = format_rag_context(selected_docs, max_docs=RAG_CONTEXT_DOCS)
        elif mail_docs and is_rag_result_relevant(effective_question, mail_docs, time_range=time_range):
            selected_rag_domain = "mail"
            mail_match = True
            rag_relevant = True
            selected_docs = mail_docs
            rag_context = format_rag_context(mail_docs, max_docs=RAG_CONTEXT_DOCS)
        elif GLOSSARY_RAG_ENABLE and glossary_intent and glossary_docs:
            glossary_match = is_glossary_result_relevant(
                effective_question,
                glossary_docs,
                topk=GLOSSARY_TOPK_MATCH,
                min_score=GLOSSARY_THRESHOLD,
            )
            if glossary_match:
                selected_rag_domain = "glossary"
                rag_relevant = True
                selected_docs = glossary_docs[:RAG_CONTEXT_DOCS]
                rag_context = format_rag_context(selected_docs, max_docs=RAG_CONTEXT_DOCS)
        else:
            combined_docs = rerank_rag_documents(all_rag_documents, prefer_recent=prefer_recent_docs)[:RAG_NUM_RESULT_DOC]
            top_docs = combined_docs[:RAG_CONTEXT_DOCS]
            top_score = float(top_docs[0].get("_combined_score") or 0.0) if top_docs else 0.0
            dynamic_threshold = get_dynamic_similarity_threshold(effective_question, time_range)
            skip_rag = top_score < dynamic_threshold
            rag_relevant = (not skip_rag) and is_rag_result_relevant(effective_question, top_docs, time_range=time_range)
            if rag_relevant:
                selected_docs = top_docs
                rag_context = format_rag_context(top_docs, max_docs=RAG_CONTEXT_DOCS)

        print(
            f"[RAG Domain Selection] selected_rag_domain={selected_rag_domain}, "
            f"glossary_intent={glossary_intent}, force_glossary={force_glossary}, mail_match={mail_match}, glossary_match={glossary_match}"
        )
        rag_selected_domain = selected_rag_domain

        if rag_context and rag_relevant:
            from langchain_core.messages import SystemMessage, HumanMessage
            stats["used_rag"] = True

            if doc_nav_mode:
                answer = build_doc_nav_answer(
                    question=effective_question,
                    documents=selected_docs,
                    max_docs=10,
                    learning_based=doc_nav_learning_mode,
                )
            elif final_intent == "hybrid" and sql_summary:
                system_prompt = build_hybrid_prompt(question, sql_summary_text, rag_context)
            elif doc_summary_mode or (issue_summary_intent and ISSUE_SUMMARY_SPEED_MODE):
                system_prompt = f"""
당신은 GOC 업무 지원 챗봇입니다. 아래 [검색 문서]만 근거로 아주 간결하게 답하세요.

규칙
1) 최신 문서일시 순(내림차순)으로 정렬
2) 문서에 없는 내용은 추측하지 않음
3) 답변은 짧게: 한줄요약 1개 + 핵심 3개 + 근거 2개
4) "LV1/LV2" 같은 중요도 라벨만 단독으로 쓰지 말고, 각 항목에 무엇이 문제인지/영향/대응 중 최소 1개를 포함
5) "관리 이슈 지속중"처럼 포괄 표현만 쓰지 말고, 대상/현상/조치 내용을 구체 명사로 적기

[검색 문서]
{rag_context}

출력 형식
📌 한줄 요약
- 1문장

📂 핵심 이슈(최신순)
- (문서일시) 내용
- (문서일시) 내용
- (문서일시) 내용

📂 근거 문서
- 제목 | 문서일시 | 링크
- 제목 | 문서일시 | 링크
"""
            else:
                system_prompt = f"""
                당신은 GOC 업무 지원 챗봇입니다.

                최우선 규칙
                1) 아래 [검색 문서]에 있는 내용만을 근거로 "📂 문서 기반 답변"을 작성하세요. (추측/일반상식/외부지식 금지)
                2) 문서에 없는 내용은 반드시 "문서에 해당 정보가 없습니다."라고 명시하세요.
                3) 질문에 기간(이번주/저번주/지난주/오늘/어제/최근N일)이 포함되면, 답변 첫 줄 또는 요약에 적용한 기간을 반드시 명시하세요.
                4) 질문에 기간 지정이 없으면, 검색 문서 중 "가장 최신 문서일시"를 기준으로 답변하고, 그 기준 문서일시를 명시하세요.
                5) 문서 간 내용이 다르면 가장 최신 문서를 우선하고, "문서 간 상충"이라고 표시하세요.
                6) 답변의 항목/불릿은 가능한 한 문서일시 최신순(내림차순)으로 배치하세요.
                7) "💡 AI 의견"은 참고용 보충설명만 가능하며, 문서 사실처럼 단정하지 마세요. 문서와 충돌하면 문서가 항상 우선입니다.

                [검색 문서]
                {rag_context}

                출력 형식(아래 순서/제목을 반드시 그대로 유지)
                📌 한줄 요약
                - (기간/기준일시 포함 1문장)

                📂 문서 기반 답변
                - 핵심 사실 2~5개 (각 항목에 가능한 경우 날짜/수량/조직/대상 포함)
                - 문서에 없는 부분은 "문서에 해당 정보가 없습니다."로 표시

                💡 AI 의견
                - (참고용) 해석/실무적 의미 1~3개
                - 단정 금지(“~일 수 있습니다/권장합니다/확인 필요”)

                📂 근거 문서
                - 1) {'문서명'} | {'문서일시'} | {'근거한줄'} | {'링크'}
                - 2) ...
                (최대 3개)

                ⚠️ 주의
                - "📂 문서 기반 답변"은 문서에 있는 사실만, "💡 AI 의견"은 참고용입니다.

                🔗 이슈지 바로가기 👉 https://go/issueG
            """

            if not doc_nav_mode:
                messages = [SystemMessage(content=system_prompt)]
                if memory_text:
                    messages.append(HumanMessage(content=f"[최근 대화 메모리]\n{memory_text}"))
                if final_intent == "hybrid" and sql_summary:
                    messages.append(HumanMessage(content=f"[SQL summary]\n{sql_summary_text}"))
                messages.append(HumanMessage(content=f"[RAG context]\n{rag_context}"))
                messages.append(HumanMessage(content=question))
                t_llm = time.perf_counter()
                response = llm_invoke_with_retry(llm, messages, attempts=1, base_delay=1.5)
                perf["llm_ms"] = (time.perf_counter() - t_llm) * 1000
                stats["llm_calls"] += 1
                answer = response.content.strip()
                if doc_summary_mode or issue_summary_intent:
                    answer = enrich_sparse_issue_lines(answer, selected_docs)

                if "📂 근거 문서" not in answer:
                    source_lines = []
                    for doc in selected_docs[:3]:
                        title = doc.get("title", "제목 없음")
                        doc_date = doc.get("_doc_date", "날짜 정보 없음")
                        url = doc.get("confluence_mail_page_url", "") or doc.get("url", "")
                        line = f"- {title} | {doc_date}"
                        if url:
                            line += f"\n  🔗 GO LINK: {url}"
                        source_lines.append(line)
                    if source_lines:
                        answer += "\n\n📂 근거 문서\n" + "\n".join(source_lines)

                # 속도 모드(이슈 요약)에서도 이슈지 바로가기 링크를 항상 하단에 고정 노출
                if issue_summary_intent and ISSUE_SUMMARY_SPEED_MODE and "https://go/issueG" not in answer:
                    answer += "\n\n🔗 이슈지 바로가기 👉 https://go/issueG"
        else:
            if final_intent == "doc_nav":
                answer = build_doc_nav_answer(
                    question=effective_question,
                    documents=[],
                    max_docs=10,
                    learning_based=doc_nav_learning_mode,
                )
            elif final_intent == "hybrid" and sql_summary:
                answer = build_hybrid_fallback_answer(sql_summary, rag_found=False)
            else:
                from langchain_core.messages import SystemMessage, HumanMessage
                fallback_system_prompt = """
당신은 GOC 업무 지원 챗봇입니다.
이번 질문은 문서 검색 결과가 없거나 관련성이 낮아 일반 LLM 답변으로 안내합니다.
과도한 추측은 피하고, 불확실한 내용은 단정하지 마세요.
"""
                messages = [SystemMessage(content=fallback_system_prompt)]
                if memory_text:
                    messages.append(HumanMessage(content=f"[최근 대화 메모리]\n{memory_text}"))
                messages.append(HumanMessage(content=question))
                t_llm = time.perf_counter()
                response = llm_invoke_with_retry(llm, messages, attempts=1, base_delay=1.5)
                perf["llm_ms"] = (time.perf_counter() - t_llm) * 1000
                stats["llm_calls"] += 1

                reason = "관련 문서를 찾지 못했습니다."
                if force_glossary:
                    reason = "용어 사전에서 일치 항목을 찾지 못했습니다."
                elif skip_rag:
                    reason = f"검색 문서 유사도가 기준치({get_dynamic_similarity_threshold(effective_question, time_range):.2f})보다 낮았습니다."
                elif (mail_docs or glossary_docs) and not rag_relevant:
                    reason = "검색 문서는 있었지만 질문과의 관련성이 낮았습니다."
                stats["fallback_reason"] = reason
                answer = f"📋 문서 기반 답변 미적용\n- {reason}\n- 아래는 일반 LLM 답변입니다.\n\n" + response.content.strip()

        print(
            f"[RAG Final] selected_rag_domain={selected_rag_domain} used_rag={stats['used_rag']} "
            f"fallback_reason={stats.get('fallback_reason','')}"
        )
        rag_user_notice = str(rag_retrieval_meta.get("user_notice") or "").strip()
        if rag_user_notice and rag_user_notice not in answer:
            answer = f"📋 검색 안내\n- {rag_user_notice}\n\n{answer}"
        if force_glossary_mode:
            answer = _sanitize_glossary_answer(answer)
        print(f"[HYBRID] sql_used={sql_used} rag_used={stats['used_rag']}")
        print(f"[ANSWER] mode={final_intent} llm_used={stats['llm_calls'] > 0}")
        _send_answer_with_feedback_card(answer)
        save_conversation_memory(
            scope_id=scope_id,
            room_id=str(chatroom_id),
            user_id=sender_knox,
            role="assistant",
            content=answer,
            chat_type=chat_type,
        )
        # 대화 맥락(state) 업데이트: 후속 질문에서 주제/기간 보완에 활용
        try:
            save_conversation_state(
                scope_id,
                topic=ctx_state.get("topic", ""),
                time_label=ctx_state.get("time_label", ""),
                last_query=effective_question,
            )
        except Exception as _e:
            print(f"[CONTEXT STATE] save failed: {_e}")
        perf["total_ms"] = (time.perf_counter() - perf["t0"]) * 1000
        if LLM_PROFILE_LOG:
            print(
                "[LLM PERF] "
                f"total={perf.get('total_ms', 0):.0f}ms "
                f"init={perf.get('llm_init_ms', 0):.0f}ms "
                f"memory={perf.get('memory_ms', 0):.0f}ms "
                f"rewrite={perf.get('rewrite_ms', 0):.0f}ms "
                f"rag={perf.get('rag_fetch_ms', 0):.0f}ms "
                f"rerank={perf.get('rerank_ms', 0):.0f}ms "
                f"llm={perf.get('llm_ms', 0):.0f}ms"
            )
        return stats

    except Exception as e:
        print(f"[LLM Background Error] {e}")
        import traceback
        traceback.print_exc()
        stats["fallback_reason"] = f"error:{e}"
        try:
            chatBot.send_text(chatroom_id, f"LLM 응답 오류: {e}")
        except Exception as send_err:
            print("[send error message failed]", send_err)
        return stats
    finally:
        try:
            latency_ms = int((time.perf_counter() - perf.get("t0", time.perf_counter())) * 1000)
            top_doc = selected_docs[0] if selected_docs else {}
            debug_json = {
                "search_queries": search_queries[:8],
                "rag_timeout_occurred": bool(rag_retrieval_meta.get("timeout_occurred")),
                "rag_fallback_used": bool(rag_retrieval_meta.get("fallback_used")),
                "rag_user_notice": str(rag_retrieval_meta.get("user_notice") or ""),
                "rag_query_meta": rag_retrieval_meta.get("queries") or [],
                "weekly_issue_query": bool(weekly_debug.get("weekly_issue_query")),
                "extracted_period_label": weekly_debug.get("extracted_period_label") or "",
                "detected_topic": weekly_debug.get("detected_topic") or "",
                "week_token": weekly_debug.get("week_token") or "",
                "exact_week_match_count": int(weekly_debug.get("exact_week_match_count") or 0),
                "recent_fallback_used": bool(weekly_debug.get("recent_fallback_used")),
                "rerank_reason_summary": weekly_debug.get("rerank_reason_summary") or [],
            }
            if request_id:
                store.log_query_event(
                    request_id=request_id,
                    sender_knox=sender_knox,
                    sender_name=sender_name,
                    chatroom_id=str(chatroom_id),
                    chat_type=chat_type,
                    raw_question=question,
                    effective_question=effective_question,
                    normalized_query=normalized_query,
                    detected_intent=final_intent,
                    sql_registry_id=sql_registry_id,
                    sql_used=int(bool(sql_used)),
                    rag_used=int(bool(stats.get("used_rag"))),
                    rag_selected_domain=rag_selected_domain,
                    rag_top_doc_title=str(top_doc.get("title") or top_doc.get("_source", {}).get("title") or ""),
                    rag_top_doc_url=str(top_doc.get("confluence_mail_page_url") or top_doc.get("url") or ""),
                    rag_top_doc_score=float(
                        top_doc.get("_weekly_score")
                        or top_doc.get("_combined_score")
                        or top_doc.get("_score")
                        or 0.0
                    ),
                    rag_doc_count=len(selected_docs),
                    fallback_reason=str(stats.get("fallback_reason") or ""),
                    answer_preview=_answer_preview(answer),
                    latency_ms=latency_ms,
                    success_flag=int(bool(success_flag)),
                    debug_json=debug_json,
                )
                print(
                    f"[QUERY_LOG] request_id={request_id} intent={final_intent or 'unknown'} "
                    f"rag_used={bool(stats.get('used_rag'))} sql_used={bool(sql_used)} success={bool(success_flag)}"
                )
        except Exception as log_err:
            print(f"[QUERY_LOG] log failed request_id={request_id} err={log_err}")


def process_llm_chat_background(task: Dict[str, Any]) -> Dict[str, Any]:
    return _process_llm_chat_background_impl(task)

def rewrite_search_queries(question: str, llm: ChatOpenAI, *, memory_text: str = "", use_memory: bool = False) -> List[str]:
    """
    LLM을 사용하여 질문을 검색 최적화 질의로 재작성

    Args:
        question: 사용자 질문
        llm: LLM 인스턴스

    Returns:
        재작성된 검색 질의 목록 (최대 2개)
    """
    from langchain_core.messages import SystemMessage, HumanMessage

    system_prompt = """사용자의 질문을 문서 검색에 최적화된 질의로 재작성하세요.
다음 조건을 반영하여 정확히 2개의 검색 질의를 생성하세요:
1. 핵심 키워드 추출
2. 동의어/업무용 표현 보강
3. 너무 긴 문장은 짧은 검색 질의로 축약
4. 질문이 문맥 의존적(예: 그거, 아까 말한거, 담당자는?, 왜?, 언제?)이면 최근 대화 문맥을 반영해 독립적인 질의로 확장

각 질의는 줄바꿈으로 구분하세요. 다른 설명은 하지 마세요.
"""

    rewrite_input = question
    if use_memory and memory_text:
        rewrite_input = f"최근 대화:\n{memory_text}\n\n현재 질문:\n{question}"

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=rewrite_input)
    ]

    try:
        response = llm_invoke_with_retry(llm, messages, attempts=1, base_delay=1.0)
        queries_text = response.content.strip()
        queries = []
        for q in queries_text.split('\n'):
            normalized = q.strip()
            if normalized and normalized not in queries:
                queries.append(normalized)

        if not queries:
            return [question]
        if len(queries) == 1:
            return [queries[0], question] if queries[0] != question else [question]
        return queries[:RAG_REWRITE_QUERY_COUNT]
    except Exception as e:
        print(f"[Query Rewrite Error] {e}")
        return [question]


# =========================
# 3) Action Parsing
# =========================
def _extract_group_llm_question(txt: str) -> str:
    text = (txt or "").strip()
    if not text:
        return ""
    mention = (LLM_GROUP_MENTION_TEXT or "").strip()
    if mention and text.startswith(mention):
        return text[len(mention):].strip(" :")

    for prefix in LLM_GROUP_PREFIXES:
        pfx = prefix.strip()
        if not pfx:
            continue
        if text.startswith(pfx):
            return text[len(pfx):].strip(" :")
        if text.startswith(pfx + ",") or text.startswith(pfx + ":"):
            return text[len(pfx)+1:].strip()
    return ""


def parse_action_payload(info: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    chat_msg = info.get("chatMsg", "") or ""
    raw = chat_msg
    if " -->" in chat_msg:
        parts = chat_msg.split(" -->", 1)
        raw = parts[1].strip()

    # 1) 버튼/카드 payload(JSON) 우선
    if raw.strip().startswith("{"):
        try:
            payload = json.loads(raw)
            action = payload.get("action", "HOME")
            return action, payload
        except Exception:
            pass

    txt = raw.strip()
    txt_u = txt.upper()
    chat_type = (info.get("chatType") or "").upper()

    # 2) 시스템 트리거 우선
    if txt_u in ("INTRO", "HOME") or txt in ("홈", "/home"):
        return "INTRO", {}
    if txt in ("바로가기", "/바로가기", "링크", "/links", "links"):
        return "QUICK_LINKS", {}

    # 3) SINGLE 단축키 OPEN_URL
    if chat_type == "SINGLE":
        key = txt_u[1:] if txt_u.startswith("/") else txt_u
        title, url = resolve_quick_link(key)
        if url:
            return "OPEN_URL", {"title": title, "url": url}

    # 4) 명령어
    if txt.startswith("/warn"):
        return "WARN_RUN", {}
    if txt.startswith("/issue"):
        return "ISSUE_FORM", {}
    if txt.startswith("/sql "):
        return "LLM_CHAT", {"question": txt[5:].strip(), "force_mode": "sql"}
    if txt == "/sql":
        return "LLM_CHAT", {"question": "", "force_mode": "sql"}
    if txt.startswith("/용어 "):
        return "LLM_CHAT", {"question": txt[4:].strip(), "force_mode": "glossary"}
    if txt == "/용어":
        return "LLM_CHAT", {"question": "", "force_mode": "glossary"}

    # 5) LLM 라우팅
    if chat_type == "SINGLE":
        if txt in MEMORY_RESET_COMMANDS:
            return "LLM_CHAT", {"question": txt}
        if txt.startswith("/ask "):
            return "LLM_CHAT", {"question": txt[5:].strip()}
        if txt.startswith("질문:"):
            return "LLM_CHAT", {"question": txt[3:].strip()}
        if not txt.startswith("/"):
            return "LLM_CHAT", {"question": txt}
        return "NOOP", {}

    if chat_type == "GROUP":
        if LLM_CHAT_DEFAULT_MODE == "all" and not txt.startswith("/"):
            return "LLM_CHAT", {"question": txt}
        if LLM_CHAT_DEFAULT_MODE == "mention":
            q = _extract_group_llm_question(txt)
            if q:
                return "LLM_CHAT", {"question": q}
        return "NOOP", {}

    return "NOOP", {}



# =========================
# 4) UI-state helpers (recall 카드)
# =========================
def extract_msgid_senttime(resp: dict):
    if not isinstance(resp, dict):
        return None, None

    pme = resp.get("processedMessageEntries")
    if isinstance(pme, list) and pme:
        x = pme[0] or {}
        mid = x.get("msgId")
        st  = x.get("sentTime")
        if mid is not None and st is not None:
            try:
                return int(mid), int(st)
            except:
                return mid, st

    for k in ("chatReplyResultList", "chatReplyResults", "resultList", "data", "results"):
        v = resp.get(k)
        if isinstance(v, list) and v:
            x = v[0] or {}
            mid = x.get("msgId") or x.get("messageId") or x.get("msgID")
            st  = x.get("sentTime") or x.get("sendTime") or x.get("sent_time")
            if mid is not None and st is not None:
                try:
                    return int(mid), int(st)
                except:
                    return mid, st

    mid = resp.get("msgId") or resp.get("messageId") or resp.get("msgID")
    st  = resp.get("sentTime") or resp.get("sendTime") or resp.get("sent_time")
    if mid is not None and st is not None:
        try:
            return int(mid), int(st)
        except:
            return mid, st

    return None, None


def send_issue_list_card(chatroom_id: int, issues: List[dict], *, scope_room_id: str, recall_prev: bool = True):
    if chatBot is None:
        print("[send_issue_list_card] KNOX 연결 안됨")
        return
    
    if recall_prev and ENABLE_RECALL:
        st = store.ui_get_issue_list_state(str(chatroom_id))
        if st and st.get("issue_list_msg_id") and st.get("issue_list_sent_time"):
            try:
                chatBot.recall_message(chatroom_id, int(st["issue_list_msg_id"]), int(st["issue_list_sent_time"]))
            except Exception as e:
                print("[recall issue_list card failed]", e)

        # ✅ D-day 계산 + 정렬(목표일 임박순) 보장
    for it in issues:
        it["d_day"] = store._dday(it.get("target_date", ""))

    issues.sort(key=lambda x: (999999 if x.get("d_day") is None else x.get("d_day"), int(x.get("issue_id", 0))))

    resp = chatBot.send_adaptive_card(chatroom_id, ui.build_issue_list_card(issues, room_id=str(scope_room_id)))

    mid, sent = extract_msgid_senttime(resp)
    if mid and sent:
        store.ui_set_issue_list_state(str(chatroom_id), mid, sent)


def send_issue_history_card(chatroom_id: int, *, scope_room_id: str, page: int, recall_prev: bool = False):
    if chatBot is None:
        print("[send_issue_history_card] KNOX 연결 안됨")
        return
    
    if recall_prev and ENABLE_RECALL:
        st = store.ui_get_history_state(str(chatroom_id))
        if st and st.get("history_msg_id") and st.get("history_sent_time"):
            try:
                chatBot.recall_message(chatroom_id, int(st["history_msg_id"]), int(st["history_sent_time"]))
            except Exception as e:
                print("[recall history card failed]", e)

    total = store.issue_count_all(str(scope_room_id))
    max_page = max(0, (total - 1) // store.HISTORY_PAGE_SIZE) if total > 0 else 0
    page = max(0, min(int(page), max_page))

    issues = store.issue_list_all_paged(str(scope_room_id), page, store.HISTORY_PAGE_SIZE)
    resp = chatBot.send_adaptive_card(
        chatroom_id,
        ui.build_issue_history_card(issues, page=page, total=total, page_size=store.HISTORY_PAGE_SIZE, room_id=str(scope_room_id))
    )

    mid, sent = extract_msgid_senttime(resp)
    if mid is not None and sent is not None:
        store.ui_set_history_state(str(chatroom_id), mid, sent)


# =========================
# 5) Oracle Query runner
# =========================
def run_oracle_query(sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    return run_oracle_query_compat(
        sql,
        params,
        host=ORACLE_HOST,
        port=ORACLE_PORT,
        service=ORACLE_SERVICE,
        user=ORACLE_USER,
        password=ORACLE_PW,
        lib_dir=r"c:\instantclient",
    )

# (추가 코드 - 추가용)  ※ run_oracle_query 아래쪽에 추가
def _likeify2(v: str) -> str:
    v = (v or "").strip()
    if not v:
        return ""
    return v if ("%" in v or "_" in v) else f"%{v}%"

def _ym6(s: str) -> str:
    s = "".join([c for c in (s or "") if c.isdigit()])
    return s[:6] if len(s) >= 6 else s

def run_oneview_ship(params: dict) -> pd.DataFrame:
    smon = _ym6(params.get("smon",""))
    emon = _ym6(params.get("emon",""))
    conv = (params.get("conv") or "deliverynum01").strip()
    qraw = (params.get("q") or "").strip()

    q = _likeify2(qraw.upper().replace(" ", ""))

    filter_map = {
        "deliverynum01": "a.DLVRY_NUM LIKE :q",
        "haitem01":      "a.SALE_ITEM_CODE LIKE :q",
        "haversion01":   "(b.DRAMVER LIKE :q OR b.NANDVER LIKE :q)",
        "hagc01":        "(a.GC_CODE LIKE :q OR a.GC_NAME LIKE :q)",
    }
    filter_clause = filter_map.get(conv, filter_map["deliverynum01"])

    sql = ui.SQL_ONEVIEW_SHIP_BASE.format(filter_clause=filter_clause)
    return run_oracle_query(sql, params={"smon": smon, "emon": emon, "q": q})

def run_pkgcode(params: dict) -> pd.DataFrame:
    raw = (params.get("q") or "").strip()
    q = raw.upper().replace(" ", "")

    like_q = _likeify2(q)

    # ✅ 입력에 따라 where_clause 분기 (원본 로직 그대로)
    if q.isalpha() and len(q) == 2:
        where_clause = "B.VERSION LIKE :q"
    elif len(q) == 3:
        where_clause = "A.PACK_CODE LIKE :q"
    else:
        where_clause = "(A.PACK_CODE||B.VERSION||B.PCBCODE) LIKE :q"

    sql = ui.SQL_PKGCODE_BASE.format(where_clause=where_clause)
    return run_oracle_query(sql, params={"q": like_q})


# (추가 코드 - 교체/추가용)
from difflib import SequenceMatcher

def _sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a or "", b or "").ratio()

def _clean_xa0(x):
    if isinstance(x, str):
        return x.replace("\xa0", " ")
    if isinstance(x, list):
        return [_clean_xa0(v) for v in x]
    if isinstance(x, dict):
        return {k: _clean_xa0(v) for k, v in x.items()}
    return x

def run_term_search(params: dict):
    q = (params.get("q") or "").strip()
    if not q:
        return ui.build_term_not_found_card(q)
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
    try:
        init_oracle_thick_mode_once(lib_dir=r"c:\instantclient")
        startup_initialize(
            host=ORACLE_HOST,
            port=ORACLE_PORT,
            service=ORACLE_SERVICE,
            user=ORACLE_USER,
            password=ORACLE_PW,
            lib_dir=r"c:\instantclient",
        )
    except Exception as e:
        print(f"[ORACLE] startup init failed: {e}")
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

        elif action == "FEEDBACK_LIKE":
            request_id = str(payload.get("request_id") or "").strip()
            if not request_id:
                chatBot.send_text(chatroom_id, "요청 ID가 없어 피드백을 저장하지 못했습니다.")
                return {"ok": True}
            qlog = store.get_query_log(request_id) or {}
            store.add_query_feedback(
                request_id=request_id,
                chatroom_id=str(chatroom_id),
                sender_knox=sender_knox,
                feedback_type=normalize_feedback_type("like"),
                reason_code="",
                memo="",
                detected_intent=str(qlog.get("detected_intent") or ""),
                sql_registry_id=str(qlog.get("sql_registry_id") or ""),
                rag_top_doc_title=str(qlog.get("rag_top_doc_title") or ""),
                rag_top_doc_score=float(qlog.get("rag_top_doc_score") or 0.0),
            )
            print(f"[FEEDBACK] request_id={request_id} type=like")
            chatBot.send_text(chatroom_id, "피드백 감사합니다. 👍")
            return {"ok": True}

        elif action == "FEEDBACK_DISLIKE":
            request_id = str(payload.get("request_id") or "").strip()
            if not request_id:
                chatBot.send_text(chatroom_id, "요청 ID가 없어 피드백을 저장하지 못했습니다.")
                return {"ok": True}
            qlog = store.get_query_log(request_id) or {}
            store.add_query_feedback(
                request_id=request_id,
                chatroom_id=str(chatroom_id),
                sender_knox=sender_knox,
                feedback_type=normalize_feedback_type("dislike"),
                reason_code="",
                memo="",
                detected_intent=str(qlog.get("detected_intent") or ""),
                sql_registry_id=str(qlog.get("sql_registry_id") or ""),
                rag_top_doc_title=str(qlog.get("rag_top_doc_title") or ""),
                rag_top_doc_score=float(qlog.get("rag_top_doc_score") or 0.0),
            )
            print(f"[FEEDBACK] request_id={request_id} type=dislike reason=")
            chatBot.send_adaptive_card(chatroom_id, ui.build_feedback_reason_card(request_id))
            return {"ok": True}

        elif action == "FEEDBACK_REASON_SUBMIT":
            request_id = str(payload.get("request_id") or "").strip()
            if not request_id:
                chatBot.send_text(chatroom_id, "요청 ID가 없어 피드백을 저장하지 못했습니다.")
                return {"ok": True}
            reason_code = normalize_reason_code(str(payload.get("reason_code") or ""))
            memo = (payload.get("memo") or "").strip()
            qlog = store.get_query_log(request_id) or {}
            store.add_query_feedback(
                request_id=request_id,
                chatroom_id=str(chatroom_id),
                sender_knox=sender_knox,
                feedback_type=normalize_feedback_type("dislike"),
                reason_code=reason_code,
                memo=memo,
                detected_intent=str(qlog.get("detected_intent") or ""),
                sql_registry_id=str(qlog.get("sql_registry_id") or ""),
                rag_top_doc_title=str(qlog.get("rag_top_doc_title") or ""),
                rag_top_doc_score=float(qlog.get("rag_top_doc_score") or 0.0),
            )
            print(f"[FEEDBACK] request_id={request_id} type=dislike reason={reason_code}")
            chatBot.send_text(chatroom_id, "아쉬운 점 반영을 위해 기록했습니다. 감사합니다.")
            return {"ok": True}
        
        # ---------- LLM Chatbot ----------
        elif action == "LLM_CHAT":
            if not is_llm_allowed_user(sender_knox):
                chatBot.send_adaptive_card(chatroom_id, ui.build_home_card(dashboard_url=DASHBOARD_URL, infocenter_url=INFOCENTER_URL))
                return {"ok": True}

            question = (payload.get("question") or "").strip()
            force_mode = (payload.get("force_mode") or "").strip().lower()
            if not question and force_mode == "sql":
                chatBot.send_text(chatroom_id, "사용법: /sql 질문내용  (예: /sql 2월 vh 판매 몇개야)")
                return {"ok": True}
            if not question and force_mode == "glossary":
                chatBot.send_text(chatroom_id, "사용법: /용어 단어  (예: /용어 hbm)")
                return {"ok": True}
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
                    "force_mode": force_mode,
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
                if f.get("required") and not (payload.get(f["id"]) or "").strip():
                    chatBot.send_text(ui_room, f"필수값 누락: {f.get('label', f['id'])}")
                    chatBot.send_adaptive_card(ui_room, ui.build_query_form_card(spec))
                    return {"ok": True}

            params_builder = spec.get("params_builder")
            params = params_builder(payload) if callable(params_builder) else None

            # (수정 코드 - 교체용)
            result = RUNNERS[spec["runner"]](params or {}) if spec.get("runner") else run_oracle_query(spec["sql"], params=params)

            # ✅ runner가 AdaptiveCard(dict)로 주면 그대로 전송하고 종료
            if isinstance(result, dict) and result.get("type") == "AdaptiveCard":
                chatBot.send_adaptive_card(ui_room, result)
                return {"ok": True}

            df = result  # DataFrame으로 간주

            if spec.get("output") == "MSG7_TABLE":
                chatBot.send_table_csv_msg7(ui_room, df, title=spec.get("title","조회 결과"))
            else:
                chatBot.send_adaptive_card(ui_room, ui.df_to_table_card(df, title=spec.get("title","조회 결과")))

            return {"ok": True}
        
        # (추가 코드 - 교체/추가용)  ※ Generic Query Router 위쪽 아무 곳에 추가
        elif action == "TERM_UNKNOWN_SUBMIT":
            ui_room = route_ui_room(chatroom_id, info, sender_name)  # ✅ 누락 보완

            findword = (payload.get("findword") or "").strip()
            memo = (payload.get("memo") or "").strip()
            rooms = [x.strip() for x in TERM_ADMIN_ROOM_IDS.split(",") if x.strip().isdigit()]

            msg = f"📩 [용어 반영 요청]\n- 단어: {findword}\n- 요청자: {sender}\n" + (f"- 메모: {memo}\n" if memo else "")
            if rooms:
                for rid in rooms:
                    chatBot.send_text(int(rid), msg)
                chatBot.send_text(ui_room, "접수 완료 ✅ (담당자에게 전달했습니다)")
            else:
                chatBot.send_text(ui_room, "접수 완료 ✅ (TERM_ADMIN_ROOM_IDS 미설정이라 전달은 생략됨)")
            return {"ok": True}      

        # ---------- Issue ----------
        elif action == "ISSUE_FORM":
            scope = store.scope_room_id(chatroom_id, payload)          # ✅ 데이터 스코프(원래 단체방)
            ui_room = route_ui_room(chatroom_id, info, sender_name)    # ✅ UI는 DM (SINGLE이면 그대로)

            chatBot.send_adaptive_card(
                ui_room,
                ui.build_issue_form_card(sender_hint=sender, room_id=str(scope))
            )
            return {"ok": True}


        elif action == "ISSUE_CREATE":
            scope = store.scope_room_id(chatroom_id, payload)          # ✅ 데이터 스코프(원래 단체방)
            ui_room = route_ui_room(chatroom_id, info, sender_name)    # ✅ UI는 DM (SINGLE이면 그대로)
            origin_room = int(scope)

            title = (payload.get("title") or "").strip()
            content = (payload.get("content") or "").strip()
            url = (payload.get("url") or "").strip()
            occur_date = (payload.get("occur_date") or "").strip()
            target_date = (payload.get("target_date") or "").strip()
            owner = (payload.get("owner") or "").strip()

            if not title:
                chatBot.send_text(ui_room, "제목이 비어있습니다. 다시 발의해 주세요.")
                chatBot.send_adaptive_card(
                    ui_room,
                    ui.build_issue_form_card(sender_hint=sender, room_id=str(origin_room))
                )
                return {"ok": True}

            issue_id = store.issue_create(
                origin_room,
                title,
                content,
                url,
                occur_date,
                target_date,
                owner,
                sender
            )

            # ✅ 완료 메시지/UI 갱신은 ui_room(DM)으로
            chatBot.send_text(ui_room, f"✅ 이슈 등록 완료: #{issue_id} {title}")

            try:
                issues = store.issue_list_open(str(origin_room))
                send_issue_list_card(ui_room, issues, scope_room_id=str(origin_room), recall_prev=True)
            except Exception as e:
                print("[dm issue list refresh failed]", e)

            return {"ok": True}



        elif action == "ISSUE_LIST":
            scope = store.scope_room_id(chatroom_id, payload)          # ✅ 데이터 스코프(원래 단체방)
            ui_room = route_ui_room(chatroom_id, info, sender_name)    # ✅ UI는 DM

            issues = store.issue_list_open(str(scope))
            send_issue_list_card(ui_room, issues, scope_room_id=str(scope), recall_prev=True)
            return {"ok": True}


        elif action == "ISSUE_CLEAR":
            scope = store.scope_room_id(chatroom_id, payload)
            ui_room = route_ui_room(chatroom_id, info, sender_name)

            issue_id = payload.get("issue_id")
            if issue_id is None:
                chatBot.send_text(ui_room, "issue_id가 없습니다.")
                return {"ok": True}

            store.issue_clear(str(scope), int(issue_id), sender)
            chatBot.send_text(ui_room, f"✅ Clear 처리 완료: #{issue_id}")

            issues = store.issue_list_open(str(scope))
            send_issue_list_card(ui_room, issues, scope_room_id=str(scope), recall_prev=True)
            return {"ok": True}


        elif action == "ISSUE_EDIT_FORM":
            scope = store.scope_room_id(chatroom_id, payload)
            ui_room = route_ui_room(chatroom_id, info, sender_name)

            issue_id = payload.get("issue_id")
            if issue_id is None:
                chatBot.send_text(ui_room, "issue_id가 없습니다.")
                return {"ok": True}

            issue = store.issue_get(str(scope), int(issue_id))
            if not issue:
                chatBot.send_text(ui_room, f"해당 이슈를 찾을 수 없습니다: #{issue_id}")
                return {"ok": True}

            chatBot.send_adaptive_card(ui_room, ui.build_issue_edit_form_card(issue, room_id=str(scope)))
            return {"ok": True}


        elif action == "ISSUE_UPDATE":
            scope = store.scope_room_id(chatroom_id, payload)
            ui_room = route_ui_room(chatroom_id, info, sender_name)

            issue_id = payload.get("issue_id")
            if issue_id is None:
                chatBot.send_text(ui_room, "issue_id가 없습니다.")
                return {"ok": True}

            title = (payload.get("title") or "").strip()
            content = (payload.get("content") or "").strip()
            url = (payload.get("url") or "").strip()
            occur_date = (payload.get("occur_date") or "").strip()
            target_date = (payload.get("target_date") or "").strip()
            owner = (payload.get("owner") or "").strip()

            if not title:
                chatBot.send_text(ui_room, "제목이 비어있습니다.")
                issue = store.issue_get(str(scope), int(issue_id))
                if issue:
                    chatBot.send_adaptive_card(ui_room, ui.build_issue_edit_form_card(issue, room_id=str(scope)))
                return {"ok": True}

            store.issue_update(str(scope), int(issue_id), title, content, url, occur_date, target_date, owner, actor=sender)
            chatBot.send_text(ui_room, f"✅ 수정 완료: #{issue_id} {title}")

            issues = store.issue_list_open(str(scope))
            send_issue_list_card(ui_room, issues, scope_room_id=str(scope), recall_prev=True)
            return {"ok": True}


        elif action == "ISSUE_HISTORY":
            scope = store.scope_room_id(chatroom_id, payload)
            ui_room = route_ui_room(chatroom_id, info, sender_name)

            is_nav = ("page" in payload)
            page = int(payload.get("page", 0) or 0)
            send_issue_history_card(ui_room, scope_room_id=str(scope), page=page, recall_prev=is_nav)
            return {"ok": True}


        elif action == "ISSUE_DELETE":
            scope = store.scope_room_id(chatroom_id, payload)
            ui_room = route_ui_room(chatroom_id, info, sender_name)

            issue_id = payload.get("issue_id")
            if issue_id is None:
                chatBot.send_text(ui_room, "issue_id가 없습니다.")
                return {"ok": True}

            page = int(payload.get("page", 0) or 0)
            ok, msg = store.issue_delete(str(scope), int(issue_id), sender)
            if not ok:
                chatBot.send_text(ui_room, msg)

            send_issue_history_card(ui_room, scope_room_id=str(scope), page=page, recall_prev=True)
            return {"ok": True}


        # ---------- Watchroom ----------
        elif action == "WATCHROOM_FORM":
            ui_room = route_ui_room(chatroom_id, info, sender_name)
            chatBot.send_adaptive_card(ui_room, ui.build_watchroom_form_card())


        elif action == "WATCHROOM_CREATE":
            ui_room = route_ui_room(chatroom_id, info, sender_name)

            room_title = (payload.get("room_title") or "").strip()
            members_raw = (payload.get("members") or "").strip()
            note = (payload.get("note") or "").strip()


            if not members_raw:
                chatBot.send_text(chatroom_id, "참여자 SSO가 비어있습니다. 예: sungmook.cho,cc.choi")
                chatBot.send_adaptive_card(chatroom_id, ui.build_watchroom_form_card())
                return {"ok": True}

            members = [x.strip() for x in members_raw.replace("\n", ",").split(",") if x.strip()]
            user_ids = chatBot.resolve_user_ids_from_loginids(members)
            if not user_ids:
                chatBot.send_text(chatroom_id, "참여자 변환(userID)이 실패했습니다. SSO가 맞는지 확인해 주세요.")
                return {"ok": True}

            title_to_use = room_title or note or "공지방"
            new_room_id = chatBot.room_create(user_ids, chatType=1, chatroom_title=title_to_use)
            store.add_watch_room(str(new_room_id), created_by=sender, note=note, chatroom_title=title_to_use)

            chatBot.send_text(
                chatroom_id,
                f"✅ 공지방 생성 & 푸시대상 등록 완료\n- chatroomId: {new_room_id}\n- title: {title_to_use}\n- note: {note}"
            )
            chatBot.send_text(
                new_room_id,
                "📣 이 방은 봇이 생성한 공지/워닝/이슈 방입니다.\n- 워닝(스케줄) / 이슈요약(스케줄) 푸시 대상입니다.\n- @공급망 챗봇 으로 기능을 실행하세요."
            )
            chatBot.send_adaptive_card(new_room_id, ui.build_home_card(dashboard_url=DASHBOARD_URL, infocenter_url=INFOCENTER_URL))
            return {"ok": True}

        else:
            chatBot.send_text(chatroom_id, f"알 수 없는 action: {action}")
            chatBot.send_adaptive_card(chatroom_id, ui.build_home_card(dashboard_url=DASHBOARD_URL, infocenter_url=INFOCENTER_URL))

    except Exception as e:
        chatBot.send_text(chatroom_id, f"오류 발생: {e}")

    return {"ok": True}


if __name__ == "__main__":
    uvicorn.run(app, host=BIND_HOST, port=BIND_PORT, workers=1)                              
