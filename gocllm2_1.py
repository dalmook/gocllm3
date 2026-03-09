### PART 1/5
# main.py
# pip install pycryptodomex fastapi uvicorn apscheduler requests pandas holidays langchain-openai cx_Oracle
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
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

import requests
import pandas as pd
import cx_Oracle
import uvicorn
import urllib3

from Cryptodome.Cipher import AES
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import HTMLResponse
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import store
import ui

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
RAG_INDEXES = os.getenv("RAG_INDEXES", "rp-gocinfo_mail_jsonl,rp-gocinfo_jsonl2,glossary_m3_100chunk50")
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
MAIL_INDEX_NAME = os.getenv("MAIL_INDEX_NAME", "rp-gocinfo_mail_jsonl,rp-gocinfo_jsonl2")

LLM_WORKERS = max(1, int(os.getenv("LLM_WORKERS", os.getenv("LLM_WORKER_COUNT", "4"))))
LLM_JOB_QUEUE_MAX = max(1, int(os.getenv("LLM_JOB_QUEUE_MAX", "200")))
LLM_MAX_CONCURRENT = max(1, int(os.getenv("LLM_MAX_CONCURRENT", "4")))
LLM_PROFILE_LOG = os.getenv("LLM_PROFILE_LOG", "true").lower() == "true"
ISSUE_SUMMARY_SPEED_MODE = os.getenv("ISSUE_SUMMARY_SPEED_MODE", "false").lower() == "true"
LLM_ALLOWED_USERS_SQL = os.getenv(
    "LLM_ALLOWED_USERS_SQL",
    "SELECT SSO_ID FROM SCM_WP.T_T_FOR_MASTER A WHERE 1=1 and a.dept_name in ('공급망운영그룹(메모리)','SCM그룹(메모리)','운영전략그룹(메모리)','Global운영팀(메모리)') and A.DEPT_NAME LIKE '%메모리%' and a.POSITION_CODE is not null AND A.SSO_ID NOT IN ('SCM.RPA','SCM 봇','메모리STO2','메모리 STO','dalbong.chatbot01', 'dalbongbot01', 'dalbong.bot01', 'command.center', 'thatcoolguy')"
)
LLM_ALLOWED_USERS_CACHE_TTL_SEC = max(0, int(os.getenv("LLM_ALLOWED_USERS_CACHE_TTL_SEC", "1800")))

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
        return self._post_encrypted(api, body)

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

# =========================
# RAG Client
# =========================
class RagClient:
    """RAG API 클라이언트"""

    def __init__(self, api_key: str, dep_ticket: str, base_url: str, timeout: int = 30):
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


def search_rag_documents(
    query: str,
    indexes: Optional[List[str]] = None,
    *,
    top_k: Optional[int] = None,
    mode: Optional[str] = None,
    filter: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    RAG 문서 검색 (다중 인덱스 지원)
    
    Args:
        query: 검색 쿼리
        indexes: 검색할 인덱스 목록 (None이면 기본 인덱스 사용)
    
    Returns:
        검색 결과 문서 목록
    """
    if indexes is None:
        indexes = [x.strip() for x in RAG_INDEXES.split(",") if x.strip()]
    
    sanitized_query = sanitize_query(query)
    if not sanitized_query:
        return []

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
    all_results = []
    
    for index in indexes:
        try:
            print(f"[RAG Search] Searching index: {index}")
            result = rag_client.retrieve(
                index_name=index,
                query_text=sanitized_query,
                mode=mode or RAG_RETRIEVE_MODE,
                num_result_doc=num_result_doc,
                permission_groups=[RAG_PERMISSION_GROUPS],
                filter=filter,
                bm25_boost=RAG_BM25_BOOST,
                knn_boost=RAG_KNN_BOOST,
            )
            # print(f"[RAG Search] Result from {index}: {result}")
            # 결과에서 문서 추출 (Elasticsearch 응답 구조: hits.hits)
            if "hits" in result and isinstance(result["hits"], dict):
                hits = result["hits"].get("hits", [])
                for hit in hits:
                    if "_source" in hit:
                        doc = hit["_source"]
                        doc["_index"] = index  # 인덱스 정보 추가
                        doc["_score"] = hit.get("_score", 0)  # 점수 추가
                        all_results.append(doc)
                print(f"[RAG Search] Found {len(hits)} documents in {index}")
            else:
                print(f"[RAG Search] No 'hits' field in response from {index}")
        except Exception as e:
            print(f"[RAG Search Error] Index: {index}, Error: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print(f"[RAG Search] Total results: {len(all_results)}")
    return all_results


DATE_FIELD_CANDIDATES = [
    "created_time", "last_modified_time", "updated_time", "modified_time",
    "updated_at", "updated_date", "last_updated", "last_modified",
    "modified_at", "modified_date", "created_at", "created_date",
    "register_date", "reg_date", "date", "datetime", "timestamp",
    "mail_date", "page_updated_at", "page_created_at"
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
    for k, v in doc.items():
        lk = str(k).lower()
        if any(token in lk for token in ("date", "time", "updated", "modified", "created", "ts")):
            dt = _parse_doc_datetime_value(v)
            if dt:
                return dt
    return None


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
