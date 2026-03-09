# gocllm3

사내 Knox 챗봇(FastAPI) 프로젝트입니다.

## 핵심 파일 역할
- `gocllm3.py`: 엔트리포인트, Knox 메시지 수신, 액션 라우팅, LLM/RAG/SQL 오케스트레이션
- `store.py`: SQLite 상태/이슈/DM/watchroom 저장소 계층
- `ui.py`: Oracle SQL 템플릿, Query Registry, Adaptive Card, Dashboard HTML
- `app/oracle_db.py`: python-oracledb Thick mode 초기화/Pool/쿼리 실행
- `app/sql_registry.py`: 자연어 질문용 SQL registry 매칭
- `app/sql_period.py`: 자연어 기간(올해/전분기/최근 N개월 등) 표준화 유틸
- `app/sql_registry.yaml`: `/sql` 메타데이터(sources/metrics/dimensions/query_families) + legacy queries
- `app/sql_answering.py`: `/sql` 다중 SQL 실행결과를 사용자 답변으로 렌더링
- `app/query_intent.py`: 질문 의도 분류(`general_llm`, `data_only`, `rag_only`, `hybrid`)
- `app/hybrid_router.py`: intent별 SQL 실행 여부/실행 래퍼
- `app/hybrid_answer.py`: data/hybrid 답변 포맷 생성

## LLM 질문 처리 흐름
`_process_llm_chat_background_impl()` 기준:
1. 메모리 로드
2. `effective_question` 생성
3. SQL registry 매칭 (`find_best_sql_registry_match`)
4. intent 분류 (`classify_query_intent`)
5. 분기 처리
- `general_llm`: 일반 LLM
- `data_only`: SQL 실행 후 데이터 답변
- `rag_only`: 기존 RAG 흐름
- `hybrid`: SQL + RAG 결합

## /sql 처리 흐름 (intent + slot)
`/sql` 모드는 metadata-driven planner를 우선 실행하고, 실패 시 legacy templates로 fallback 합니다.
1. `normalize_question(question)`
2. `extract_slots_rule_based(question)`:
  metric / aggregation / period / version / dimension / compare / trend
3. `classify_intent_rule_based(question, slots)`
4. 모호할 때만 `classify_sql_intent_with_llm` 보조 판별(JSON only)
5. `infer_default_period(intent, slots, question)`으로 기간 미지정 보정
6. `resolve_period(slots)`으로 `start_yyyymm/end_yyyymm` 계산
7. `build_execution_plan_from_slots(...)`로 `plan(JSON)` 생성
8. `build_sql_from_plan(...)`로 whitelist 기반 safe SQL 조립
9. 실패 시 `select_query_template(intent, slots)` legacy fallback
10. `build_execution_plan(question, intent, slots)`로 primary/aux SQL 계획 생성
11. `execute_registered_sql(query_id, params)`
12. `render_answer_rule_based()` 후 필요 시 `render_answer_with_llm()` 보조

## 1단계 Prefix 정책 (운영 안정화)
- SQL 조회는 `/sql ...` 입력일 때만 수행합니다.
- 용어(Glossary) 조회는 `/용어 ...` 입력일 때만 수행합니다.
- 일반 문장은 자동 SQL/자동 용어 매칭을 하지 않고 기존 RAG/일반 LLM 흐름으로 처리합니다.

예시:
- `/sql 2월 버전 vh 판매 몇개야`
- `/용어 hbm`

## SQL 등록 방법 (YAML)
신규 확장은 `queries`가 아니라 아래 메타데이터에 추가하는 것을 기본으로 합니다.
- `sources`: 테이블/컬럼/기본필터/스냅샷 규칙
- `metrics`: 집계 대상 컬럼
- `dimensions`: 필터/그룹 기준 축
- `query_families`: 질의 형태

`queries`는 legacy compatibility fallback 용도로 유지합니다.

필드 예시:
```yaml
queries:
  - id: psi_sales_by_month
    description: "특정 월/버전 판매량 합계"
    sql: |
      SELECT SUM(SALES_MEQ) AS sales
      FROM mst_psi_simul_report
      WHERE VERSION = :version
        AND YEARMONTH = :yearmonth
    params:
      version:
        type: string
        required: true
        aliases: ["버전", "VERSION", "vh", "version"]
      yearmonth:
        type: yyyymm
        required: true
        aliases: ["월", "년월", "YEARMONTH", "month"]
    result:
      mode: scalar
      field: sales
      empty_message: "해당 조건의 판매 데이터가 없습니다."
```

확장 필드(의도/슬롯 기반):
- `intent`: `sales_total`, `sales_trend`, `sales_grouped`, `sales_compare`
- `supported_slots`: 템플릿이 해석 가능한 슬롯 목록
- `default_aggregation`: 기본 집계 방식
- `supports_compare`: 비교형 질문 지원 여부
- `supports_trend`: 추이형 질문 지원 여부
- `groupable_dimensions`: 허용 차원(예: `version`, `month`)

대표 query id:
- `sales_total_month`
- `sales_total_period_range`
- `sales_trend_monthly`
- `sales_grouped_by_version`
- `sales_compare_periods`

질문 예시:
- `/sql 2월 vh 판매 몇개야`
- `/sql 버전 vh 202602 판매량`
- `/sql version=VH yearmonth=202602 판매량`

동작 완화 정책:
- `version` 값은 대소문자를 구분하지 않습니다. (`vh`, `VH` 모두 허용)
- 월을 `N월`로만 입력하면 연도는 최근 기준으로 자동 보정합니다.
- 당일 `workdate` 고정이 아니라 조건에 맞는 최신 적재일 기준으로 조회합니다.

## Oracle 연결 정책
- 드라이버: `python-oracledb` (Thin 금지)
- Thick mode 1회 초기화: `oracledb.init_oracle_client(lib_dir=r"c:\instantclient")`
- Pool 우선, 실패 시 connect fallback
- 환경변수 유지:
  - `ORACLE_HOST`
  - `ORACLE_PORT`
  - `ORACLE_SERVICE`
  - `ORACLE_USER`
  - `ORACLE_PW`

## 주요 로그 Prefix
- `[ORACLE]`
- `[SQL_REGISTRY]`
- `[SQL_NLU]`
- `[SQL_PLAN]`
- `[SQL_DEBUG]`
- `[SQL_ANSWER]`
- `[INTENT]`
- `[SQL_EXEC]`
- `[SQL_RESULT]`
- `[HYBRID]`
- `[ANSWER]`

## 실행
```bash
pip install -r requirements.txt
uvicorn gocllm3:app --host 0.0.0.0 --port 9500
```

## 운영 업데이트 규칙
모든 기능 변경/추가 시 아래를 같이 업데이트합니다.
1. `README.md`의 "핵심 파일 역할" 및 "LLM 질문 처리 흐름"
2. 신규 env var가 생기면 "Oracle 연결 정책" 또는 별도 섹션
3. 신규 로그 prefix가 생기면 "주요 로그 Prefix"
4. 배포/실행 방식이 바뀌면 "실행"

## 빠른 점검 (질문이 SQL을 탔는지)
로그에서 아래 순서로 확인:
1. `[SQL_REGISTRY] matched=...`
2. `[INTENT] ... final=data_only` 또는 `final=hybrid`
3. `[SQL_EXEC] runner=... ok=True`
4. `[SQL_RESULT] summary_chars=...`

`matched=None` 또는 `final=rag_only`면 SQL 미사용입니다.
