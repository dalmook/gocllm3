# gocllm3

사내 Knox 챗봇(FastAPI) 프로젝트입니다. `gocllm3.py`가 엔트리포인트이며, LLM/RAG/SQL 응답을 하나의 봇 서버에서 처리합니다.

## 핵심 파일
- `gocllm3.py`: FastAPI 엔트리포인트, Knox 메시지 수신, `/sql` 및 일반 대화 라우팅, 백그라운드 LLM 작업 큐
- `store.py`: SQLite 기반 상태 저장
- `ui.py`: Adaptive Card, 대시보드, 메시지 렌더링 유틸
- `app/oracle_db.py`: Oracle Thick mode 초기화와 쿼리 실행 래퍼
- `app/sql_registry.py`: `/sql` 자연어 해석, 슬롯 추출, 메타데이터 기반 SQL 계획/생성
- `app/sql_registry.yaml`: SQL source/metric/dimension/query family 정의와 legacy fallback query 정의
- `app/sql_period.py`: 월/년/분기/상대기간 해석
- `app/sql_answering.py`: SQL 실행 결과를 규칙기반 또는 LLM 기반 답변으로 렌더링
- `app/query_intent.py`: 일반 질의 intent 분류
- `app/hybrid_router.py`: SQL/RAG/Hybrid 실행 분기
- `app/hybrid_answer.py`: hybrid 응답 포맷

## 런타임 개요
일반 질문은 `_process_llm_chat_background_impl()`에서 처리합니다.

1. 대화 메모리 로드
2. 질문 정규화와 contextual rewrite 여부 판단
3. SQL registry 매칭과 query intent 분류
4. `general_llm`, `data_only`, `rag_only`, `hybrid`, `doc_nav`, `doc_summary` 중 하나로 라우팅
5. SQL 또는 RAG 실행
6. 최종 텍스트 답변 전송
7. 필요 시 진행 메시지 recall, 메모리 저장, 피드백 카드 전송

참고:
- 진행 안내 메시지 recall은 `ENABLE_RECALL`로 제어합니다.
- 피드백 카드는 기본적으로 숨김이며 `ENABLE_FEEDBACK_CARD=true`일 때만 전송합니다.

## `/sql` 처리 흐름
`/sql ...` 입력은 metadata-driven planner를 우선 사용하고, 필요 시 legacy query fallback을 사용합니다.

1. `normalize_question(question)`
2. 공통 synonym/alias canonicalization
3. `extract_slots_rule_based(question, now=...)`
4. `classify_intent_rule_based(question, slots)`
5. 모호한 경우에만 LLM 기반 SQL intent classifier 보조 사용
6. `resolve_metric`, `resolve_versions`, `resolve_filters`, `resolve_periods`, `resolve_period_groups`
7. `infer_default_period(...)`로 기간 미지정 질문 보정
8. `resolve_period_slots(...)`로 실제 적용 기간 계산
9. `canonicalize_plan(...)`로 builder 입력을 공통 plan으로 정리
10. `build_sql_from_plan(...)`로 common plan 기반 SQL 조립
11. `build_execution_plan(...)`로 primary/aux 조회 계획 생성
12. 실행 결과를 `render_answer_rule_based()` 또는 `render_answer_with_llm()`로 응답화

모호한 질문이어도 바로 실패시키기보다, 실행 가능한 기본값으로 먼저 보정하는 것이 현재 3차 개선의 기본 정책입니다.

## `/sql` 해석 규칙

### Metric
- `판매`, `판매량`, `매출`, `실적` -> `sales`
- `sales`, `qty`, `quantity`, `출하` -> `sales`
- `순생산`, `생산` -> `net_prod`
- `production`, `production qty`, `생산량` -> `net_prod`
- `순입고`, `입고` -> `net_ipgo`

### Period
- `2월` 같은 월 단독 표현은 현재 시점 기준으로 연도를 보정합니다.
- `올해`, `작년`, `이번달`, `지난달`, `이번 분기`, `전분기`, `최근 N개월`을 해석합니다.
- `2월~4월`, `2월 3월 4월`, `최근3개월` 같은 붙여쓰기/범위 표현도 정규화 후 해석합니다.
- 답변 기준에는 질문 표현이 아니라 실제 해석된 기간이 노출됩니다.
  - 예: `기준 기간: 2026년 2월`
  - 예: `해석 기간: 2026년 2월 (질문 표현: 2월)`

### 기본값 추론 정책
- `total`: 기간이 없으면 최신 완결 월 기준
- `trend`: 기간이 없으면 최근 3개월 추이 기준
- `compare`: 비교 대상이 불명확하면 최신 월 vs 전월 기준
- `grouped`: 기간이 없으면 최신 완결 월 기준, 그룹 차원이 비어 있으면 질문에 암시된 대표 차원 우선

예:
- `/sql vh 판매 몇개야` -> `VH`, `판매`, `최신 완결 월`
- `/sql 판매 추이 보여줘` -> `판매`, `최근 3개월`
- `/sql 버전별 판매 알려줘` -> `버전별`, `판매`, `최신 완결 월`
- `/sql dram 순생산 비교` -> `FAM1=DRAM`, `순생산`, `최신 월 vs 전월`

### Dimension / Filter
- `FAM1 DRAM`, `APP MOBILE`처럼 차원명과 값이 함께 오면 필터로 해석합니다.
- `DRAM`, `MOBILE`처럼 차원값만 단독으로 와도 `sample_values` / `value_aliases` 기반으로 필터로 우선 해석합니다.
- 차원 필터로 해석된 값은 version 후보에서 제거합니다.

### Version
- `VH`, `VL`, `WC` 같은 버전 코드는 version 후보로 인식합니다.
- `vh`, `V/H`, `vl`, `V/L`, `wc`, `W/C`도 같은 canonical version으로 정규화합니다.
- 다중 버전이면 비교형 의도로 승격될 수 있습니다.

### Normalization / Synonym
- 붙여쓰기, 혼합 언어, 구두점 표현은 먼저 NLU용 normalized form으로 정리합니다.
  - 예: `/sql 2월vh판매몇개야` -> `2 월 vh 판매`
  - 예: `/sql vh/vl 비교` -> `vh 비교 vl 비교`
  - 예: `/sql vh sales trend` -> `vh 판매 추이`
- 조사와 군더더기 표현은 slot 해석 전에 제거합니다.
  - 예: `알려줘`, `보여줘`, `몇개야`, `부탁해`
- 목표는 질문별 예외처리가 아니라, 서로 다른 표현을 동일 canonical plan으로 수렴시키는 것입니다.
- 질문이 다소 모호해도 canonical plan 단계에서 실행 가능한 기본값을 붙여 재질문 없이 답변하는 방향을 우선합니다.

## `sql_registry.yaml` 작성 규칙
신규 확장은 질문별 `queries` 추가가 아니라 아래 메타데이터 + common plan 조합을 우선 사용합니다.

- `sources`: 테이블, 기준 컬럼, snapshot 컬럼, 기본 필터
- `metrics`: 집계 대상 컬럼
  - metric별 `semantic_type`, `default_aggregation`, `allowed_aggregations` 지정 가능
  - `weighted_avg` 확장을 위한 `numerator_column`, `denominator_column` 필드 예약
  - `%` metric은 `percent_scale`(`fraction|percent`)로 표시 스케일을 정의
    - `fraction`: DB `0.4` -> `40.0%`
    - `percent`: DB `40` -> `40.0%`
- `dimensions`: 필터/그룹 차원
- `query_families`: planner가 사용하는 공통 분석 패턴

`queries` 섹션은 legacy fallback 용도로만 유지합니다. 새 질문을 처리하기 위해 query를 질문별로 계속 추가하는 방식은 지양합니다.

### Common plan 구조
planner 내부 공통 plan은 아래 축으로 정리됩니다.

- `metric`
- `aggregation`
- `periods`
- `filters`
- `group_by`
- `analysis_type`
- `versions`
- `compare_target(optional)`

현재 `analysis_type`은 아래 공통 패턴을 사용합니다.
- `total`
- `trend`
- `compare`
- `grouped`

기간 그룹 비교는 `analysis_type=compare`에 `period_groups`가 같이 있는 형태로 처리되고, 내부 동적 query id는 `compare_groups`를 사용합니다.

### Source 예시
```yaml
sources:
  psi_simul:
    table: mst_psi_simul_report
    period_column: YEARMONTH
    quarter_column: QUARTER
    snapshot_column: WORKDATE
    version_column: VERSION
    default_filters:
      TYPE: "P"
      P_MODULE: "PMIX"
      S_MODULE: "SOKBO"
```

설명:
- `period_column`: 월/연 범위 기본 컬럼
- `quarter_column`: 분기 질의 전용 컬럼
- 분기 질의일 때만 planner가 `QUARTER` 컬럼과 `anchor_quarter/start_quarter/end_quarter` 바인딩을 사용합니다.

### Query Family 예시
```yaml
query_families:
  total:
    source: psi_simul
    description: "공통 total plan"
  trend:
    source: psi_simul
    description: "공통 trend plan"
  compare:
    source: psi_simul
    description: "공통 compare plan"
  compare_groups:
    source: psi_simul
    description: "공통 compare plan for period groups"
  grouped:
    source: psi_simul
    description: "공통 grouped plan"
```

### Dimension 예시
```yaml
dimensions:
  fam1:
    source: psi_simul
    column: FAM1
    aliases: ["fam1", "패밀리1", "family1", "fam1별"]
    supports_filter: true
    supports_groupby: true
    value_mode: "catalog_or_free_text"
    sample_values: ["DRAM", "NAND", "FLASH"]
    value_aliases:
      dram: "DRAM"
      nand: "FLASH"
      flash: "FLASH"
```

설명:
- `sample_values`: 단독 값 등장 시 차원 필터 추론에도 사용
- `value_aliases`: 사용자 표현을 실제 필터 값으로 정규화

## 현재 지원하는 주요 질의 유형
- 단일 기간 합계
  - `/sql 2월 vh 판매 알려줘`
  - `/sql 올해 dram 순생산 알려줘`
  - `/sql 2월vh판매몇개야`
  - `/sql vh 판매 몇개야`
- 기간 범위 합계
  - `/sql 올해 vh 판매량`
- 버전 비교
  - `/sql 2월 vh, vl 순생산 비교해줘`
  - `/sql vh,vl 비교`
  - `/sql vh/vl 비교`
  - `/sql dram 순생산 비교`
- 기간 추이
  - `/sql 2월 3월 4월 vh 트렌드 분석해줘`
  - `/sql vh sales trend`
  - `/sql 판매 추이 보여줘`
- 기간 그룹 비교
  - `/sql vh 25년 대비 26년 순입고 비교 분석해줘`
- 차원 그룹화
  - `/sql vh 기준 fam1별 순생산 보여줘`
  - `/sql fam1별 vh 생산 보여줘`
  - `/sql 버전별 판매 알려줘`

위 질문들은 모두 별도 query를 새로 추가하지 않고 `normalize + synonym canonicalization + common plan + metadata builder` 조합으로 처리하는 것이 기본입니다.
표현이 조금 달라도 같은 의미면 동일 canonical plan으로 정규화되도록 유지하는 것이 현재 `/sql` 자동화 2차 개선의 핵심입니다.
3차 개선에서는 여기에 기본값 추론을 더해, period/group/compare 대상이 일부 빠져도 실행 가능한 plan으로 보정합니다.

## SQL 답변 정책
`app/sql_answering.py`는 결과를 3개 섹션으로 만듭니다.

- `📌 한줄 요약`
  - 단순 재진술이 아니라 우세 항목, 변동 방향, 격차 수준을 포함하도록 구성
- `📊 데이터 기반 답변`
  - 핵심 수치 2개 이상을 포함
- `🧭 해석 기준`
  - 기준 기간
  - 해석 기간
  - 적용 필터
  - 버전 기준
  - 분석 차원
  - 집계 방식
  - 기준 source
  - 필요 시 기본값 적용 내역과 비교 기준

예:
- 비교형 답변은 절대 차이, 상대 격차, 상위 비중을 설명합니다.
- 추이형 답변은 최고/최저 시점, 평균, 고저 차이를 설명합니다.
- 합계형 답변도 metric별 라벨을 사용합니다.
  - `판매 합계`
  - `순생산 합계`
  - `순입고 합계`
- ratio metric(`unit=%`)은 퍼센트 포맷 규칙을 강제합니다.
  - `%` metric에는 `개` 단위를 붙이지 않습니다.
  - `semantic_type=ratio`에서 집계 미지정 시 `avg`로 해석합니다.
  - `%` metric에 `sum`이 들어오면 경고 후 `avg`로 fallback 합니다.
  - `percent_scale=fraction`이면 `0.4 -> 40.0%`, `percent`면 `40 -> 40.0%`로 표시합니다.
- 기본값이 적용된 경우 답변 하단에 짧게 명시합니다.
  - 예: `기본값 적용: 기간 지정이 없어 최신 완결 월(2026-02) 기준으로 조회했습니다.`
  - 예: `비교 기준: 최신 월 vs 전월`

## Prefix 정책
- `/sql ...`: SQL 질의
- `/용어 ...`: 용어 조회
- 그 외 일반 텍스트: 기본 LLM/RAG/Hybrid 라우팅

## 일반 질의 Intent 규칙
- `doc_nav`: 문서 제목/목록/최근/최신/금주 학습 문서 탐색 질문
  - 예: `최근 문서 제목 알려줘`, `금주에 학습한 문서 알려줘`
  - 응답은 최신순 `title/date/url` 목록 중심으로 제공
  - `학습한 문서` 계열 표현은 `ingested_at` 계열 필드를 우선 기준으로 기간 필터
- `doc_summary`: 주차 맥락이 있는 보고/문서 요약 질문
  - 예: `지난주 주간 보고에서 ... 정리해줘`, `이번주 주간 보고 기준 ... 정리해줘`
  - `주간 보고/주간보고/보고서` 표현도 weekly rerank 규칙 대상에 포함
- 위 규칙 외 문서성 질문은 기존 `rag_only`/`hybrid` fallback 유지

## RAG 조회 정책
- RAG HTTP timeout은 `requests` tuple timeout 기준으로 `connect=5초`, `read=60초`를 사용합니다.
- 1차 조회는 기존 query/index/top_k 설정을 그대로 사용합니다.
- `requests.exceptions.ReadTimeout` 발생 시 즉시 실패시키지 않고 warning 로그를 남긴 뒤 fallback 재조회합니다.
- 2차 fallback은 우선순위가 가장 높은 단일 index만 대상으로 재조회합니다.
- fallback 시 `top_k`는 절반 수준으로 축소합니다. 최소값은 `1`입니다.
- 1차에서 timeout이 일부 index에서만 발생해도 fallback은 수행합니다.
- fallback도 결과가 없지만 1차에서 일부 문서를 이미 받았다면, 완전 실패 대신 1차 partial result를 유지합니다.

## 검색 로직 상세 (중요)
일반 질의(`_process_llm_chat_background_impl`) 기준 검색 흐름은 아래 순서입니다.

1. 질문 원문 `question` 수신
2. 기간 추출: `_extract_time_range_from_question(question)`
   - `YYYY년 M월`, `올해/작년 M월`, `이번주/저번주`, `최근 N일/N주/N개월`
   - **연도 생략 월 표현(`3월`, `3월달`)도 현재연도로 보정**
3. 컨텍스트 보강 질의 생성: `_build_effective_question(...)`
   - 직전 대화 state(topic/time_label)로 후속 질문 보완
4. 검색 질의 확장: `build_search_queries(effective_question, ...)`
5. RAG 병렬 조회: `retrieve_rag_documents_parallel(...)`
6. 인덱스 분리
   - mail index: `MAIL_INDEX_NAME`
   - glossary index: `GLOSSARY_INDEX_NAME`
7. 시간 필터 적용(메일)
   - `_filter_docs_by_datetime_range(start,end)`
   - 기간 내 문서가 없으면 직전 14일 확장 1회 재시도
   - 그래도 없으면 원본 fallback 없이 빈 결과 유지
8. 재랭킹: `rerank_rag_documents(...)`
   - 벡터점수 + 최신성 점수 + query_hit_bonus 결합
   - `prefer_recent`이면 최신시각 우선 정렬
9. 의도별 최종 도메인 선택
   - `mail` / `glossary` / `combined`
10. relevance gate 통과 문서만 최종 컨텍스트로 사용

### 날짜 필터 기준 필드
- 기본: `_extract_doc_datetime` (`created_time`, `updated_time`, title 내 date 등)
- 학습문서 탐색(doc_nav learning mode): `_extract_doc_ingested_datetime` (`ingested_at` 우선)

## "3월 VT SBL 회의"가 25년 12월로 튀는 대표 원인
아래 케이스가 실제로 오탐을 유발합니다.

1) 질의에서 월 파싱 실패
- 예: `3월달 VT SBL 회의내용 정리해줘`
- 월 파싱이 안 되면 기간 필터가 적용되지 않아 과거 문서가 점수로 역전될 수 있음
- 이번 업데이트에서 `3월/3월달` 패턴을 직접 보강함

2) 문서 날짜 메타가 비어있거나 비표준
- 기간 필터 시 날짜 추출 실패 문서는 제외됨
- 하지만 기간 필터가 아예 없는 질의에서는 오래된 문서도 벡터 유사도만으로 상위에 남을 수 있음

3) 검색어 확장(rewrite)로 핵심 토큰이 약해지는 경우
- `VT SBL` 같은 핵심 토큰이 약화되면 semantic 유사 문서(오래된 회의 공지)가 상위로 갈 수 있음

## 검색 개선 방안 (권장 적용 순서)
1. **기간의도 강제 필터 강화**
   - 월/주/최근 표현 감지 시, 해당 범위를 벗어난 문서는 최종 후보에서 제거(soft가 아니라 hard)

2. **토큰 앵커 가중치 추가**
   - 질문 내 영문 대문자 토큰(`VT`, `SBL`)이 title/content에 정확히 매칭되면 가중치↑
   - 미매칭이면 penalty

3. **최신성 우선 질의의 recency weight 상향** (적용)
   - `최근/최신/이번달/이번주`가 있으면 `RAG` relevance 임계치를 완화하고 recency 가중치를 동적 상향
   - 기간의도 질의는 relevance threshold를 완화해 관련 문서가 불필요하게 탈락하지 않도록 처리

4. **월 표현 질의 확장** (적용)
   - `3월`, `3월달` 같은 표현은 deterministic query variant에서 `3.` 패턴 변형도 함께 생성

5. **월 단위 질의 fallback 정책 분리**
   - 월 의도는 14일 확장 fallback을 더 보수적으로(예: 같은 월 내만 허용)

5. **디버그 로그 표준화**
   - 매 질의에 아래를 로그로 남겨 원인 추적 단축
     - `effective_question`
     - `extracted_time_range`
     - `ranged_docs_count`
     - `top_docs_date/title`

6. **회귀 테스트 케이스 추가**
   - `3월달 VT SBL 회의내용 정리해줘` => 3월 문서 우선
   - 동일 키워드가 25년12월/26년3월 모두 있을 때 3월이 상단 유지

## RAG 사용자 안내 / 메타데이터
- RAG 병렬 조회 결과는 문서 목록과 별도로 timeout/fallback/user notice 메타데이터를 함께 수집합니다.
- timeout fallback이 실제로 발생하면 최종 사용자 응답 앞에 아래 안내를 붙일 수 있습니다.
  - `문서 검색 응답이 지연되어 범위를 줄여 재조회했습니다.`
- query log의 `debug_json`에는 아래 항목이 기록됩니다.
  - `rag_timeout_occurred`
  - `rag_fallback_used`
  - `rag_user_notice`
  - `rag_query_meta`

## 운영 환경변수

### 서버 / Knox
- `BIND_HOST`
- `BIND_PORT`
- `KNOX_HOST`
- `KNOX_SYSTEM_ID`
- `KNOX_TOKEN`

### Oracle
- `ORACLE_HOST`
- `ORACLE_PORT`
- `ORACLE_SERVICE`
- `ORACLE_USER`
- `ORACLE_PW`

### 대화 / UI
- `ENABLE_RECALL`
- `ENABLE_FEEDBACK_CARD`
- `LLM_CHAT_DEFAULT_MODE`
- `ENABLE_CONVERSATION_MEMORY`

### RAG / LLM
- `LLM_API_KEY`
- `LLM_API_URL`
- `LLM_MODEL_NAME`
- `RAG_BASE_URL`
- `RAG_INDEXES`

## 주요 로그 Prefix
- `[LLM]`
- `[MEMORY]`
- `[RAG]`
- `[RAG Search]`
- `[RAG Retrieve INFO]`
- `[RAG Retrieve WARN]`
- `[RAG Retrieve ERROR]`
- `[SQL_NLU]`
- `[SQL_PLAN]`
- `[SQL_EXEC]`
- `[SQL_RESULT]`
- `[SQL_ANSWER]`
- `[INTENT]`
- `[HYBRID]`
- `[FEEDBACK]`
- `[ORACLE]`

## 실행
```bash
pip install -r requirements.txt
uvicorn gocllm3:app --host 0.0.0.0 --port 9500
```

## 테스트
대표 SQL NLU/렌더링 검증:

```bash
pytest -q /workspaces/gocllm3/tests
```

이 테스트는 현재 다음 내용을 검증합니다.
- 기간 해석
- metric / version / filter 슬롯 추출
- direct dimension value 추론
- quarter column 사용
- 답변 렌더링 문구와 기준 섹션
- RAG timeout fallback과 병렬 조회 메타데이터 집계

## 문서 업데이트 규칙
코드 수정 시 `README.md`도 함께 업데이트합니다. 특히 아래 변경은 반드시 반영합니다.

1. `/sql` 해석 규칙 변경
2. `sql_registry.yaml` 스키마 변경
3. 답변 렌더링 방식 변경
4. RAG timeout / fallback / 로그 정책 변경
5. 신규 env var 추가
6. 실행/테스트 명령 변경
