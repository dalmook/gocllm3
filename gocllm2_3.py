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
    sender_knox = task.get("sender_knox") or ""
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

        prefer_general = should_prefer_general_llm(question)
        if prefer_general:
            from langchain_core.messages import SystemMessage, HumanMessage

            fallback_system_prompt = """
당신은 GOC 업무 지원 챗봇입니다.
이번 질문은 일반 지식/실시간 성격의 질문으로 판단하여 문서 검색 없이 일반 LLM 답변으로 안내합니다.
과도한 추측은 피하고, 불확실한 내용은 단정하지 마세요.

답변 형식
📌 한줄 요약
한 문장 요약

✅ 일반 답변
- 핵심 내용 2~5개

⚠️ 참고
- 이번 답변은 문서 기반이 아니라 일반 답변임을 짧게 안내
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
            chatBot.send_text(chatroom_id, f"🤖 {format_for_knox_text(answer)}")
            save_conversation_memory(
                scope_id=scope_id,
                room_id=str(chatroom_id),
                user_id=sender_knox,
                role="assistant",
                content=answer,
                chat_type=chat_type,
            )
            try:
                save_conversation_state(
                    scope_id,
                    topic=_extract_topic_from_question(question),
                    time_label=_extract_time_label_from_question(question, _extract_time_range_from_question(question)),
                    last_query=question,
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
                    f"llm={perf.get('llm_ms', 0):.0f}ms"
                )
            return stats

        time_range = _extract_time_range_from_question(question)
        effective_question, ctx_state = _build_effective_question(question, scope_id=scope_id, time_range=time_range)

        normalized_query = normalize_query_for_search(effective_question)
        glossary_intent = is_glossary_intent(effective_question)
        force_glossary = is_force_glossary_query(effective_question)
        print(f"[RAG] original question={question}")
        print(f"[RAG] effective question={effective_question}")
        print(f"[RAG] normalized query={normalized_query}")
        print(f"[RAG] glossary_intent={glossary_intent}")
        print(f"[RAG] force_glossary={force_glossary}")

        t_rewrite = time.perf_counter()
        search_queries = build_search_queries(effective_question, llm, memory_text=memory_text, use_memory_for_rewrite=use_memory_for_rewrite)
        perf["rewrite_ms"] = (time.perf_counter() - t_rewrite) * 1000
        print(f"[RAG] search queries: {search_queries}")
        issue_summary_intent = is_issue_summary_intent(effective_question)
        strong_mail_intent = has_strong_mail_intent(effective_question)
        prefer_recent_docs = bool(time_range) or should_prioritize_recent_docs(effective_question) or issue_summary_intent
        retrieve_top_k = RAG_NUM_RESULT_DOC
        if time_range:
            retrieve_top_k = max(RAG_NUM_RESULT_DOC, RAG_TEMPORAL_NUM_RESULT_DOC)
        elif issue_summary_intent:
            retrieve_top_k = max(RAG_NUM_RESULT_DOC, RAG_API_MAX_NUM_RESULT_DOC)

        target_indexes = None
        if strong_mail_intent or issue_summary_intent:
            # 이슈/요약류 질문은 glossary 잡음을 줄이기 위해 메일 인덱스 우선
            target_indexes = [MAIL_INDEX_NAME]

        print(
            f"[RAG] prefer_recent_docs={prefer_recent_docs} issue_summary_intent={issue_summary_intent} "
            f"strong_mail_intent={strong_mail_intent} time_range={(time_range or {}).get('label') if time_range else 'none'} "
            f"top_k={retrieve_top_k} indexes={target_indexes or 'default'}"
        )

        t_rag = time.perf_counter()
        all_rag_documents = retrieve_rag_documents_parallel(
            search_queries,
            top_k=retrieve_top_k,
            indexes=target_indexes,
        )
        perf["rag_fetch_ms"] = (time.perf_counter() - t_rag) * 1000
        stats["rag_calls"] = len(search_queries)

        all_mail_docs = [d for d in all_rag_documents if d.get("_index") == MAIL_INDEX_NAME]
        all_glossary_docs = [d for d in all_rag_documents if d.get("_index") == GLOSSARY_INDEX_NAME]

        if time_range and all_mail_docs:
            ranged_mail_docs = _filter_docs_by_datetime_range(
                all_mail_docs,
                time_range["start"],
                time_range["end"],
            )
            if ranged_mail_docs:
                print(
                    f"[RAG] 메일 기간 필터 적용: {time_range['label']} "
                    f"{time_range['start']}~{time_range['end']} docs={len(ranged_mail_docs)}"
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

        t_rerank = time.perf_counter()
        reranked_mail_docs = rerank_rag_documents(all_mail_docs, prefer_recent=prefer_recent_docs)[:RAG_NUM_RESULT_DOC]
        reranked_glossary_docs = rerank_rag_documents(all_glossary_docs, prefer_recent=prefer_recent_docs)[:RAG_NUM_RESULT_DOC]
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
            elif mail_docs and is_rag_result_relevant(effective_question, mail_docs):
                selected_rag_domain = "mail"
                mail_match = True
                rag_relevant = True
                selected_docs = mail_docs
                rag_context = format_rag_context(mail_docs, max_docs=RAG_CONTEXT_DOCS)
        elif mail_docs and is_rag_result_relevant(effective_question, mail_docs):
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
            skip_rag = top_score < RAG_SIMILARITY_THRESHOLD
            rag_relevant = (not skip_rag) and is_rag_result_relevant(effective_question, top_docs)
            if rag_relevant:
                selected_docs = top_docs
                rag_context = format_rag_context(top_docs, max_docs=RAG_CONTEXT_DOCS)

        print(
            f"[RAG Domain Selection] selected_rag_domain={selected_rag_domain}, "
            f"glossary_intent={glossary_intent}, force_glossary={force_glossary}, mail_match={mail_match}, glossary_match={glossary_match}"
        )

        if rag_context and rag_relevant:
            from langchain_core.messages import SystemMessage, HumanMessage
            stats["used_rag"] = True

            if issue_summary_intent and ISSUE_SUMMARY_SPEED_MODE:
                system_prompt = f"""
당신은 GOC 업무 지원 챗봇입니다. 아래 [검색 문서]만 근거로 아주 간결하게 답하세요.

규칙
1) 최신 문서일시 순(내림차순)으로 정렬
2) 문서에 없는 내용은 추측하지 않음
3) 답변은 짧게: 한줄요약 1개 + 핵심 3개 + 근거 2개

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

            messages = [SystemMessage(content=system_prompt)]
            if memory_text:
                messages.append(HumanMessage(content=f"[최근 대화 메모리]\n{memory_text}"))
            messages.append(HumanMessage(content=f"[RAG context]\n{rag_context}"))
            messages.append(HumanMessage(content=question))
            t_llm = time.perf_counter()
            response = llm_invoke_with_retry(llm, messages, attempts=1, base_delay=1.5)
            perf["llm_ms"] = (time.perf_counter() - t_llm) * 1000
            stats["llm_calls"] += 1
            answer = response.content.strip()

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
            if skip_rag:
                reason = f"검색 문서 유사도가 기준치({RAG_SIMILARITY_THRESHOLD})보다 낮았습니다."
            elif (mail_docs or glossary_docs) and not rag_relevant:
                reason = "검색 문서는 있었지만 질문과의 관련성이 낮았습니다."
            stats["fallback_reason"] = reason
            answer = f"📋 문서 기반 답변 미적용\n- {reason}\n- 아래는 일반 LLM 답변입니다.\n\n" + response.content.strip()

        print(
            f"[RAG Final] selected_rag_domain={selected_rag_domain} used_rag={stats['used_rag']} "
            f"fallback_reason={stats.get('fallback_reason','')}"
        )
        chatBot.send_text(chatroom_id, f"🤖 {format_for_knox_text(answer)}")
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
    dsn = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, service_name=ORACLE_SERVICE)
    con = cx_Oracle.connect(user=ORACLE_USER, password=ORACLE_PW, dsn=dsn, encoding="UTF-8")
    try:
        return pd.read_sql(sql, con, params=params)
    finally:
        try:
            con.close()
        except Exception:
            pass

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

