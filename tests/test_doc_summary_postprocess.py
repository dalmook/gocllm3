from app.doc_summary_postprocess import enrich_sparse_issue_lines


def test_enrich_sparse_issue_lines_replaces_lv_only_bullets():
    answer = """📌 한줄 요약
- 주요 관리 이슈가 지속 중입니다.

📂 핵심 이슈(최신순)
- (2026-03-09 08:00) LV1 관리 이슈가 지속중
- (2026-03-08 09:00) LV2 관리 이슈가 진행중

📂 근거 문서
- 문서 A | 2026-03-09 | 링크
"""
    docs = [
        {
            "_doc_date": "2026-03-09 08:00",
            "title": "W11 Mobile 이슈",
            "content": "AP 공급 지연으로 모바일 주력 모델 출하 일정이 하루씩 순연되고 있으며, 고객사 공유 후 대체 재고를 우선 투입 중입니다.",
        },
        {
            "_doc_date": "2026-03-08 09:00",
            "title": "W11 Server 이슈",
            "content": "서버향 부품 승인 지연으로 양산 전환 일정 검토가 필요하며 품질팀과 함께 승인 일정을 재조정하고 있습니다.",
        },
    ]

    enriched = enrich_sparse_issue_lines(answer, docs)

    assert "LV1 관리 이슈가 지속중" not in enriched
    assert "AP 공급 지연으로 모바일 주력 모델 출하 일정이 하루씩 순연" in enriched
    assert "서버향 부품 승인 지연으로 양산 전환 일정 검토가 필요" in enriched


def test_enrich_sparse_issue_lines_keeps_specific_bullets():
    answer = """📂 핵심 이슈(최신순)
- (2026-03-09 08:00) AP 공급 지연으로 모바일 주력 모델 출하 일정이 하루씩 순연되고 있습니다.
"""
    docs = [
        {
            "_doc_date": "2026-03-09 08:00",
            "title": "W11 Mobile 이슈",
            "content": "AP 공급 지연으로 모바일 주력 모델 출하 일정이 하루씩 순연되고 있습니다.",
        }
    ]

    assert enrich_sparse_issue_lines(answer, docs) == answer
