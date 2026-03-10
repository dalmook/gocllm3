import unittest

from app.hybrid_router import build_hybrid_search_queries, build_route_decision
from app.query_intent import classify_query_intent
from app.sql_registry import SQLRegistryItem, SQLRegistryMatch, SQLResultSpec


def _dummy_match(score: float = 30.0) -> SQLRegistryMatch:
    item = SQLRegistryItem(
        id="total",
        description="dummy",
        sql="SELECT 1 FROM DUAL",
        params={},
        result=SQLResultSpec(mode="table", field=""),
        keywords=[],
        patterns=[],
        intent="sales_total",
    )
    return SQLRegistryMatch(item=item, score=score)


class HybridRoutingTest(unittest.TestCase):
    def _trace(self, *, metric="sales", versions=None, periods=None, filters=None, analysis_type="total"):
        return {
            "planner_plan": {
                "metric": metric,
                "versions": versions or [],
                "periods": periods or [],
                "filters": filters or {},
                "analysis_type": analysis_type,
            },
            "final_slots": {
                "metric": metric,
                "versions": versions or [],
                "periods": periods or [],
                "filters": filters or {},
                "analysis_type": analysis_type,
            },
        }

    def test_router_prefers_data_only_for_numeric_question(self):
        intent = classify_query_intent(
            "2월 vh 판매 몇개야",
            "2월 vh 판매 몇개야",
            _dummy_match(),
            prefer_general=False,
            sql_trace=self._trace(versions=["VH"], periods=["202602"]),
        )
        self.assertEqual("data_only", intent)

    def test_router_uses_hybrid_for_numeric_and_issue_question(self):
        intent = classify_query_intent(
            "2월 vh 판매 어때? 관련 이슈도 같이",
            "2월 vh 판매 어때? 관련 이슈도 같이",
            _dummy_match(),
            prefer_general=False,
            sql_trace=self._trace(versions=["VH"], periods=["202602"]),
        )
        self.assertEqual("hybrid", intent)

    def test_router_uses_hybrid_for_reason_question(self):
        intent = classify_query_intent(
            "dram 순생산 떨어진 이유 뭐야",
            "dram 순생산 떨어진 이유 뭐야",
            _dummy_match(),
            prefer_general=False,
            sql_trace=self._trace(metric="net_prod", filters={"fam1": ["DRAM"]}),
        )
        self.assertEqual("hybrid", intent)

    def test_router_allows_rag_or_hybrid_for_issue_question(self):
        intent = classify_query_intent(
            "vh 이번주 이슈 있어?",
            "vh 이번주 이슈 있어?",
            _dummy_match(),
            prefer_general=False,
            sql_trace=self._trace(versions=["VH"]),
        )
        self.assertIn(intent, {"rag_only", "hybrid"})

    def test_router_allows_rag_or_hybrid_for_bad_result_reason_question(self):
        intent = classify_query_intent(
            "wc 버전 실적 안좋은 원인 있나",
            "wc 버전 실적 안좋은 원인 있나",
            _dummy_match(),
            prefer_general=False,
            sql_trace=self._trace(metric="sales", versions=["WC"]),
        )
        self.assertIn(intent, {"rag_only", "hybrid"})

    def test_hybrid_query_expansion_uses_sql_context(self):
        queries = build_hybrid_search_queries(
            "2월 vh 판매 어때? 관련 이슈도 같이",
            ["vh 판매 관련 이슈"],
            sql_context={
                "slots": {"metric": "sales", "versions": ["VH"]},
                "period": {"label": "2026년 2월", "start_yyyymm": "202602", "end_yyyymm": "202602"},
            },
            sql_summary={"summary": "2026년 2월 VH 판매은 120MEQ"},
        )
        merged = " | ".join(queries)
        self.assertIn("VH", merged)
        self.assertIn("판매", merged)
        self.assertTrue("2026-02" in merged or "2026년 2월" in merged)
        self.assertTrue("이슈" in merged or "보고" in merged)

    def test_data_only_without_sql_match_falls_back_to_rag_route(self):
        route = build_route_decision("data_only", None)
        self.assertEqual("rag_only", route.intent)
        self.assertFalse(route.use_sql)
        self.assertTrue(route.use_rag)


if __name__ == "__main__":
    unittest.main()
