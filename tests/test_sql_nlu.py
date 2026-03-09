import unittest
from datetime import datetime

from app.sql_registry import analyze_sql_question, build_sql_params_with_missing


class SqlNluTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 3, 9)

    def _analyze(self, q: str):
        return analyze_sql_question(q, now=self.now)

    def test_core_cases(self):
        cases = [
            ("2월 WC 버전 판매 몇개야", "sales_total"),
            ("올해 WC 판매량 얼마야", "sales_total"),
            ("이번 분기 WC 몇개", "sales_total"),
            ("전분기 대비 WC 판매량 어때", "sales_compare"),
            ("올해 월별 WC 판매 추이", "sales_trend"),
            ("1분기 버전별 판매량", "sales_grouped"),
            ("VH 판매", "sales_total"),
            ("올해 몇개", "sales_total"),
            ("2월 실적", "sales_total"),
            ("최근 3개월 판매 추이", "sales_trend"),
        ]
        for q, expected_intent in cases:
            tr = self._analyze(q)
            self.assertEqual(expected_intent, tr.get("final_intent"), msg=q)
            self.assertTrue(tr.get("selected_query_id"), msg=q)

    def test_spacing_variations(self):
        cases = ["올해몇개", "이번분기판매량", "WC올해판매", "버전별판매량"]
        for q in cases:
            tr = self._analyze(q)
            self.assertTrue(tr.get("selected_query_id"), msg=q)

    def test_synonyms(self):
        for q in ["올해 판매", "올해 판매량", "올해 매출", "올해 실적"]:
            tr = self._analyze(q)
            self.assertEqual("sales_total", tr.get("final_intent"), msg=q)

    def test_period_resolution(self):
        tr = self._analyze("이번 분기 WC 몇개")
        p = tr.get("resolved_period") or {}
        self.assertEqual("202601", p.get("start_yyyymm"))
        self.assertEqual("202603", p.get("end_yyyymm"))

        tr2 = self._analyze("전분기 대비 WC 판매량")
        p2 = tr2.get("resolved_period") or {}
        self.assertEqual("202510", p2.get("compare_start_yyyymm"))
        self.assertEqual("202512", p2.get("compare_end_yyyymm"))

    def test_missing_slot_fallback(self):
        tr = self._analyze("판매량")
        self.assertTrue(tr.get("selected_query_id"))
        m = tr.get("match")
        self.assertIsNotNone(m)
        params, missing = build_sql_params_with_missing(m, "판매량")
        self.assertEqual([], missing)
        self.assertTrue(params.get("start_yyyymm"))
        self.assertTrue(params.get("end_yyyymm"))


if __name__ == "__main__":
    unittest.main()
