import unittest
from datetime import datetime

from app.sql_answering import render_answer_rule_based
from app.sql_registry import (
    analyze_sql_question,
    build_execution_plan,
    build_sql_params_with_missing,
)


class FakeDF:
    def __init__(self, rows):
        self._rows = rows
        self.empty = len(rows) == 0

    def to_dict(self, orient="records"):
        if orient != "records":
            return {}
        return list(self._rows)


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
            ("올해 월별 WC 판매 추이", "metric_trend_by_period"),
            ("1분기 버전별 판매량", "metric_grouped_dimension"),
            ("VH 판매", "sales_total"),
            ("올해 몇개", "sales_total"),
            ("2월 실적", "sales_total"),
            ("최근 3개월 판매 추이", "metric_trend_by_period"),
            ("vh와 vl 순생산 비교 분석해줘", "metric_compare_versions"),
            ("vh와 vl 판매 비교 분석해줘", "metric_compare_versions"),
            ("vh와 vl 순입고 비교 분석해줘", "metric_compare_versions"),
            ("2월 vh와 vl 순생산 비교 분석해줘", "metric_compare_versions"),
            ("version vh vs vl 판매 비교", "metric_compare_versions"),
            ("2월 3월 4월 vh 트렌드 분석해줘", "metric_trend_by_period"),
            ("2월 vh 판매 알려줘", "sales_total"),
            ("vh 25년 대비 26년 순입고 비교 분석해줘", "metric_compare_period_groups"),
            ("2월부터 5월까지 판매 트렌드 알려줘", "metric_trend_by_period"),
            ("작년 대비 올해 vl 판매 비교", "metric_compare_period_groups"),
            ("1분기 vl 순생산 추이 분석", "metric_trend_by_period"),
            ("vh vl 판매 차이 분석", "metric_compare_versions"),
            ("fam1별 판매 보여줘", "metric_grouped_dimension"),
            ("vh 기준 fam1별 순생산 보여줘", "metric_grouped_dimension"),
            ("FAM1 DRAM 순생산 알려줘", "sales_total"),
            ("26년 FAM1 순생산 알려줘", "metric_grouped_dimension"),
            ("26년 FAM1별 순생산 알려줘", "metric_grouped_dimension"),
            ("FAM1 DRAM VH와 VL 순생산 비교 분석해줘", "metric_compare_versions"),
            ("APP MOBILE 판매 알려줘", "sales_total"),
        ]
        for q, expected_intent in cases:
            tr = self._analyze(q)
            self.assertEqual(expected_intent, tr.get("final_intent"), msg=q)
            self.assertTrue(tr.get("selected_query_id"), msg=q)

    def test_default_period_inference(self):
        tr = self._analyze("VH 판매 몇개야?")
        self.assertEqual("sales_total", tr.get("final_intent"))
        self.assertTrue(tr.get("period_inferred"))
        self.assertIn("올해 누적", tr.get("period_infer_reason") or "")
        p = tr.get("resolved_period") or {}
        self.assertEqual("202601", p.get("start_yyyymm"))
        self.assertEqual("202612", p.get("end_yyyymm"))

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

    def test_quarter_column_is_used_for_quarter_queries(self):
        tr = self._analyze("이번 분기 WC 몇개")
        m = tr.get("match")
        self.assertIsNotNone(m)
        self.assertIn("QUARTER", m.item.sql)
        params, missing = build_sql_params_with_missing(m, "이번 분기 WC 몇개")
        self.assertEqual([], missing)
        self.assertEqual("2026Q1", params.get("anchor_quarter"))

    def test_execution_plan(self):
        tr = self._analyze("VH 판매 몇개야?")
        plan = build_execution_plan(
            "VH 판매 몇개야?",
            tr.get("final_intent") or "",
            tr.get("final_slots") or {},
            tr.get("selected_query_id") or "",
        )
        self.assertGreaterEqual(len(plan), 2)
        self.assertEqual("primary", plan[0].role)
        self.assertEqual("aux", plan[1].role)
        self.assertEqual("total", plan[0].query_id)

    def test_missing_slot_fallback(self):
        tr = self._analyze("판매량")
        self.assertTrue(tr.get("selected_query_id"))
        m = tr.get("match")
        self.assertIsNotNone(m)
        params, missing = build_sql_params_with_missing(m, "판매량")
        self.assertEqual([], missing)
        self.assertTrue(params.get("start_yyyymm") or params.get("anchor_yyyymm"))

    def test_rule_renderer_sentence_format(self):
        tr = self._analyze("VH 판매 몇개야?")
        answer = render_answer_rule_based(
            "VH 판매 몇개야?",
            intent=tr.get("final_intent") or "sales_total",
            slots=tr.get("final_slots") or {},
            period=tr.get("resolved_period") or {},
            results=[
                {"query_id": "sales_total_period_range", "role": "primary", "df": FakeDF([{"SALES": 1566.50128}])},
                {
                    "query_id": "sales_trend_monthly",
                    "role": "aux",
                    "df": FakeDF([
                        {"YEARMONTH": "202601", "SALES": 500.0},
                        {"YEARMONTH": "202602", "SALES": 600.0},
                        {"YEARMONTH": "202603", "SALES": 466.50128},
                    ]),
                },
            ],
            period_infer_reason=tr.get("period_infer_reason") or "",
        )
        self.assertIn("📌 한줄 요약", answer)
        self.assertIn("📊 데이터 기반 답변", answer)
        self.assertIn("🧭 해석 기준", answer)
        self.assertNotIn("SALES=", answer)

    def test_compare_versions_slots_and_params(self):
        tr = self._analyze("2월 vh와 vl 순생산 비교 분석해줘")
        self.assertEqual("metric_compare_versions", tr.get("final_intent"))
        slots = tr.get("final_slots") or {}
        self.assertEqual("net_prod", slots.get("metric"))
        self.assertEqual(["VH", "VL"], slots.get("versions"))
        self.assertTrue(slots.get("compare"))
        self.assertTrue(slots.get("analysis"))

        m = tr.get("match")
        self.assertIsNotNone(m)
        params, missing = build_sql_params_with_missing(m, "2월 vh와 vl 순생산 비교 분석해줘")
        self.assertEqual([], missing)
        self.assertEqual("VH", params.get("v1"))
        self.assertEqual("VL", params.get("v2"))
        self.assertTrue(params.get("anchor_yyyymm") or (params.get("start_yyyymm") and params.get("end_yyyymm")))

    def test_rule_renderer_shows_resolved_month_in_criteria(self):
        tr = self._analyze("2월 vh와 vl 순생산 비교 분석해줘")
        answer = render_answer_rule_based(
            "2월 vh와 vl 순생산 비교 분석해줘",
            intent=tr.get("final_intent") or "metric_compare_versions",
            slots=tr.get("final_slots") or {},
            period=tr.get("resolved_period") or {},
            results=[
                {
                    "query_id": "compare_versions_same_period",
                    "role": "primary",
                    "df": FakeDF([
                        {"VERSION": "VH", "VALUE": 120.0},
                        {"VERSION": "VL", "VALUE": 100.0},
                    ]),
                },
            ],
            period_infer_reason=tr.get("period_infer_reason") or "",
        )
        self.assertIn("기준 기간: 2026년 2월", answer)
        self.assertIn("해석 기간: 2026년 2월", answer)
        self.assertIn("질문 표현: 2월", answer)

    def test_rule_renderer_enriches_compare_summary_and_criteria(self):
        tr = self._analyze("2월 vh와 vl 순생산 비교 분석해줘")
        answer = render_answer_rule_based(
            "2월 vh와 vl 순생산 비교 분석해줘",
            intent=tr.get("final_intent") or "metric_compare_versions",
            slots=tr.get("final_slots") or {},
            period=tr.get("resolved_period") or {},
            results=[
                {
                    "query_id": "compare_versions_same_period",
                    "role": "primary",
                    "df": FakeDF([
                        {"VERSION": "VH", "VALUE": 140.0},
                        {"VERSION": "VL", "VALUE": 100.0},
                    ]),
                },
            ],
            period_infer_reason=tr.get("period_infer_reason") or "",
        )
        self.assertIn("격차는 +40.0%", answer)
        self.assertIn("절대 차이는 40 MEQ", answer)
        self.assertIn("기준 source", answer)

    def test_rule_renderer_enriches_total_answer(self):
        tr = self._analyze("VH 판매 몇개야?")
        answer = render_answer_rule_based(
            "VH 판매 몇개야?",
            intent=tr.get("final_intent") or "sales_total",
            slots=tr.get("final_slots") or {},
            period=tr.get("resolved_period") or {},
            results=[
                {"query_id": "sales_total_period_range", "role": "primary", "df": FakeDF([{"SALES": 1566.50128}])},
            ],
            period_infer_reason=tr.get("period_infer_reason") or "",
        )
        self.assertIn("평균 기준값", answer)
        self.assertIn("집계 대상 건수", answer)

    def test_rule_renderer_uses_metric_label_for_non_sales_total(self):
        tr = self._analyze("올해 dram 순생산 알려줘")
        answer = render_answer_rule_based(
            "올해 dram 순생산 알려줘",
            intent=tr.get("final_intent") or "sales_total",
            slots=tr.get("final_slots") or {},
            period=tr.get("resolved_period") or {},
            results=[
                {"query_id": "total_single_period", "role": "primary", "df": FakeDF([{"VALUE": 321.0}])},
            ],
            period_infer_reason=tr.get("period_infer_reason") or "",
        )
        self.assertIn("순생산 합계", answer)
        self.assertNotIn("판매량", answer)

    def test_trend_periods_slots_and_params(self):
        tr = self._analyze("2월 3월 4월 vh 트렌드 분석해줘")
        self.assertEqual("metric_trend_by_period", tr.get("final_intent"))
        self.assertEqual("trend", tr.get("selected_query_id"))
        slots = tr.get("final_slots") or {}
        self.assertEqual(["202602", "202603", "202604"], slots.get("periods"))
        self.assertEqual(["VH"], slots.get("versions"))
        self.assertTrue(slots.get("analysis"))

        m = tr.get("match")
        self.assertIsNotNone(m)
        params, missing = build_sql_params_with_missing(m, "2월 3월 4월 vh 트렌드 분석해줘")
        self.assertEqual([], missing)
        self.assertEqual("202602", params.get("p1"))
        self.assertEqual("202603", params.get("p2"))
        self.assertEqual("202604", params.get("p3"))
        self.assertEqual("VH", params.get("v1"))

    def test_compare_period_groups_slots_and_params(self):
        tr = self._analyze("vh 25년 대비 26년 순입고 비교 분석해줘")
        self.assertEqual("metric_compare_period_groups", tr.get("final_intent"))
        slots = tr.get("final_slots") or {}
        self.assertEqual("net_ipgo", slots.get("metric"))
        self.assertEqual(["VH"], slots.get("versions"))
        groups = slots.get("period_groups") or []
        self.assertEqual(2, len(groups))
        self.assertEqual("202501", groups[0].get("start_yyyymm"))
        self.assertEqual("202612", groups[1].get("end_yyyymm"))

        m = tr.get("match")
        self.assertIsNotNone(m)
        params, missing = build_sql_params_with_missing(m, "vh 25년 대비 26년 순입고 비교 분석해줘")
        self.assertEqual([], missing)
        self.assertEqual("202501", params.get("g1_start"))
        self.assertEqual("202512", params.get("g1_end"))
        self.assertEqual("202601", params.get("g2_start"))
        self.assertEqual("202612", params.get("g2_end"))

    def test_grouped_by_dimension_slots_and_params(self):
        tr = self._analyze("vh 기준 fam1별 순생산 보여줘")
        self.assertEqual("metric_grouped_dimension", tr.get("final_intent"))
        self.assertEqual("grouped", tr.get("selected_query_id"))
        slots = tr.get("final_slots") or {}
        self.assertEqual("net_prod", slots.get("metric"))
        self.assertEqual("fam1", slots.get("dimension"))
        self.assertEqual("fam1", slots.get("group_by"))
        self.assertEqual(["VH"], slots.get("versions"))

        m = tr.get("match")
        self.assertIsNotNone(m)
        params, missing = build_sql_params_with_missing(m, "vh 기준 fam1별 순생산 보여줘")
        self.assertEqual([], missing)
        self.assertEqual("VH", params.get("v1"))

    def test_dimension_value_filter_fam1_total(self):
        tr = self._analyze("FAM1 DRAM 순생산 알려줘")
        self.assertEqual("sales_total", tr.get("final_intent"))
        self.assertEqual("total", tr.get("selected_query_id"))
        slots = tr.get("final_slots") or {}
        self.assertEqual("net_prod", slots.get("metric"))
        self.assertEqual(["DRAM"], (slots.get("filters") or {}).get("fam1"))

        m = tr.get("match")
        self.assertIsNotNone(m)
        params, missing = build_sql_params_with_missing(m, "FAM1 DRAM 순생산 알려줘")
        self.assertEqual([], missing)
        self.assertEqual("DRAM", params.get("fam1_1"))

    def test_common_plan_shape_for_core_queries(self):
        cases = [
            ("2월 vh 판매 알려줘", "total", "", ["VH"], {}),
            ("올해 dram 순생산 알려줘", "total", "", [], {"fam1": ["DRAM"]}),
            ("2월 vh, vl 순생산 비교해줘", "compare", "version", ["VH", "VL"], {}),
            ("2월 3월 4월 vh 트렌드 분석해줘", "trend", "", ["VH"], {}),
            ("vh 기준 fam1별 순생산 보여줘", "grouped", "fam1", ["VH"], {}),
        ]
        for question, analysis_type, group_by, versions, filters in cases:
            tr = self._analyze(question)
            planner = tr.get("planner_plan") or {}
            self.assertEqual(analysis_type, planner.get("analysis_type"), msg=question)
            self.assertEqual(group_by, planner.get("group_by") or "", msg=question)
            self.assertEqual(versions, planner.get("versions") or [], msg=question)
            for k, vals in filters.items():
                self.assertEqual(vals, (planner.get("filters") or {}).get(k), msg=question)

    def test_noisy_expression_normalization_cases(self):
        cases = [
            ("vh판매추이", "trend", ["VH"], {}, ""),
            ("2월vh판매몇개야", "total", ["VH"], {}, ""),
            ("vh vl 비교", "compare", ["VH", "VL"], {}, "version"),
            ("vh,vl 비교해줘", "compare", ["VH", "VL"], {}, "version"),
            ("vh/vl 비교", "compare", ["VH", "VL"], {}, "version"),
            ("올해 dram 순생산", "total", [], {"fam1": ["DRAM"]}, ""),
            ("fam1별 vh 생산 보여줘", "grouped", ["VH"], {}, "fam1"),
            ("2월 3월 4월 vh 판매 트렌드", "trend", ["VH"], {}, ""),
            ("vh sales trend", "trend", ["VH"], {}, ""),
            ("버전별 판매 비교", "grouped", [], {}, "version"),
        ]
        for question, analysis_type, versions, filters, group_by in cases:
            tr = self._analyze(question)
            planner = tr.get("planner_plan") or {}
            self.assertEqual(analysis_type, planner.get("analysis_type"), msg=question)
            self.assertEqual(versions, planner.get("versions") or [], msg=question)
            self.assertEqual(group_by, planner.get("group_by") or "", msg=question)
            for k, vals in filters.items():
                self.assertEqual(vals, (planner.get("filters") or {}).get(k), msg=question)

    def test_normalized_question_keeps_noisy_inputs_stable(self):
        tr = self._analyze("2월vh판매몇개야")
        self.assertIn("2 월 vh 판매", tr.get("normalized_question") or "")
        tr2 = self._analyze("vh/vl 비교")
        self.assertIn("비교", tr2.get("normalized_question") or "")

    def test_dimension_value_filter_app_total(self):
        tr = self._analyze("APP MOBILE 판매 알려줘")
        self.assertEqual("sales_total", tr.get("final_intent"))
        slots = tr.get("final_slots") or {}
        self.assertEqual("sales", slots.get("metric"))
        self.assertEqual(["MOBILE"], (slots.get("filters") or {}).get("app"))

        m = tr.get("match")
        self.assertIsNotNone(m)
        params, missing = build_sql_params_with_missing(m, "APP MOBILE 판매 알려줘")
        self.assertEqual([], missing)
        self.assertEqual("MOBILE", params.get("app_1"))

    def test_direct_dimension_value_is_not_treated_as_version(self):
        tr = self._analyze("dram 올해 판매 트렌드 분석해줘")
        self.assertEqual("metric_trend_by_period", tr.get("final_intent"))
        slots = tr.get("final_slots") or {}
        self.assertEqual(["DRAM"], (slots.get("filters") or {}).get("fam1"))
        self.assertEqual([], slots.get("versions") or [])

    def test_dimension_filter_compare_versions(self):
        tr = self._analyze("FAM1 DRAM VH와 VL 순생산 비교 분석해줘")
        self.assertEqual("metric_compare_versions", tr.get("final_intent"))
        slots = tr.get("final_slots") or {}
        self.assertEqual(["DRAM"], (slots.get("filters") or {}).get("fam1"))
        self.assertEqual(["VH", "VL"], slots.get("versions"))

        m = tr.get("match")
        self.assertIsNotNone(m)
        params, missing = build_sql_params_with_missing(m, "FAM1 DRAM VH와 VL 순생산 비교 분석해줘")
        self.assertEqual([], missing)
        self.assertEqual("DRAM", params.get("fam1_1"))
        self.assertEqual("VH", params.get("v1"))
        self.assertEqual("VL", params.get("v2"))

    def test_dimension_filter_trend(self):
        tr = self._analyze("FAM1 DRAM 2월부터 5월까지 판매 트렌드 알려줘")
        self.assertEqual("metric_trend_by_period", tr.get("final_intent"))
        slots = tr.get("final_slots") or {}
        self.assertEqual(["DRAM"], (slots.get("filters") or {}).get("fam1"))
        self.assertEqual(["202602", "202603", "202604", "202605"], slots.get("periods"))

        m = tr.get("match")
        self.assertIsNotNone(m)
        params, missing = build_sql_params_with_missing(m, "FAM1 DRAM 2월부터 5월까지 판매 트렌드 알려줘")
        self.assertEqual([], missing)
        self.assertEqual("DRAM", params.get("fam1_1"))
        self.assertEqual("202602", params.get("p1"))
        self.assertEqual("202605", params.get("p4"))


if __name__ == "__main__":
    unittest.main()
