import os
import json
import unittest

class TestAssociationRules(unittest.TestCase):
    def setUp(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.data_dir = os.path.join(self.base_dir, "app", "data")
        self.rules_sample = os.path.join(self.data_dir, "recommendation_rules.json")
        self.rules_prior = os.path.join(self.data_dir, "recommendation_rules_prior.json")

    def test_sample_rules_integrity(self):
        """Kiểm tra tính hợp lệ của tập luật kết hợp chính (Big Data 3.2M)"""
        self.assertTrue(os.path.exists(self.rules_sample), "Tệp recommendation_rules.json không tồn tại!")
        with open(self.rules_sample, "r", encoding="utf-8") as f:
            rules = json.load(f)
        self.assertGreater(len(rules), 50, "Số lượng luật khai phá phải > 50!")
        for r in rules:
            self.assertTrue(len(r["antecedent"]) > 0, "Antecedent không được rỗng!")
            self.assertTrue(len(r["consequent"]) > 0, "Consequent không được rỗng!")
            self.assertGreater(r["lift"], 1.2, "Mọi luật khai phá phải có Lift > 1.2!")
            self.assertGreater(r["confidence"], 0.0)
            self.assertLessEqual(r["confidence"], 1.0)

    def test_prior_rules_integrity(self):
        """Kiểm tra tính hợp lệ của tập luật kết hợp đồng bộ (Big Data 3.2M)"""
        self.assertTrue(os.path.exists(self.rules_prior), "Tệp recommendation_rules_prior.json không tồn tại!")
        with open(self.rules_prior, "r", encoding="utf-8") as f:
            rules = json.load(f)
        self.assertGreater(len(rules), 50, "Số lượng luật Big Data phải > 50!")
        for r in rules:
            self.assertTrue(len(r["antecedent"]) > 0)
            self.assertTrue(len(r["consequent"]) > 0)
            self.assertGreater(r["lift"], 1.2)
            self.assertGreater(r["confidence"], 0.0)
            self.assertLessEqual(r["confidence"], 1.0)

    def test_frequent_itemsets_integrity(self):
        """Kiểm tra sự tồn tại và tính hợp lệ của tập mục phổ biến Frequent Itemsets và phân biệt với luật kết hợp"""
        itemsets_file = os.path.join(self.data_dir, "frequent_itemsets.json")
        self.assertTrue(os.path.exists(itemsets_file), "Tệp frequent_itemsets.json không tồn tại!")
        with open(itemsets_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertIn("itemsets_1", data)
        self.assertIn("itemsets_multi", data)
        all_it = data["itemsets_1"] + data["itemsets_multi"]
        self.assertGreater(len(all_it), 20)
        
        for it in all_it:
            self.assertIn("items", it)
            self.assertIn("k", it)
            self.assertIn("frequency", it)
            self.assertIn("support", it)
            self.assertEqual(it["k"], len(it["items"]), "Kích thước k phải bằng số lượng phần tử trong items!")
            self.assertEqual(len(it["items"]), len(set(it["items"])), "Sản phẩm trong tập phổ biến không được trùng lặp!")
            self.assertGreater(it["frequency"], 0)
            self.assertGreater(it["support"], 0.0)
            # Tập phổ biến là tập vô hướng (không có antecedent/consequent)
            self.assertNotIn("antecedent", it)
            self.assertNotIn("consequent", it)
            self.assertNotIn("confidence", it)
            self.assertNotIn("lift", it)

    def test_data_quality_report_dynamic_integrity(self):
        """Kiểm tra báo cáo kiểm định chất lượng dữ liệu được sinh động với run_id, kpis và đối soát toán học"""
        dq_file = os.path.join(self.data_dir, "data_quality_reports.json")
        self.assertTrue(os.path.exists(dq_file), "Tệp data_quality_reports.json không tồn tại!")
        with open(dq_file, "r", encoding="utf-8") as f:
            dq = json.load(f)
        
        # Kiểm tra metadata lượt chạy và tham số
        self.assertIn("run_id", dq)
        self.assertIn("timestamp", dq)
        self.assertIn("parameters", dq)
        self.assertIn("kpis", dq)
        self.assertIn("tables", dq)
        self.assertEqual(len(dq["tables"]), 5, "Báo cáo phải kiểm định đủ 5 tệp CSV Instacart!")
        
        # Đối soát số lượng giỏ hợp lệ và số đơn bị loại
        kpis = dq["kpis"]
        valid_baskets = kpis["valid_baskets"]
        excluded_baskets = kpis["excluded_baskets"]
        total_orders = kpis["total_prior_orders"]
        self.assertEqual(valid_baskets + excluded_baskets, total_orders, "Tổng giỏ hợp lệ + đơn bị loại phải bằng tổng đơn Prior!")
        
        # Đối soát 3 nhóm phân rã đơn bị loại không chồng lặp
        breakdown = dq.get("excluded_orders_breakdown", [])
        self.assertEqual(len(breakdown), 3, "Phân rã đơn bị loại phải gồm đúng 3 nhóm!")
        sum_breakdown = sum(g["count"] for g in breakdown)
        self.assertEqual(sum_breakdown, excluded_baskets, "Tổng 3 nhóm phân rã phải khớp đúng số đơn bị loại!")

if __name__ == "__main__":
    unittest.main()
