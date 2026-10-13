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
        """Kiểm tra tính hợp lệ của tập luật kết hợp phân hệ 1"""
        self.assertTrue(os.path.exists(self.rules_sample), "Tệp recommendation_rules.json không tồn tại!")
        with open(self.rules_sample, "r", encoding="utf-8") as f:
            rules = json.load(f)
        self.assertGreater(len(rules), 50, "Số lượng luật phân hệ 1 phải > 50!")
        for r in rules:
            self.assertTrue(len(r["antecedent"]) > 0, "Antecedent không được rỗng!")
            self.assertTrue(len(r["consequent"]) > 0, "Consequent không được rỗng!")
            self.assertGreater(r["lift"], 1.2, "Mọi luật khai phá phải có Lift > 1.2!")
            self.assertGreater(r["confidence"], 0.0)
            self.assertLessEqual(r["confidence"], 1.0)

    def test_prior_rules_integrity(self):
        """Kiểm tra tính hợp lệ của tập luật kết hợp phân hệ 2 (Big Data 3.2M)"""
        self.assertTrue(os.path.exists(self.rules_prior), "Tệp recommendation_rules_prior.json không tồn tại!")
        with open(self.rules_prior, "r", encoding="utf-8") as f:
            rules = json.load(f)
        self.assertGreater(len(rules), 50, "Số lượng luật phân hệ 2 phải > 50!")
        for r in rules:
            self.assertTrue(len(r["antecedent"]) > 0)
            self.assertTrue(len(r["consequent"]) > 0)
            self.assertGreater(r["lift"], 1.2)
            self.assertGreater(r["confidence"], 0.0)
            self.assertLessEqual(r["confidence"], 1.0)

if __name__ == "__main__":
    unittest.main()
