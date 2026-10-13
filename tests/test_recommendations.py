import os
import json
import unittest

class TestRecommendationEngine(unittest.TestCase):
    def setUp(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.data_dir = os.path.join(self.base_dir, "app", "data")
        self.rules_prior = os.path.join(self.data_dir, "recommendation_rules_prior.json")
        with open(self.rules_prior, "r", encoding="utf-8") as f:
            self.rules = json.load(f)

    def recommend(self, cart_items):
        cart_set = set(cart_items)
        recs = {}
        for rule in self.rules:
            ant_set = set(rule["antecedent"])
            if ant_set.issubset(cart_set):
                for cons in rule["consequent"]:
                    if cons not in cart_set:
                        if cons not in recs or rule["lift"] > recs[cons]["lift"]:
                            recs[cons] = rule
        sorted_recs = sorted(recs.values(), key=lambda x: x["lift"], reverse=True)
        return [r["consequent"][0] if len(r["consequent"]) == 1 else ", ".join(r["consequent"]) for r in sorted_recs]

    def test_recommendation_trigger(self):
        """Kiểm tra khi giỏ có Limes thì phải gợi ý được Organic Cilantro"""
        cart = ["Limes"]
        recs = self.recommend(cart)
        self.assertIn("Organic Cilantro", recs, "Giỏ hàng có Limes phải gợi ý Organic Cilantro theo luật khai phá!")

    def test_recommendation_does_not_suggest_items_already_in_cart(self):
        """Kiểm tra hệ thống không bao giờ gợi ý lại món đã có trong giỏ hàng"""
        cart = ["Limes", "Organic Cilantro"]
        recs = self.recommend(cart)
        self.assertNotIn("Limes", recs)
        self.assertNotIn("Organic Cilantro", recs)

    def test_empty_cart_handling(self):
        """Kiểm tra khi giỏ hàng rỗng thì không sinh ra gợi ý giả"""
        cart = []
        recs = self.recommend(cart)
        self.assertEqual(len(recs), 0)

if __name__ == "__main__":
    unittest.main()
