import os
import json
import unittest

class TestDataQuality(unittest.TestCase):
    def setUp(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.data_dir = os.path.join(self.base_dir, "app", "data")
        self.dq_file = os.path.join(self.data_dir, "data_quality_reports.json")
        self.runs_file = os.path.join(self.data_dir, "pipeline_runs.json")

    def test_data_quality_report_exists_and_valid(self):
        """Kiểm tra báo cáo chất lượng dữ liệu tồn tại và có đủ thông tin"""
        self.assertTrue(os.path.exists(self.dq_file), "Tệp data_quality_reports.json không tồn tại!")
        with open(self.dq_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertIn("tables", data)
        self.assertIn("cleaning_stages", data)
        self.assertGreater(len(data["tables"]), 0)

    def test_pipeline_runs_metadata_exists(self):
        """Kiểm tra nhật ký thực thi Spark Pipeline có ghi nhận đầy đủ các giai đoạn"""
        self.assertTrue(os.path.exists(self.runs_file), "Tệp pipeline_runs.json không tồn tại!")
        with open(self.runs_file, "r", encoding="utf-8") as f:
            runs = json.load(f)
        self.assertGreaterEqual(len(runs), 2, "Cần có ít nhất 2 lượt chạy ghi nhận cho các pipeline!")
        for r in runs:
            self.assertIn("run_id", r)
            self.assertIn("stages", r)
            self.assertGreaterEqual(len(r["stages"]), 5, "Mỗi pipeline phải có ít nhất 5 stage xử lý!")

    def test_distinct_items_per_basket(self):
        """Bảo đảm sản phẩm trong mỗi giỏ hàng là duy nhất (không trùng lặp)"""
        with open(self.dq_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        sample_records = data.get("sample_records", [])
        self.assertGreater(len(sample_records), 0, "Phải có mẫu giỏ hàng sau chuẩn hóa!")
        for s in sample_records:
            items = s.get("cleaned_items", [])
            self.assertEqual(len(items), len(set(items)), f"Giỏ hàng order_id={s.get('order_id')} có sản phẩm bị trùng lặp!")
            self.assertGreaterEqual(len(items), 2, f"Giỏ hàng order_id={s.get('order_id')} phải có ít nhất 2 sản phẩm!")

    def test_nulls_and_referential_integrity(self):
        """Kiểm tra tính toàn vẹn khóa ngoại, trùng lặp và giá trị null"""
        with open(self.dq_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        tables = data.get("tables", [])
        for t in tables:
            self.assertEqual(t.get("duplicates"), 0, f"Bảng {t['table_name']} có bản ghi trùng lặp khóa chính!")
            self.assertTrue(t.get("foreign_key_valid", True), f"Bảng {t['table_name']} vi phạm khóa ngoại!")
            if t["table_name"] != "orders.csv":
                self.assertEqual(t.get("null_count"), 0, f"Bảng {t['table_name']} không được có giá trị null!")

    def test_pruning_distinction_and_reconciliation(self):
        """Phân biệt loại dữ liệu lỗi vs lọc sản phẩm ít xuất hiện hoặc giỏ dưới 2 món"""
        with open(self.dq_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        kpis = data.get("kpis", {})
        valid_baskets = kpis.get("valid_baskets", 0)
        excluded_baskets = kpis.get("excluded_baskets", 0)
        total_orders = kpis.get("total_prior_orders", 0)
        self.assertEqual(valid_baskets + excluded_baskets, total_orders)
        
        breakdown = data.get("excluded_orders_breakdown", [])
        self.assertEqual(len(breakdown), 3)
        self.assertEqual(sum(g["count"] for g in breakdown), excluded_baskets)

if __name__ == "__main__":
    unittest.main()
