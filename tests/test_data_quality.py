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
        self.assertGreaterEqual(len(runs), 2, "Cần có ít nhất 2 lượt chạy ghi nhận cho 2 phân hệ!")
        for r in runs:
            self.assertIn("run_id", r)
            self.assertIn("stages", r)
            self.assertGreaterEqual(len(r["stages"]), 5, "Mỗi pipeline phải có ít nhất 5 stage xử lý!")

if __name__ == "__main__":
    unittest.main()
