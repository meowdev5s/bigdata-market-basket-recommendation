# HỆ THỐNG PHÂN TÍCH HÀNH VI MUA SẮM VÀ GỢI Ý SẢN PHẨM MUA KÈM
### (Market Basket Analysis & Real-time Product Recommendation System)
**Học phần:** Các công nghệ dữ liệu lớn (Big Data) | **Nhóm:** 11  

---

## 1. Giới thiệu Đề tài
Dự án xây dựng chuỗi giá trị xử lý dữ liệu lớn toàn diện theo chuẩn đề cương:
* **Mục tiêu kỹ thuật:** Kết hợp cơ sở dữ liệu **NoSQL MongoDB** (Storage & Serving Layer) và động cơ tính toán phân tán **Apache Spark MLlib** (Compute & ETL Layer).
* **Mục tiêu nghiệp vụ:** Phân tích hành vi mua sắm từ hơn **3.2 triệu đơn hàng** (hơn **32.4 triệu dòng giao dịch** từ bộ dữ liệu Instacart), ứng dụng thuật toán **FP-Growth (Frequent Pattern Growth)** để khai phá các quy luật mua kèm (**Association Rules**) và xây dựng ứng dụng gợi ý bán chéo (**Cross-selling**) với độ trễ phản hồi thời gian thực dưới 5ms.

---

## 2. Kiến trúc Hệ thống & Luồng Dữ liệu 2 Tầng (Lambda-lite Architecture)

```text
[ Dữ Liệu Gốc Instacart CSV (32.4M Dòng / 3.2M Đơn) ]
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│           TẦNG XỬ LÝ LÔ (APACHE SPARK BATCH LAYER)           │
│  1. Đọc song song (Parallel CSV Read)                        │
│  2. Làm sạch tính toàn vẹn (Inner Join Referential Check)   │
│  3. Khử trùng lặp giỏ hàng (collect_set deduplication)       │
│  4. Lọc bỏ giỏ hàng rác (size >= 2 items)                    │
│  5. Khử nhiễu thưa thớt (Apriori candidate pruning)          │
│  6. Khai phá luật kết hợp phân tán (MLlib FP-Growth)         │
└──────────────────────────────────────────────────────────────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
┌───────────────────────────┐ ┌────────────────────────────────┐
│ TẦNG PHỤC VỤ (MONGODB)    │ │ TẦNG TRỰC QUAN (STREAMLIT APP) │
│ - products (49.688 docs)  │ │ - 1. Tổng quan hành vi mua sắm │
│ - orders (124k & 200k)    │ │ - 2. Khám phá & chất lượng     │
│ - recommendation_rules    │ │ - 3. Spark ETL & Giám sát      │
│   (Index: antecedent,lift)│ │ - 4. Khai phá luật kết hợp     │
│ - dashboard_cache         │ │ - 5. Gợi ý mua kèm real-time   │
│ - data_quality_reports    │ │ - 6. Thực nghiệm & Đánh giá    │
└───────────────────────────┘ └────────────────────────────────┘
```

---

## 3. Cấu trúc Thư mục Dự án

```text
bigData/
├── app/
│   ├── app.py                         # Ứng dụng Web Streamlit 6 phân hệ
│   └── data/                          # Artifacts & Cache dữ liệu kiểm chứng
│       ├── recommendation_rules.json       # Tập luật Phân hệ 1 (124k đơn)
│       ├── recommendation_rules_prior.json # Tập luật Phân hệ 2 (3.2M đơn)
│       ├── dashboard_cache.json            # Thống kê Phân hệ 1
│       ├── dashboard_cache_prior.json      # Thống kê Phân hệ 2
│       ├── data_quality_reports.json       # Báo cáo chất lượng dữ liệu Before/After
│       ├── pipeline_runs.json              # Nhật ký thực thi các Stage của Spark
│       ├── experiment_results.json         # Ma trận thử nghiệm tham số FP-Growth
│       ├── evaluation_results.json         # Đánh giá Offline vs Baseline Top Popular
│       └── mongodb_benchmark.json          # Benchmark chỉ mục B-Tree (IXSCAN vs COLLSCAN)
├── notebooks/
│   └── Train_FP_Growth_Colab.ipynb    # Sổ tay huấn luyện trên Google Colab
├── scripts/
│   ├── import_to_mongodb.py           # Nạp dữ liệu vào MongoDB
│   ├── train_fpgrowth_pyspark.py      # Pipeline Spark cho Phân hệ 1 (Tập mẫu)
│   ├── train_fpgrowth_prior_3m.py     # Pipeline Spark cho Phân hệ 2 (Big Data 3.2M)
│   ├── generate_metrics_and_evaluation.py # Sinh artifacts kiểm định và benchmark
│   └── monitor_spark_ui.py            # Tiện ích theo dõi Spark UI (cổng 4040)
├── tests/
│   ├── test_data_quality.py           # Kiểm thử tự động tính toàn vẹn dữ liệu
│   ├── test_rules.py                  # Kiểm thử các ràng buộc toán học của tập luật
│   └── test_recommendations.py        # Kiểm thử thuật toán gợi ý và cơ chế Fallback
├── Docs/                              # Tài liệu đồ án, đề cương và kịch bản thuyết trình
│   ├── KeHoachHoanThienBigData.docx   # Kế hoạch hoàn thiện chuẩn
│   └── KichBanThuyetTrinhDemo.md      # Kịch bản demo và câu hỏi phản biện
├── .env.example                       # File mẫu biến môi trường cấu hình
├── requirements.txt                   # Danh mục thư viện Python
├── run_app.bat                        # Khởi chạy 1-click ứng dụng Web
└── README.md
```

---

## 4. Hướng dẫn Cài đặt & Khởi chạy

### 4.1. Cài đặt môi trường
```powershell
# Tạo môi trường ảo Python
python -m venv .venv
.\.venv\Scripts\activate

# Cài đặt các thư viện cần thiết
pip install -r requirements.txt
```

### 4.2. Khởi chạy Ứng dụng Web Streamlit
```powershell
# Cách 1: Nhấp đúp chuột vào file run_app.bat

# Cách 2: Chạy lệnh bằng terminal
.\.venv\Scripts\streamlit.exe run app/app.py
```
Truy cập giao diện tại: `http://localhost:8501`

### 4.3. Chạy toàn bộ Unit Tests kiểm thử
```powershell
.\.venv\Scripts\python.exe -m unittest discover tests
```

---

## 5. Bảng Nghiệm Thu Theo Mục 10 (Checklist & Evidence)

| STT | Tiêu chí nghiệm thu theo Mục 10 | Trạng thái | Minh chứng cụ thể trong dự án |
| :---: | :--- | :---: | :--- |
| **1** | Sơ đồ kiến trúc, luồng dữ liệu & mô hình MongoDB | **ĐẠT** | Mục 2 trong README; 6 Collections có Index trong MongoDB. |
| **2** | Dữ liệu gốc hợp lệ, mô tả các cột và cách tải | **ĐẠT** | Bảng Data Catalog 6 tệp trong Phân hệ 2 (`data_quality_reports.json`). |
| **3** | Bảng chất lượng dữ liệu trước/sau ETL & mã Spark | **ĐẠT** | Phân hệ 2; loại bỏ 6.845 đơn rác (tập Train) và 1.14M đơn rác (tập Prior). |
| **4** | Nhật ký stage, thời gian chạy Spark & Spark UI | **ĐẠT** | Phân hệ 3; 8 Stages ghi nhận trong `pipeline_runs.json`; `xem_spark_ui.bat`. |
| **5** | Frequent itemsets, association rules & giải thích | **ĐẠT** | Phân hệ 4 & 5; giải thích công thức Support, Confidence, Lift. |
| **6** | Ít nhất 3 cấu hình tham số & bảng so sánh | **ĐẠT** | Phân hệ 6 (Tab 1); 5 cấu hình thử nghiệm `EXP-01` đến `EXP-05`. |
| **7** | Gợi ý mua kèm có giải thích & fallback minh bạch | **ĐẠT** | Phân hệ 5; giải thích luật kích hoạt + fallback Top Popular có dán nhãn. |
| **8** | Đánh giá offline so với baseline & nhận xét | **ĐẠT** | Phân hệ 6 (Tab 2); Precision@K, Recall@K, HitRate@K, Coverage@K vs Baseline. |
| **9** | Dashboard tương tác 6 phân hệ, số liệu thật | **ĐẠT** | Ứng dụng Streamlit `app/app.py` với 6 phân hệ có câu hỏi dẫn dắt. |
| **10**| Benchmark truy vấn & hướng dẫn chạy lại | **ĐẠT** | Phân hệ 6 (Tab 3); `explain("executionStats")` IXSCAN vs COLLSCAN. |
| **11**| Test cơ bản, requirements, README, .env.example | **ĐẠT** | Thư mục `tests/` (7 tests PASS), `requirements.txt`, `.env.example`. |
| **12**| Kịch bản demo và câu hỏi phản biện Hội đồng | **ĐẠT** | Tệp [`Docs/KichBanThuyetTrinhDemo.md`](file:///e:/SEM7/bigData/Docs/KichBanThuyetTrinhDemo.md). |
