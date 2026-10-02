# Hệ Thống Phân Tích Hành Vi Mua Sắm & Gợi Ý Sản Phẩm Mua Kèm
### (Market Basket Analysis & Real-time Product Recommendation System)

---

## 1. Giới thiệu Đề tài
Dự án giải quyết bài toán thực tế trong lĩnh vực Thương mại điện tử và Bán lẻ hiện đại:
* **Mục tiêu kỹ thuật:** Xây dựng pipeline xử lý dữ liệu lớn kết hợp cơ sở dữ liệu **NoSQL MongoDB** và tính toán phân tán **Apache Spark (PySpark)**.
* **Mục tiêu nghiệp vụ:** Phân tích hành vi mua sắm từ hơn **124.000 giỏ hàng** (hơn 1.38 triệu giao dịch sản phẩm từ bộ dữ liệu Instacart), ứng dụng thuật toán **FP-Growth (Frequent Pattern Growth)** để khai phá các quy luật mua kèm (**Association Rules**) và xây dựng ứng dụng gợi ý bán chéo (**Cross-selling**) với độ trễ truy vấn thời gian thực dưới 5ms.

---

## 2. Kiến trúc Hệ thống & Luồng Dữ liệu

```
[Raw Instacart Data] 
         │
         ▼ (1. ETL & Chuẩn hóa Document Model)
[(MongoDB) instacart_bigdata]
 ├── products              (49.688 sản phẩm)
 └── orders                (124.364 giỏ hàng - Document array)
         │
         ▼ (2. Spark Native Partitioning)
[Apache Spark 3.5 Engine]
         │
         ▼ (3. Thuật toán FP-Growth phân tán)
[Frequent Itemsets & Association Rules]
         │ (Lọc Lift > 1.2)
         ▼ 
[(MongoDB) recommendation_rules] (302 luật + Index antecedent)
         │
         ▼ (4. Truy vấn thời gian thực < 5ms)
[Streamlit Web App]
 ├── Phân hệ 1: Gợi ý sản phẩm mua kèm (Live Recommendation: antecedent ⊆ cart_items)
 ├── Phân hệ 2: Dashboard phân tích hành vi mua sắm (Top Products, Peak Hours, Heatmap)
 └── Phân hệ 3: Khám phá luật kết hợp (Association Rules Explorer)
```

---

## 3. Cấu trúc Thư mục Dự án

```text
bigData/
├── app/
│   ├── app.py                      # Ứng dụng Web Demo Streamlit
│   └── data/                       # Dữ liệu xuất bản quyền tự động cho Cloud
│       ├── recommendation_rules.json
│       └── dashboard_cache.json
├── notebooks/
│   └── Train_FP_Growth_Colab.ipynb # Notebook huấn luyện trên Google Colab
├── scripts/
│   ├── import_to_mongodb.py        # Script nạp dữ liệu Instacart vào MongoDB
│   ├── train_fpgrowth_pyspark.py   # Script chạy Spark FP-Growth cục bộ
│   └── export_for_cloud.py         # Script xuất dữ liệu phục vụ deploy Cloud
├── Docs/                           # Báo cáo và tài liệu đồ án
├── requirements.txt                # Thư viện cho Web Deployment
├── run_app.bat                     # Tiện ích 1-click khởi chạy trên Windows
├── .gitignore                      # Bảo vệ không upload file CSV nặng lên GitHub
└── README.md
```

---

## 4. Kết quả Thực nghiệm

* **Dữ liệu phân tích:** 124.364 đơn hàng ($\ge 2$ mặt hàng/giỏ).
* **Tập phổ biến (Frequent Itemsets):** 914 tập mặt hàng được phát hiện.
* **Luật kết hợp (Association Rules):** Khai phá thành công **302 luật mua kèm mạnh** ($\text{Lift} > 1.2$).
* **Top các quy luật có độ nâng (Lift) cao nhất:**
  1. `Orange Bell Pepper` $\Rightarrow$ `Yellow Bell Pepper` (**Lift: 22.66x**, Confidence: 21.2%)
  2. `Lime Sparkling Water` $\Rightarrow$ `Sparkling Water Grapefruit` (**Lift: 9.58x**, Confidence: 25.8%)
  3. `Green Bell Pepper` $\Rightarrow$ `Red Peppers` (**Lift: 8.21x**, Confidence: 19.2%)
  4. `Organic Ginger Root` $\Rightarrow$ `Organic Garlic` (**Lift: 6.64x**, Confidence: 22.2%)
  5. `Bunched Cilantro` $\Rightarrow$ `Limes` (**Lift: 6.01x**, Confidence: 29.1%)

---

## 5. Hướng dẫn Cài đặt & Khởi chạy

### 5.1. Khởi chạy ứng dụng Web Demo (Cục bộ trên máy tính)
```powershell
# Cách 1: Nhấp đúp chuột vào file run_app.bat

# Cách 2: Chạy lệnh bằng terminal
.\.venv\Scripts\streamlit.exe run app/app.py
```
Truy cập giao diện tại: `http://localhost:8501`

### 5.2. Huấn luyện lại mô hình Big Data
```powershell
# Nạp dữ liệu vào MongoDB
.\.venv\Scripts\python.exe scripts/import_to_mongodb.py

# Chạy pipeline PySpark FP-Growth
.\.venv\Scripts\python.exe scripts/train_fpgrowth_pyspark.py
```

---

## 6. Hướng dẫn Deploy lên Streamlit Community Cloud (Miễn phí)

1. Đẩy mã nguồn lên **GitHub Repository** của bạn.
2. Truy cập [share.streamlit.io](https://share.streamlit.io/) và đăng nhập bằng GitHub.
3. Bấm **New app**:
   * **Repository:** Chọn repository vừa tạo.
   * **Branch:** `main`
   * **Main file path:** `app/app.py`
4. Bấm **Deploy!** Ứng dụng sẽ hoạt động trực tuyến 24/7 với đường link công khai để nộp bài hoặc demo.
