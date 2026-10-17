# HỆ THỐNG PHÂN TÍCH HÀNH VI MUA SẮM VÀ GỢI Ý SẢN PHẨM MUA KÈM
### (Market Basket Analysis & Real-time Product Recommendation System)
**Học phần:** Các công nghệ dữ liệu lớn (Big Data) | **Nhóm:** 11  

---

## 1. Giới thiệu Đề tài
Dự án xây dựng chuỗi giá trị xử lý dữ liệu lớn toàn diện theo chuẩn đề cương:
* **Mục tiêu kỹ thuật:** Kết hợp cơ sở dữ liệu **NoSQL MongoDB** (Storage & Serving Layer) và động cơ tính toán phân tán **Apache Spark MLlib** (Compute & ETL Layer).
* **Mục tiêu nghiệp vụ:** Phân tích hành vi mua sắm từ hơn **3.2 triệu đơn hàng** (hơn **32.4 triệu dòng giao dịch** từ bộ dữ liệu Instacart), ứng dụng thuật toán **FP-Growth (Frequent Pattern Growth)** để khai phá tập mục phổ biến (**Frequent Itemsets**) và các quy luật mua kèm (**Association Rules**), từ đó xây dựng ứng dụng gợi ý bán chéo (**Cross-selling**) với độ trễ phản hồi thời gian thực dưới 5ms.

---

## 2. Kiến trúc Hệ thống & Luồng Dữ liệu Phân Tầng

```text
[ Dữ Liệu Gốc Instacart CSV (32.4M Dòng / 3.2M Đơn) ]
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│           TẦNG XỬ LÝ LÔ (APACHE SPARK BATCH LAYER)           │
│  1. Đọc song song phân tán (Parallel CSV Read)              │
│  2. Kiểm tra toàn vẹn khóa ngoại (Referential Join)          │
│  3. Gom giỏ hàng & khử trùng lặp (collect_list deduplication)│
│  4. Lọc bỏ giỏ hàng rác (size >= 2 items)                    │
│  5. Khử nhiễu thưa thớt (Apriori candidate pruning >= 15k)   │
│  6. Khai phá tập phổ biến & luật kết hợp (MLlib FP-Growth)   │
└──────────────────────────────────────────────────────────────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
┌───────────────────────────┐ ┌────────────────────────────────┐
│ TẦNG PHỤC VỤ (MONGODB)    │ │ TẦNG TRỰC QUAN (STREAMLIT APP) │
│ - products (49.688 docs)  │ │ - 1. Tổng quan hành vi mua sắm │
│ - orders_prior (2.06M docs)│ │ - 2. Khám phá & chất lượng     │
│ - recommendation_rules    │ │ - 3. Spark ETL & Giám sát      │
│   (Index: antecedent,lift)│ │ - 4. Khai phá luật kết hợp     │
│ - dashboard_cache         │ │ - 5. Gợi ý mua kèm real-time   │
│ - frequent_itemsets       │ │ - 6. Thực nghiệm & Đánh giá    │
└───────────────────────────┘ └────────────────────────────────┘
```

---

## 3. Cấu trúc Thư mục Dự án

```text
bigData/
├── app/
│   ├── app.py                         # Ứng dụng Web Streamlit 6 phân hệ
│   └── data/                          # Artifacts & Cache dữ liệu kiểm chứng (Big Data 3.2M)
│       ├── recommendation_rules.json       # Tập luật kết hợp đã khai phá (102 rules)
│       ├── frequent_itemsets.json          # Tập mục phổ biến 1-itemset, 2-itemset, 3-itemset
│       ├── dashboard_cache.json            # Thống kê tổng hợp toàn bộ 3.2M đơn hàng
│       ├── data_quality_reports.json       # Báo cáo chất lượng dữ liệu Before/After
│       ├── pipeline_runs.json              # Nhật ký thực thi các Stage của Spark
│       ├── experiment_results.json         # Ma trận thử nghiệm tham số FP-Growth
│       ├── evaluation_results.json         # Đánh giá Offline vs Baseline Top Popular
│       └── mongodb_benchmark.json          # Benchmark chỉ mục B-Tree (IXSCAN vs COLLSCAN)
├── notebooks/
│   └── Train_FP_Growth_Colab.ipynb    # Sổ tay huấn luyện trên Google Colab
├── scripts/
│   ├── import_to_mongodb.py           # Nạp danh mục sản phẩm và đơn hàng vào MongoDB
│   ├── train_fpgrowth_prior_3m.py     # Pipeline Spark chính thức trên 3.2M đơn hàng
│   ├── train_fpgrowth_pyspark.py      # Script chuyển tiếp gọi pipeline Spark chính thức
│   ├── generate_metrics_and_evaluation.py # Sinh artifacts kiểm định và benchmark
│   └── monitor_spark_ui.py            # Tiện ích theo dõi Spark UI (cổng 4040)
├── tests/
│   ├── test_data_quality.py           # Kiểm thử tự động tính toàn vẹn dữ liệu
│   ├── test_rules.py                  # Kiểm thử các ràng buộc toán học của tập luật & tập phổ biến
│   └── test_recommendations.py        # Kiểm thử thuật toán gợi ý và cơ chế Fallback
├── Docs/                              # Tài liệu hướng dẫn sử dụng và kịch bản demo
│   └── HuongDanSuDungVaDemo.md        # Hướng dẫn thao tác chi tiết 6 phân hệ & phản biện
├── .env.example                       # File mẫu biến môi trường cấu hình
├── requirements.txt                   # Danh mục thư viện Python
├── run_app.bat                        # Khởi chạy 1-click ứng dụng Web
└── README.md
```

---

## 4. Nguồn Dữ Liệu & Từ Điển Dữ Liệu (Data Catalog & Schema)

### 4.1. Nguồn Tải & Điều Kiện Sử Dụng (Dataset Source & License)
* **Nguồn chính xác:** Bộ dữ liệu chính thức trên Kaggle: [Instacart Market Basket Analysis (psparks)](https://www.kaggle.com/datasets/psparks/instacart-market-basket-analysis).
* **Lệnh tải qua Kaggle CLI:**
  ```bash
  kaggle datasets download -d psparks/instacart-market-basket-analysis
  ```
* **Điều kiện sử dụng (Terms of Use / License):** Bộ dữ liệu được công ty Instacart phát hành công khai phục vụ mục đích nghiên cứu học thuật, giáo dục phi thương mại (**Non-Commercial / Academic Research Use Only**). Toàn bộ thông tin cá nhân và định danh khách hàng đã được ẩn danh hóa (**Anonymized Customer IDs**).

---

### 4.2. Hướng Dẫn Đặt Tệp CSV Vào Dự Án
1. Tạo thư mục dữ liệu tại thư mục gốc:
   ```powershell
   mkdir -p data/InstacartMarketBasketAnalysis
   ```
2. Giải nén toàn bộ các tệp `.csv` vào đúng đường dẫn sau:
   ```text
   bigData/
   └── data/
       └── InstacartMarketBasketAnalysis/
           ├── aisles.csv                  # 134 quầy hàng (~2.6 KB)
           ├── departments.csv             # 21 ngành hàng (~270 B)
           ├── products.csv                # 49.688 sản phẩm (~2.1 MB)
           ├── orders.csv                  # 3.421.083 đơn hàng (~108 MB)
           └── order_products__prior.csv   # 32.434.489 dòng giao dịch (~577 MB)
   ```

*(Lưu ý: Dự án đã tích hợp sẵn toàn bộ Artifacts/Cache JSON trong thư mục `app/data/`, do đó ứng dụng Web Streamlit có thể khởi chạy và trình diễn đầy đủ ngay lập tức mà không bắt buộc phải tải lại tệp CSV 3.5GB).*

---

### 4.3. Bảng Ý Nghĩa Các Trường & Đặc Tả Lược Đồ (Data Dictionary)

| Tên tệp CSV | Thực thể | Số dòng | Số cột | Khóa chính | Ràng buộc / Ý nghĩa các trường dữ liệu |
| :--- | :--- | :---: | :---: | :--- | :--- |
| **`orders.csv`** | Metadata đơn hàng | **3.421.083** | 7 | `order_id` | - `order_id` (int): Mã định danh duy nhất của đơn hàng.<br>- `user_id` (int): Mã khách hàng ẩn danh.<br>- `eval_set` (str): Tập dữ liệu (`prior`: 3.214.874 đơn, `train`: 131.209 đơn, `test`: 75.000 đơn).<br>- `order_number` (int): Thứ tự đơn hàng của khách hàng (1, 2, 3...).<br>- `order_dow` (int): Ngày trong tuần (0 = Chủ Nhật, 1 = Thứ Hai, ..., 6 = Thứ Bảy).<br>- `order_hour_of_day` (int): Khung giờ đặt hàng trong ngày (0 đến 23).<br>- `days_since_prior_order` (float): Số ngày kể từ đơn đặt gần nhất (null ở đơn đầu tiên của user: 206.209 dòng, 6.03%). |
| **`order_products__prior.csv`** | Giao dịch Big Data | **32.434.489** | 4 | `(order_id, product_id)` | - `order_id` (int): Mã đơn hàng (Khóa ngoại tham chiếu `orders.csv`).<br>- `product_id` (int): Mã sản phẩm (Khóa ngoại tham chiếu `products.csv`).<br>- `add_to_cart_order` (int): Thứ tự sản phẩm được khách chọn vào giỏ.<br>- `reordered` (int): Trạng thái mua lại (1: Đã từng mua trước đây, 0: Mua lần đầu). |
| **`products.csv`** | Danh mục sản phẩm | **49.688** | 4 | `product_id` | - `product_id` (int): Mã sản phẩm chuẩn hóa.<br>- `product_name` (str): Tên hiển thị đầy đủ của mặt hàng.<br>- `aisle_id` (int): Mã quầy hàng (FK tham chiếu `aisles.csv`).<br>- `department_id` (int): Mã ngành hàng (FK tham chiếu `departments.csv`). |
| **`aisles.csv`** | Quầy hàng chi tiết | **134** | 2 | `aisle_id` | - `aisle_id` (int): Mã định danh quầy hàng.<br>- `aisle` (str): Tên quầy hàng (fresh fruits, yogurt, packaged cheese...). |
| **`departments.csv`** | Ngành hàng lớn | **21** | 2 | `department_id` | - `department_id` (int): Mã định danh ngành hàng.<br>- `department` (str): Tên ngành hàng lớn (produce, dairy eggs, beverages, snacks...). |

---

## 5. Thiết Kế & Lược Đồ Cơ Sở Dữ Liệu NoSQL MongoDB

### 5.1. Vai Trò & Phạm Vi Lưu Trữ Của MongoDB
Trong kiến trúc xử lý dữ liệu lớn của đề tài:
* **Apache Spark** đảm nhiệm tầng tính toán phân tán (Compute Layer) trên toàn bộ **32.4 triệu dòng giao dịch** và **3.2 triệu đơn hàng gốc**.
* **MongoDB** đóng vai trò tầng lưu trữ và phục vụ (Storage & Serving Data Mart), lưu trữ danh mục sản phẩm chuẩn hóa, toàn bộ **2.066.543** giỏ hàng hợp lệ (100% giỏ hàng phân tích có $\ge 2$ món và chứa sản phẩm phổ biến) với cấu trúc Document chứa mảng danh sách sản phẩm `items`, tập luật kết hợp và tập mục phổ biến phục vụ gợi ý thời gian thực.

### 5.2. Đặc Tả Lược Đồ & Chỉ Mục Các Collection Trong MongoDB (`instacart_bigdata`)

| Tên Collection | Số lượng bản ghi | Cấu trúc lược đồ (Document Schema) | Các chỉ mục kích hoạt (B-Tree Indexes) |
| :--- | :---: | :--- | :--- |
| **`products`** | **49.688** docs | `{ _id: int, product_id: int, product_name: str, aisle: str, department: str }` | `_id_`, `product_name_1` |
| **`orders_prior`** | **2.066.543** docs | `{ _id: int, order_id: int, user_id: int, order_dow: int, order_hour_of_day: int, items_count: int, items: [str] }` | `_id_`, `order_id_1`, `order_dow_1_order_hour_of_day_1`, `items_1` |
| **`recommendation_rules_prior`** | **102** docs | `{ _id: ObjectId, antecedent: [str], consequent: [str], confidence: float, lift: float, support: float }` | `_id_`, `antecedent_1`, `lift_-1`, `confidence_-1` |
| **`frequent_itemsets`** | **350** docs | `{ _id: ObjectId, items: [str], k: int, frequency: int, support: float }` | `_id_`, `k_1`, `support_-1`, `items_1` |
| **`dashboard_cache_prior`** | **1** doc | `{ total_orders: 3214874, total_transactions: 32434489, top_products: [...], hourly_data: [...], heat_data: [...] }` | `_id_` |

---

## 6. Phương Pháp Luận FP-Growth: Chuỗi Khai Phá, Mẫu Số & Cảnh Báo Nhân Quả

### 6.1. Chuỗi Khai Phá Dữ Liệu Đầu - Cuối (End-to-End Pipeline)
$$\text{Giỏ hàng } T_k (\ge 2 \text{ món}) \xrightarrow{\text{Pha 1: Cây FP-Tree}} \text{Frequent Itemsets } (k \ge 1) \xrightarrow{\text{Pha 2: Logic Partition}} \text{Association Rules } (A \rightarrow B) \xrightarrow{\text{MongoDB Serving}} \text{Gợi ý Real-time } (< 2\text{ms})$$

### 6.2. Ví Dụ Minh Họa Bước-Theo-Bước Với Số Liệu Thực Tế
* **Bước 1 (Giỏ hàng mẫu):** Đơn hàng thực tế gồm các sản phẩm: `[Limes, Large Lemon, Organic Yellow Onion, Organic Garlic]`.
* **Bước 2 (Tập mục phổ biến):** Cây FP-Tree nén $N = 2.066.543$ giỏ hàng hợp lệ và đệ quy tìm ra tập 2-itemset:
  $$I = \{\text{Limes}, \text{Large Lemon}\} \quad (\text{Frequency} = 27.403 \text{ giỏ}, \ \text{Support} = 1.33\%)$$
* **Bước 3 (Sinh luật kết hợp):** Phân hoạch $I$ thành luật có hướng $\{\text{Limes}\} \rightarrow \{\text{Large Lemon}\}$:
  * Số giỏ chứa $\{\text{Limes}\}$: $136.407$ giỏ ($\text{Support} = 6.60\%$).
  * Số giỏ chứa $\{\text{Large Lemon}\}$: $147.434$ giỏ ($\text{Support} = 7.13\%$).
  * **Confidence:** $\frac{27.403}{136.407} = 20.09\%$ *(Trong 100 khách mua Limes, có hơn 20 khách mua thêm Large Lemon)*.
  * **Lift:** $\frac{20.09\%}{7.13\%} = 2.82$ *(Tần suất mua Large Lemon khi đã mua Limes cao gấp 2.82 lần so với mua độc lập ngẫu nhiên)*.
  * *(Tương tự, luật có Lift cao nhất trong tập luật là $\{\text{Limes}\} \rightarrow \{\text{Organic Cilantro}\}$ với $\text{Lift} = 3.9115$ và $\text{Confidence} = 12.88\%$)*.

### 6.3. Bảng Chuẩn Hóa Công Thức Toán Học & Mẫu Số Sử Dụng Cụ Thể

| Chỉ số đo lường | Công thức toán học | Mẫu số sử dụng cụ thể trong dự án | Ý nghĩa & Bản chất xác suất |
| :--- | :--- | :--- | :--- |
| **Support** $\{A, B\}$ | $\frac{\|\{T \in \mathcal{D} \mid A \cup B \subseteq T\}\|}{\|\mathcal{D}\|}$ | $\|\mathcal{D}\| = \mathbf{2.066.543}$ giỏ hàng hợp lệ (đã lọc tần suất $\ge 2$ món) | Xác suất biên $P(A \cap B)$: Tỷ lệ giỏ hàng ngẫu nhiên chứa cả $A$ và $B$. |
| **Confidence** $A \rightarrow B$ | $\frac{\text{Support}(A \cup B)}{\text{Support}(A)} = P(B \mid A)$ | $\text{Support}(A)$ hoặc Số đơn chứa tiền đề $A$ (Ví dụ: Limes có 136.407 giỏ) | Xác suất có điều kiện $P(B \mid A)$: Tỷ lệ khách mua $B$ khi đã chọn $A$. |
| **Lift** $A \rightarrow B$ | $\frac{\text{Confidence}(A \rightarrow B)}{\text{Support}(B)} = \frac{P(A \cap B)}{P(A) \cdot P(B)}$ | $\text{Support}(A) \times \text{Support}(B)$ (Tần suất kỳ vọng nếu $A, B$ độc lập) | Độ nâng tương quan: Đo lường mức độ phụ thuộc thống kê đồng thời so với ngẫu nhiên. |

### 6.4. Cảnh Báo Phương Pháp Luận: Tương Quan Không Phải Nhân Quả (Correlation $\neq$ Causation)
> [!WARNING]
> **Độ nâng Lift phản ánh sự phụ thuộc tương quan đồng thời (Co-occurrence correlation), tuyệt đối KHÔNG PHẢI quan hệ nhân quả (Causation):**
> * Việc $\text{Lift}(\text{Limes} \rightarrow \text{Large Lemon}) = 3.91$ **không đồng nghĩa** với việc *"Mua Limes là nguyên nhân khiến khách mua Large Lemon"*.
> * Cả hai sản phẩm cùng xuất hiện trong giỏ hàng có thể do **yếu tố gây nhiễu chung (Confounding Factor)**: chẳng hạn như nhu cầu làm tiệc nướng, pha nước cocktail hoặc nấu món hải sản.
> * Khai phá luật kết hợp là mô hình quan sát hành vi (Observational Mining). Để khẳng định quan hệ nhân quả có thể can thiệp kinh doanh, hệ thống bán lẻ bắt buộc phải thực hiện **Thử nghiệm A/B Testing ngẫu nhiên (Randomized Controlled Trials)**.

---

---

## 7. Hướng Dẫn Cài Đặt, Tái Lập Toàn Bộ Hệ Thống & Kiểm Chứng Môi Trường

### 7.1. Bảng Thông Số Môi Trường Đã Kiểm Chứng (Tested Environment Specifications)

| Thành phần hạ tầng | Phiên bản khuyến nghị | Phiên bản đã kiểm chứng thực tế | Ghi chú kỹ thuật |
| :--- | :--- | :--- | :--- |
| **Hệ điều hành** | Windows 10/11 x64, Ubuntu 20.04+, macOS | **Windows 11 x64** | Hỗ trợ đa nền tảng |
| **Python Runtime** | Python 3.10 đến 3.13 | **Python 3.13.5 x64** | Kiểm tra bằng: `python --version` |
| **Java Runtime (JRE/JDK)** | Java 8, 11 hoặc 17 | **Java 1.8.0_471 (build 25.471-b09 64-Bit)** | Bắt buộc cho Spark: `java -version` |
| **Apache Spark / MLlib** | Spark 3.5.x | **PySpark 3.5.9** (Spark MLlib 3.5.x) | Khai phá FP-Growth phân tán |
| **NoSQL Database** | MongoDB 6.0+ hoặc 7.0+ | **MongoDB Community Server 7.0+** | Cổng mặc định `localhost:27017` |
| **Web Dashboard** | Streamlit >= 1.30.0 | **Streamlit 1.64.0** | Giao diện tương tác 6 phân hệ |
| **Trực quan hóa** | Plotly >= 5.15.0, Pandas >= 2.0.0 | **Plotly 7.1.0, Pandas 3.0.6** | Biểu đồ động & xử lý dữ liệu |

---

### 7.2. Quy Trình Tái Lập 7 Bước Từ A Đến Z (End-to-End Reproducibility Guide)

#### Bước 1: Tải bộ dữ liệu gốc từ Kaggle & đặt vào dự án
Tải bộ dữ liệu [Instacart Market Basket Analysis (psparks)](https://www.kaggle.com/datasets/psparks/instacart-market-basket-analysis):
```powershell
kaggle datasets download -d psparks/instacart-market-basket-analysis
```
Giải nén 5 tệp CSV (`orders.csv`, `order_products__prior.csv`, `products.csv`, `aisles.csv`, `departments.csv`) vào thư mục:  
`data/InstacartMarketBasketAnalysis/`.

#### Bước 2: Thiết lập môi trường ảo Python & Cài đặt thư viện
```powershell
# Tạo môi trường ảo
python -m venv .venv

# Kích hoạt môi trường (Windows PowerShell)
.\.venv\Scripts\activate

# Cài đặt toàn bộ thư viện (bao gồm PySpark 3.5.x)
pip install -r requirements.txt
```

#### Bước 3: Khởi động dịch vụ NoSQL MongoDB
Đảm bảo MongoDB Server đang hoạt động và lắng nghe tại `mongodb://localhost:27017/`. (Có thể cấu hình chuỗi kết nối Cloud Atlas thông qua file `.env` hoặc biến `MONGO_URI`).

#### Bước 4: Nạp dữ liệu vào MongoDB (Data Ingestion)
```powershell
python scripts/import_to_mongodb.py
```
*Script sẽ nạp 49.688 sản phẩm vào collection `products` và các bộ dữ liệu giỏ hàng mẫu vào MongoDB.*

#### Bước 5: Chạy Spark ETL & Huấn luyện FP-Growth trên toàn bộ 3.2M đơn hàng
```powershell
python scripts/train_fpgrowth_prior_3m.py
```
*Pipeline Apache Spark sẽ thực thi 6 bước nghiệp vụ tuần tự: đọc 32.4 triệu dòng giao dịch, khử trùng lặp `collect_set`, lọc 2.066.543 giỏ hàng hợp lệ, nén cây FP-Tree, khai phá 350 frequent itemsets & 102 association rules, sinh `data_quality_reports.json` và đồng bộ vào MongoDB.*

#### Bước 6: Sinh báo cáo thực nghiệm, Offline Evaluation & Benchmark
```powershell
python scripts/generate_metrics_and_evaluation.py
```
*Tự động đo đạc và sinh 4 artifacts: `experiment_results.json` (5 cấu hình EXP-01 đến EXP-05), `evaluation_results.json` (Leave-k-out trên 1.000 giỏ test thực tế), `mongodb_benchmark.json` (IXSCAN vs COLLSCAN trên 2.06M bản ghi), và `pipeline_runs.json`.*

#### Bước 7: Khởi chạy Ứng dụng Web Dashboard Streamlit
```powershell
# Cách 1: Nhấp đúp chuột vào file run_app.bat

# Cách 2: Chạy lệnh terminal
.\.venv\Scripts\streamlit.exe run app/app.py
```
Truy cập giao diện tại: `http://localhost:8501`.

---

### 7.3. Chạy Toàn Bộ Unit Tests Kiểm Thử Tự Động
```powershell
.\.venv\Scripts\python.exe -m unittest discover tests
```
*Dự án tích hợp sẵn 12 unit tests kiểm thử tự động trên 3 tệp (`test_data_quality.py`, `test_rules.py`, `test_recommendations.py`), đảm bảo 100% tests PASS.*

---

### 7.4. Giám Sát Apache Spark Web UI & Nhật Ký Thực Thi
> [!NOTE]
> **Minh bạch môi trường thực thi:** Đề tài vận hành trên **Spark Local Mode (`local[*]`)** trên 01 máy trạm cục bộ, sử dụng toàn bộ nhân CPU logic và 4GB RAM Driver với cơ chế G1GC. Đề tài **không** giả lập cụm phân tán nhiều máy vật lý (Multi-node cluster).

```powershell
# Khởi chạy giám sát Spark UI
.\.venv\Scripts\python.exe scripts/monitor_spark_ui.py
# Hoặc nhấp đúp file: xem_spark_ui.bat
```
* Truy cập giao diện Spark Web UI tại: `http://localhost:4040` để giám sát biểu đồ DAG, Shuffle Read/Write và Task Timeline.
* Nhật ký thực thi chi tiết của Spark Engine được lưu trữ tại: [`logs/spark_execution_prod.log`](file:///e:/SEM7/bigData/logs/spark_execution_prod.log).

---

## 8. Bảng Nghiệm Thu Theo Mục 10 (Checklist & Evidence)

| STT | Tiêu chí nghiệm thu theo Mục 10 | Trạng thái | Minh chứng cụ thể trong dự án |
| :---: | :--- | :---: | :--- |
| **1** | Sơ đồ kiến trúc, luồng dữ liệu & mô hình MongoDB | **ĐẠT** | Mục 2 & 5 trong README; Collections có Index B-Tree & Multikey trong MongoDB `instacart_bigdata`. |
| **2** | Dữ liệu gốc hợp lệ, mô tả các cột và cách tải | **ĐẠT** | Mục 4 trong README; Data Dictionary 5 tệp trong Phân hệ 2; Link tải Kaggle chính thức. |
| **3** | Bảng chất lượng dữ liệu trước/sau ETL & mã Spark | **ĐẠT** | Phân hệ 2; đối soát 3.2M đơn; 10 giỏ hàng mẫu thực tế; loại bỏ 1.14M đơn (Apriori Pruning). |
| **4** | Nhật ký stage, thời gian chạy Spark & Spark UI | **ĐẠT** | Phân hệ 3; phân biệt rõ 6 Bước nghiệp vụ vs 8 Stage Spark DAG; `logs/spark_execution_prod.log`; Spark UI cổng 4040; công bố Spark Local Mode. |
| **5** | Frequent itemsets, association rules & giải thích | **ĐẠT** | Phân hệ 4 gồm 3 Tab: Tập phổ biến, Luật kết hợp, Phương pháp luận; ví dụ thật `Limes` → `Large Lemon`; công bố mẫu số $N = 2.066.543$ giỏ; cảnh báo tương quan $\neq$ nhân quả (Mục 6). |
| **6** | Ít nhất 3 cấu hình tham số & bảng so sánh | **ĐẠT** | Phân hệ 6 (Tab 1); 5 cấu hình thử nghiệm `EXP-01` đến `EXP-05` trên 3.2M đơn. |
| **7** | Gợi ý mua kèm có giải thích & fallback minh bạch | **ĐẠT** | Phân hệ 5; giải thích luật kích hoạt + fallback Top Popular có dán nhãn minh bạch. |
| **8** | Đánh giá offline so với baseline & nhận xét | **ĐẠT** | Phân hệ 6 (Tab 2); Precision@K, Recall@K, HitRate@K, Coverage@K vs Baseline; nhận xét hạn chế. |
| **9** | Dashboard tương tác 6 phân hệ, số liệu thật | **ĐẠT** | Ứng dụng Streamlit `app/app.py` với 6 phân hệ chuyên sâu, dẫn dắt câu hỏi, số liệu nguồn thật. |
| **10**| Benchmark truy vấn & hướng dẫn chạy lại | **ĐẠT** | Phân hệ 6 (Tab 3); `explain("executionStats")` IXSCAN (<1ms) vs COLLSCAN (~2.48s); Hướng dẫn chạy lại 7 bước Mục 7. |
| **11**| Test cơ bản, requirements, README, .env.example | **ĐẠT** | Thư mục `tests/` (12 tests PASS), `requirements.txt` (có `pyspark>=3.5.0`), `.env.example`, `run_app.bat`. |
| **12**| Kịch bản demo và câu hỏi phản biện Hội đồng | **ĐẠT** | Thao tác demo trực tiếp 6 phân hệ trên Web App và bộ câu hỏi phản biện trong Báo cáo Đồ án. |
