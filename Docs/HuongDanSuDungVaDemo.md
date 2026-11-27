# HƯỚNG DẪN SỬ DỤNG VÀ KỊCH BẢN DEMO HỆ THỐNG
## Đề tài: Phân tích hành vi mua sắm và Gợi ý sản phẩm mua kèm
**Học phần:** Các công nghệ dữ liệu lớn (Big Data) — **Nhóm thực hiện:** Nhóm 11  
**Nền tảng công nghệ:** Apache Spark MLlib 3.5.9 (Local Mode), MongoDB NoSQL 7.0+, Python 3.13, Streamlit

---

## 1. Tổng Quan Kiến Trúc & Quy Trình Trình Diễn
Hệ thống được thiết kế theo kiến trúc **Lambda-lite 2 tầng**:
1. **Tầng tính toán theo lô (Batch Compute Layer - Apache Spark):** Đọc trực tiếp và xử lý toàn bộ 32.434.489 dòng giao dịch và 3.214.874 đơn hàng Instacart (`eval_set == 'prior'`), nén cấu trúc cây tiền tố FP-Tree trong RAM để khai phá **350 tập mục phổ biến** (251 tập đơn lẻ $k=1$ và 99 tập đa sản phẩm $k \ge 2$) cùng **102 luật kết hợp mạnh** ($\text{Lift} > 1.2$).
2. **Tầng lưu trữ & phục vụ trực tuyến (Storage & Serving Layer - MongoDB & Streamlit):** Lưu trữ toàn bộ **2.066.543 giỏ hàng hợp lệ** (`orders_prior`), 350 tập phổ biến (`frequent_itemsets`), 102 luật kết hợp (`recommendation_rules_prior`) với hệ thống chỉ mục B-Tree và Multikey Index, đồng thời nạp tập luật vào bộ nhớ RAM (`@st.cache_data`) để phục vụ gợi ý tức thì (~0.25 ms) trên giao diện web gồm 6 phân hệ chuyên sâu.

```text
[ Dữ liệu thô CSV 32.4M dòng ] ──> [ Apache Spark ETL & FP-Growth ] ──(PyMongo)──> [ MongoDB NoSQL ] ──> [ Streamlit Web UI ]
                                              │                                                                │
                                              ▼                                                                ▼
                                    [ Spark UI cổng 4040 ]                                           [ Gợi ý Real-time ~0.25ms ]
```

---

## 2. Hướng Dẫn Thao Tác Chi Tiết 6 Phân Hệ Trên Giao Diện Web

### Phân hệ 1: Tổng Quan Dữ Liệu & Hành Vi
* **Mục tiêu:** Giới thiệu bức tranh toàn cảnh về quy mô dữ liệu và hành vi tiêu dùng.
* **Các thành phần quan sát:**
  - **5 Thẻ KPI chính:**
    * *Tổng đơn hàng gốc:* **3.214.874** đơn (`orders.csv` tập `prior`).
    * *Giỏ đưa vào FP-Growth:* **2.066.543** giỏ (đã qua lọc $\ge 2$ sản phẩm cốt lõi có tần suất $\ge 15.000$).
    * *Tổng giao dịch gốc:* **32.434.489** dòng (`order_products__prior.csv`).
    * *Số luật kết hợp:* **102** luật (lọc $\text{Lift} > 1.2$).
    * *Thời gian huấn luyện:* **62.60 s** (Spark MLlib `FPGrowth.fit` trên 2.066.543 giỏ; tổng thời gian toàn trình E2E gồm nạp 2.06M bản ghi vào MongoDB là **923.27 s**).
  - **Biểu đồ Top 10 sản phẩm bán chạy:** Chuối (`Banana`: 472.565 lượt) và Chuối hữu cơ (`Bag of Organic Bananas`: 379.450 lượt) dẫn đầu toàn sàn.
  - **Biểu đồ khung giờ & Bản đồ nhiệt 168 ô:** Cao điểm mua sắm tập trung từ **10:00 - 15:00** (đỉnh điểm 10h sáng đạt 271.885 đơn) vào **Chủ Nhật (Ngày 0: 557.772 đơn)** và **Thứ Hai (Ngày 1: 556.705 đơn)**.
  - **Minh chứng đối soát:** Tổng 24 khung giờ = 3.214.874 đơn; Tổng 168 ô heatmap = 3.214.874 đơn (khớp 100% với tổng đơn Prior gốc).

### Phân hệ 2: Khám Phá & Chất Lượng Dữ Liệu
* **Mục tiêu:** Minh chứng tính toàn vẹn và quy trình làm sạch, lọc phạm vi khai phá nghiêm ngặt.
* **Các thành phần quan sát:**
  - **Bảng Data Catalog (6 tệp CSV):** Liệt kê đầy đủ 6 tệp CSV của bộ dữ liệu Instacart (`orders.csv`: 3.421.083 dòng, `order_products__prior.csv`: 32.434.489 dòng, `order_products__train.csv`: 1.384.617 dòng, `products.csv`: 49.688 dòng, `aisles.csv`: 134 dòng, `departments.csv`: 21 dòng), nêu rõ `order_products__train.csv` là tập đánh giá tách biệt không nạp vào pipeline Prior.
  - **Bảng đối soát Before / After ETL & Phân rã 3 nhóm đơn bị loại (Tính toán động 100%):**
    * Đơn hàng Prior ban đầu: **3.214.874** đơn.
    * Đơn hàng bị lược bỏ khỏi phạm vi khai phá ($C_{\text{core}} < 2$): **1.148.331** đơn ($35{,}72\%$), được phân hoạch tuần tự không giao nhau thành 3 nhóm:
      1. **Nhóm 1 ($C_{\text{raw}} = 1$):** **156.748** đơn ($4{,}88\%$ tổng đơn Prior) — Đơn vốn chỉ mua đúng 1 món ngay từ dữ liệu gốc.
      2. **Nhóm 2 ($C_{\text{raw}} \ge 2, C_{\text{core}} = 0$):** **469.014** đơn ($14{,}59\%$ tổng đơn Prior) — Đơn có $\ge 2$ món nhưng toàn bộ là sản phẩm hiếm ($< 15.000$ lượt mua).
      3. **Nhóm 3 ($C_{\text{raw}} \ge 2, C_{\text{core}} = 1$):** **522.569** đơn ($16{,}25\%$ tổng đơn Prior) — Đơn sau khi lọc sản phẩm hiếm chỉ còn đúng 1 món cốt lõi.
    * Giỏ hàng hợp lệ đưa vào FP-Growth ($C_{\text{core}} \ge 2$): **2.066.543** giỏ ($64{,}28\%$).
    * Dòng giao dịch: Từ **32.434.489** dòng gốc $\rightarrow$ giữ lại **11.188.797** dòng thuộc 274 sản phẩm cốt lõi.
  - **Bảng truy vết chuỗi đơn hàng liên tiếp (`order_id` từ 2 đến 12):** Trích xuất trực tiếp từ dữ liệu gốc và đối chiếu với MongoDB `orders_prior`.

### Phân hệ 3: Spark ETL & Quy Trình Xử Lý
* **Mục tiêu:** Thể hiện bằng chứng vận hành thực tế của Apache Spark Engine.
* **Các thành phần quan sát:**
  - **Tab 1 - 6 Bước nghiệp vụ tuần tự:** Từ khởi tạo SparkSession (18.70s), đọc song song CSV & phân tích giờ (16.60s), lọc ngưỡng tần suất $\ge 15.000$ (7.05s), gom giỏ hàng `collect_set` (33.33s), huấn luyện FP-Growth (62.60s) đến nạp 2.06M giỏ vào MongoDB (143.10s).
  - **Tab 2 - Các Stage vật lý trong Spark DAG:** Phân tách tại các ranh giới Shuffle Boundary (`groupBy`, `join`, `dropDuplicates`).
  - **Tab 3 - Spark Execution Log:** Hiển thị trực tiếp tệp nhật ký `logs/spark_execution_prod.log` với dấu thời gian thực tế của từng Job.
  - **Công bố minh bạch:** Vận hành ở chế độ **Spark Local Mode (`local[*]`)** trên 01 máy trạm cục bộ (PySpark 3.5.9, Driver RAM 4GB, G1GC).

### Phân hệ 4: Khai Phá FP-Growth & Luật Kết Hợp
* **Mục tiêu:** Khai phá tri thức giỏ hàng và giải thích cơ chế thuật toán một cách khoa học.
* **Các thành phần quan sát:**
  - **Tab 1 - Tập mục phổ biến (Pha 1):** **350** $k$-itemsets (gồm **251** mặt hàng đơn lẻ $k=1$ và **99** tổ hợp mua kèm $k \ge 2$), bộ lọc kích thước $k$ và tên sản phẩm.
  - **Tab 2 - Tập luật kết hợp (Pha 2):** **102** luật kết hợp có hướng ($A \rightarrow B$), biểu đồ Scatter Plot Support vs Confidence tô màu theo Lift, Top 10 luật có Lift cao nhất.
  - **Tab 3 - Phương pháp luận & Giải thích thuật toán:**
    * Ví dụ thực tế trên mẫu số $N = 2.066.543$ giỏ hàng hợp lệ:
      - Cặp `{Limes, Large Lemon}` đồng xuất hiện trong **27.403 giỏ** ($\text{Support} = 1{,}326\%$).
      - Số giỏ chứa `{Limes}`: **136.407 giỏ** ($\text{Support} = 6{,}6007\%$).
      - Số giỏ chứa `{Large Lemon}`: **147.434 giỏ** ($\text{Support} = 7{,}1343\%$).
      - Luật `{Limes} → {Large Lemon}` có $\text{Confidence} = 27.403 / 136.407 = 20{,}09\%$ và $\text{Lift} = 20{,}089\% / 7{,}1343\% = 2{,}82$.
      - Luật có Lift cao nhất là `{Organic Cilantro} ↔ {Limes}` với $\text{Lift} = 3{,}9115$ (chiều `Limes → Organic Cilantro` có $\text{Confidence} = 12{,}88\%$; chiều `Organic Cilantro → Limes` có $\text{Confidence} = 25{,}82\%$).
    * **Cảnh báo khoa học:** Độ nâng Lift chỉ đo lường sự tương quan đồng thời (Co-occurrence Correlation), tuyệt đối **KHÔNG PHẢI quan hệ nhân quả (Correlation $\neq$ Causation)**.

### Phân hệ 5: Ứng Dụng Gợi Ý Mua Kèm Real-time
* **Mục tiêu:** Trình diễn tính năng gợi ý bán chéo thời gian thực với độ trễ so khớp trong bộ nhớ ~0.25 ms.
* **Các tình huống demo:**
  - **Kịch bản 1 (Combo Chanh & Rau):** Chọn `Limes` $\rightarrow$ Hệ thống lập tức kích hoạt luật gợi ý `Organic Cilantro` (Lift 3.91x, Conf 12.9%) và `Large Lemon` (Lift 2.82x, Conf 20.1%).
  - **Kịch bản 2 (Combo Hành & Tỏi):** Chọn `Organic Yellow Onion` hoặc `Organic Garlic` $\rightarrow$ Hệ thống gợi ý mặt hàng tương ứng với Lift 3.80x (Conf ~20%).
  - **Kịch bản 3 (Cơ chế Fallback minh bạch):** Chọn sản phẩm chưa có luật kết hợp (ví dụ `Cold Brew Coffee Tahitian Vanilla`) $\rightarrow$ Hệ thống chuyển sang cơ chế Fallback đề xuất Top 3 sản phẩm phổ biến toàn sàn (loại trừ món đã có trong giỏ) và dán nhãn rõ ràng: `[Fallback: Sản phẩm phổ biến toàn sàn]`.

### Phân hệ 6: Thực Nghiệm & Đánh Giá Chất Lượng
* **Mục tiêu:** Chứng minh tính hiệu quả và minh bạch của hệ thống qua 3 bài thực nghiệm định lượng.
* **Các thành phần quan sát:**
  - **Tab 1 - Ma trận 5 cấu hình tham số (`EXP-01` đến `EXP-05`):** Đo đạc thực tế sự biến thiên số tập phổ biến (từ 162 lên 1.088 tập) và số luật kết hợp (từ 24 lên 691 luật) khi thay đổi `minSupport` (1.0% xuống 0.3%) và `minConfidence` (15% xuống 3%). Cấu hình chuẩn `EXP-03*` ($\text{minSupport} \approx 0{,}726\%, \text{minConfidence} = 8\%$) sinh ra 350 tập phổ biến và 102 luật kết hợp.
  - **Tab 2 - Offline Evaluation (Leave-k-out):** Truy vấn **1.000 giỏ hàng thực tế** từ `orders_prior` (cả **1.000/1.000 giỏ đều hợp lệ** $\ge 2$ món tham gia đánh giá, giữ sản phẩm đầu tiên làm Observed Basket, phần còn lại làm Held-out Ground Truth):
    * Tại $K=10$, FP-Growth vượt lên dẫn trước Baseline Top Popular trên cả 3 chỉ số: **HitRate@10 đạt 61.1%** (so với 60.5%), **Precision@10 đạt 9.1%** (so với 8.9%) và **Recall@10 đạt 26.3%** (so với 25.9%).
    * Về độ bao phủ danh mục (**Catalog Coverage** trên 274 sản phẩm cốt lõi): 102 luật hội tụ về 19 sản phẩm hậu tố thiết yếu (trần lý thuyết của riêng vế hệ quả là $19/274 \approx 6{,}93\%$). Tại $K=5$, FP-Growth đạt **6.9%**, cao gấp **3.1 lần** so với Baseline Top Popular (chỉ đạt **2.2%**).
  - **Tab 3 - Benchmark Chỉ mục MongoDB:** Đối chiếu 2 tình huống truy vấn thực tế trên `orders_prior` (2.066.543 documents):
    * **Tình huống 1 (Truy vấn theo trường có chỉ mục B-Tree `order_id_1`):** Kế hoạch `FETCH -> IXSCAN`, duyệt đúng 1 key và 1 document, thời gian thực thi **< 1 ms (0–6 ms)**.
    * **Tình huống 2 (Truy vấn theo trường chưa lập chỉ mục `items_count`):** Kế hoạch `COLLSCAN`, phải quét tuần tự toàn bộ 2.066.543 documents, thời gian thực thi **~2.218 – 2.283 ms (~2.3 giây)**.

---

## 3. Quy Trình Chạy Lại Hệ Thống Từ A Đến Z (Reproducibility Guide)

```powershell
# Bước 1: Kích hoạt môi trường ảo Python
.\.venv\Scripts\activate

# Bước 2: Cài đặt đầy đủ dependencies (bao gồm PySpark 3.5.9)
pip install -r requirements.txt

# Bước 3: Đảm bảo MongoDB đang chạy tại localhost:27017

# Bước 4: Nạp danh mục sản phẩm vào MongoDB
python scripts/import_to_mongodb.py

# Bước 5: Chạy Spark ETL & FP-Growth trên toàn bộ 3.2M đơn
python scripts/train_fpgrowth_prior_3m.py

# Bước 6: Chạy thực nghiệm 5 cấu hình & sinh báo cáo đánh giá động
python scripts/benchmark_parameter_experiments.py
python scripts/generate_metrics_and_evaluation.py

# Bước 7: Khởi chạy Web Dashboard
.\run_app.bat
# Hoặc: .\.venv\Scripts\streamlit.exe run app/app.py

# Bước 8: Chạy toàn bộ 12 Unit Tests kiểm thử tự động
.\.venv\Scripts\python.exe -m unittest discover tests -v
```
