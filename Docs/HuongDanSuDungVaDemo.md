# HƯỚNG DẪN SỬ DỤNG VÀ KỊCH BẢN DEMO HỆ THỐNG
## Đề tài: Phân tích hành vi mua sắm và Gợi ý sản phẩm mua kèm
**Học phần:** Các công nghệ dữ liệu lớn (Big Data) — **Nhóm thực hiện:** Nhóm 11  
**Nền tảng công nghệ:** Apache Spark MLlib 3.5.3, MongoDB NoSQL, Python 3.13, Streamlit

---

## 1. Tổng Quan Kiến Trúc & Quy Trình Trình Diễn
Hệ thống được thiết kế theo kiến trúc **Lambda-lite 2 tầng**:
1. **Tầng tính toán phân tán (Batch Compute Layer - Apache Spark):** Đọc và xử lý toàn bộ 32.4 triệu dòng giao dịch và 3.2 triệu đơn hàng Instacart, nén cấu trúc cây FP-Tree trong RAM để khai phá 82 tập mục phổ biến và 102 luật kết hợp.
2. **Tầng phục vụ trực tuyến (Online Serving Layer - MongoDB & Streamlit):** Lưu trữ 2.06 triệu giỏ hàng và danh mục luật với Multikey Index, phục vụ gợi ý tức thì dưới 2ms trên giao diện web gồm 6 phân hệ chuyên sâu.

```text
[ Dữ liệu thô 32.4M dòng ] ──> [ Apache Spark ETL & FP-Growth ] ──> [ MongoDB NoSQL ] ──> [ Streamlit Web UI ]
                                          │                                                      │
                                          ▼                                                      ▼
                                [ Spark UI cổng 4040 ]                                 [ Gợi ý Real-time < 2ms ]
```

---

## 2. Hướng Dẫn Thao Tác Chi Tiết 6 Phân Hệ Trên Giao Diện Web

### Phân hệ 1: Tổng Quan Dữ Liệu & Hành Vi
* **Mục tiêu:** Giới thiệu bức tranh toàn cảnh về quy mô dữ liệu và hành vi tiêu dùng thời gian thực.
* **Các thành phần quan sát:**
  - **4 Thẻ KPI chính:**
    * *Tổng đơn hàng gốc:* **3.214.874** đơn.
    * *Giỏ đưa vào khai phá:* **2.066.543** giỏ (đã qua lọc $\ge 2$ món thường xuyên).
    * *Tổng dòng giao dịch:* **32.434.489** dòng.
    * *Số luật kết hợp:* **102** luật (lọc $\text{Lift} > 1.2$).
    * *Thời gian huấn luyện:* **91.66s** (Spark MLlib FP-Growth trên máy trạm).
  - **Biểu đồ Top 10 sản phẩm bán chạy:** Chuối (`Banana`: 472.565 lượt) và Chuối hữu cơ (`Bag of Organic Bananas`: 379.445 lượt) dẫn đầu toàn sàn.
  - **Biểu đồ khung giờ & Bản đồ nhiệt 168 ô:** Cao điểm mua sắm tập trung từ **10:00 - 15:00** vào **Chủ Nhật (Ngày 0)** và **Thứ Hai (Ngày 1)**.
  - **Minh chứng đối soát:** Tổng 24 khung giờ = 3.214.874 đơn; Tổng 168 ô heatmap = 3.214.874 đơn (khớp 100% với đơn gốc).

### Phân hệ 2: Khám Phá & Chất Lượng Dữ Liệu
* **Mục tiêu:** Minh chứng tính toàn vẹn và quy trình làm sạch dữ liệu nguồn nghiêm ngặt.
* **Các thành phần quan sát:**
  - **Bảng Data Catalog:** Liệt kê 5 tệp CSV gốc (3.42M orders, 32.4M order_products, 49.688 products, 134 aisles, 21 departments) kèm khóa chính và đường dẫn Kaggle chính thức.
  - **Bảng đối soát Before / After ETL:**
    * Đơn hàng ban đầu: 3.214.874 đơn.
    * Đơn hàng bị loại bỏ (Apriori Pruning): 1.148.331 đơn (198.924 đơn 1 món ban đầu; 386.150 đơn không chứa sản phẩm $\ge 15.000$ lượt mua; 563.257 đơn chỉ còn 1 món sau khi lọc tần suất).
    * Đơn hàng hợp lệ đưa vào khai phá: 2.066.543 giỏ.
  - **10 Giỏ hàng mẫu thực tế:** Trích xuất trực tiếp từ Spark DataFrame, minh chứng các giỏ hàng đều thỏa mãn $|T_k| \ge 2$.

### Phân hệ 3: Spark ETL & Quy Trình Xử Lý
* **Mục tiêu:** Thể hiện bằng chứng vận hành thực tế của Apache Spark Engine.
* **Các thành phần quan sát:**
  - **Tab 1 - 6 Bước nghiệp vụ tuần tự:** Mô tả chi tiết từ khởi tạo SparkSession, đọc song song CSV, khử trùng lặp `collect_set`, lọc giỏ hàng, huấn luyện FP-Growth và nạp MongoDB.
  - **Tab 2 - 8 Stage vật lý trong Spark DAG:** Phân tách chính xác tại các ranh giới Shuffle Boundary (`groupBy`, `join`, `dropDuplicates`), hiển thị biểu đồ thời gian thực thi (tổng 91.66s).
  - **Tab 3 - Spark Execution Log:** Trích xuất nhật ký thực thi thực tế từ tệp `logs/spark_execution_prod.log`.
  - **Công bố minh bạch:** Chế độ vận hành **Spark Local Mode (`local[*]`)** trên 01 máy trạm, Driver RAM 4GB, G1GC.

### Phân hệ 4: Khai Phá FP-Growth & Luật Kết Hợp
* **Mục tiêu:** Khai phá tri thức giỏ hàng và giải thích cơ chế thuật toán một cách khoa học.
* **Các thành phần quan sát:**
  - **Tab 1 - Tập mục phổ biến (Pha 1):** 82 $k$-itemsets ($k=1, 2, 3$), bộ lọc kích thước $k$ và tên sản phẩm, biểu đồ Top 10 tập phổ biến có Support cao nhất.
  - **Tab 2 - Tập luật kết hợp (Pha 2):** 102 luật kết hợp có hướng ($A \rightarrow B$), biểu đồ phân tán Support vs Confidence theo Lift, Top 10 luật có Lift cao nhất, bộ lọc động theo Slider Min Lift và Min Confidence.
  - **Tab 3 - Phương pháp luận & Giải thích thuật toán:**
    * Ví dụ thực tế: `{Limes, Large Lemon, Organic Garlic, Onion}` $\rightarrow$ tập phổ biến `{Limes, Large Lemon}` (Freq: 40.880, Supp: 1.98%) $\rightarrow$ luật `{Limes} → {Large Lemon}` (Conf: 28.92%, Lift: 3.91).
    * Bảng chuẩn hóa công thức toán học và **mẫu số sử dụng cụ thể**: Support tính trên đúng **2.066.543 giỏ hàng hợp lệ**.
    * **Cảnh báo khoa học:** Độ nâng Lift đo lường sự tương quan đồng thời trong giỏ hàng (Co-occurrence Correlation), tuyệt đối **KHÔNG PHẢI quan hệ nhân quả (Correlation $\neq$ Causation)**.

### Phân hệ 5: Ứng Dụng Gợi Ý Mua Kèm Real-time
* **Mục tiêu:** Trình diễn tính năng gợi ý bán chéo thời gian thực với độ trễ phản hồi dưới 2ms.
* **Các tình huống demo:**
  - **Kịch bản 1 (Combo Chanh & Rau):** Bấm nút chọn nhanh `Limes` và `Organic Lemon` $\rightarrow$ Hệ thống lập tức kích hoạt luật gợi ý `Organic Cilantro` và `Large Lemon` kèm công thức luật và chỉ số đo lường.
  - **Kịch bản 2 (Combo Hành & Tỏi):** Chọn `Organic Yellow Onion` và `Organic Garlic` $\rightarrow$ Hệ thống gợi ý mua kèm các loại rau củ hữu cơ có Lift cao.
  - **Kịch bản 3 (Cơ chế Fallback minh bạch):** Chọn sản phẩm ít phổ biến hoặc không có luật kích hoạt $\rightarrow$ Hệ thống tự động chuyển sang cơ chế Fallback gợi ý Top 3 sản phẩm bán chạy nhất toàn sàn, dán nhãn rõ ràng: `[Fallback: Sản phẩm phổ biến toàn sàn]`.

### Phân hệ 6: Thực Nghiệm & Đánh Giá Chất Lượng
* **Mục tiêu:** Chứng minh tính hiệu quả của mô hình thông qua thực nghiệm khoa học.
* **Các thành phần quan sát:**
  - **Tab 1 - Ma trận tham số:** 5 cấu hình thử nghiệm `EXP-01` đến `EXP-05` trên 3.2M đơn, phân tích sự thay đổi số lượng luật và thời gian thực thi theo `minSupport` và `minConfidence`.
  - **Tab 2 - Offline Evaluation:** Đánh giá Leave-k-out trên 5.000 giỏ test độc lập. Ở $K=5$, FP-Growth đạt HitRate **54.8%** (vượt trội 1.84 lần Baseline Top Popular), Precision@5 đạt **18.8%** (gấp 2.3 lần Baseline), Catalog Coverage đạt **74.8%** trên 274 sản phẩm thường xuyên.
  - **Tab 3 - Benchmark MongoDB:** So sánh thời gian truy vấn có chỉ mục `IXSCAN` (1.15ms) vs quét toàn bộ `COLLSCAN` (4.8ms), giảm 76% độ trễ phản hồi.

---

## 3. Quy Trình Chạy Lại Hệ Thống Từ A Đến Z (Reproducibility Guide)

```powershell
# Bước 1: Kích hoạt môi trường ảo Python
.\.venv\Scripts\activate

# Bước 2: Cài đặt đầy đủ dependencies (bao gồm PySpark 3.5.x)
pip install -r requirements.txt

# Bước 3: Đảm bảo MongoDB đang chạy tại localhost:27017

# Bước 4 (Tùy chọn): Nạp dữ liệu vào MongoDB
python scripts/import_to_mongodb.py

# Bước 5: Chạy Spark ETL & FP-Growth trên toàn bộ 3.2M đơn
python scripts/train_fpgrowth_prior_3m.py

# Bước 6: Sinh báo cáo thực nghiệm và benchmark
python scripts/generate_metrics_and_evaluation.py

# Bước 7: Khởi chạy Web Dashboard
.\run_app.bat
# Hoặc: .\.venv\Scripts\streamlit.exe run app/app.py

# Bước 8: Chạy toàn bộ Unit Tests kiểm thử
.\.venv\Scripts\python.exe -m unittest discover tests
```

---

## 4. Danh Sách Câu Hỏi Phản Biện Mẫu Của Hội Đồng & Hướng Dẫn Trả Lời

1. **Vì sao Confidence của các luật chỉ đạt 15% - 28%, con số này có thấp không?**  
   *Trả lời:* Trong không gian 50.000 sản phẩm cực kỳ thưa thớt của siêu thị, xác suất ngẫu nhiên một món hàng lọt vào giỏ chỉ dưới 1% - 2%. Mức Confidence 20% - 28% đồng nghĩa với việc khách mua A có xác suất chọn tiếp B cao gấp 3 đến 4 lần so với ngẫu nhiên (Lift từ 3.0x - 4.0x). Đây là con số phản ánh đúng quy luật thống kê trong ngành bán lẻ.

2. **Hệ thống chạy trên 1 máy hay cụm nhiều máy?**  
   *Trả lời:* Hệ thống vận hành ở chế độ Spark Local Mode (`local[*]`), tận dụng đa luồng trên tất cả nhân CPU của một máy trạm. Nhóm minh bạch công bố rõ giới hạn này trên giao diện và tài liệu. Mã nguồn hoàn toàn tuân thủ Spark DataFrame API chuẩn, sẵn sàng mở rộng lên Spark Cluster (YARN / Kubernetes) khi có hạ tầng cụm.

3. **Vai trò của MongoDB trong đề tài là gì khi đã có Spark?**  
   *Trả lời:* Hệ thống áp dụng kiến trúc phân tách hai tầng: Tầng tính toán lô (Batch Layer) do Apache Spark định kỳ xử lý hàng chục triệu dòng dữ liệu để nén cây FP-Tree và sinh luật; Tầng phục vụ trực tuyến (Serving Layer) do MongoDB NoSQL đảm nhận. Nhờ chỉ mục Multikey Index và lưu trữ 2.06 triệu giỏ hàng, MongoDB giúp hệ thống phản hồi truy vấn dưới 2 mili-giây cho người dùng cuối mà không phải gọi lại Spark nặng nề.

4. **Chỉ số Lift cao (ví dụ Lift = 3.91) có chứng minh quan hệ nhân quả không?**  
   *Trả lời:* Hoàn toàn không. Tương quan thống kê không đồng nghĩa với quan hệ nhân quả (Correlation is not Causation). Lift chỉ lượng hóa sự xuất hiện đồng thời trong giỏ hàng. Khách có thể cùng mua hai món vì một nhu cầu tiềm ẩn chung (Confounding Factor). Để kiểm chứng nhân quả, doanh nghiệp phải thực hiện thử nghiệm ngẫu nhiên A/B Testing.
