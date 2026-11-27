# KỊCH BẢN THUYẾT TRÌNH VÀ DEMO ĐỒ ÁN BIG DATA
## Đề tài: Phân tích hành vi mua sắm và Gợi ý sản phẩm mua kèm
**Học phần:** Các công nghệ dữ liệu lớn (Big Data)  
**Nhóm thực hiện:** Nhóm 11  

---

## 1. Mở đầu & Đặt vấn đề (1 - 2 Phút)
* **Lời chào:** "Kính thưa Thầy/Cô trong Hội đồng chấm đồ án, hôm nay Nhóm 11 xin phép báo cáo đề tài: *Phân tích hành vi mua sắm và Gợi ý sản phẩm mua kèm trên nền tảng Apache Spark và MongoDB*."
* **Mục tiêu kỹ thuật:** Giải quyết bài toán quy mô Big Data trên **32.434.489 dòng giao dịch** (3.214.874 đơn hàng Instacart Prior) thông qua chuỗi giá trị khép kín:
  $$\text{Dữ liệu thô CSV (32.4M dòng)} \longrightarrow \text{Spark ETL & Lọc tần suất} \longrightarrow \text{FP-Growth (2.066.543 giỏ)} \longrightarrow \text{MongoDB NoSQL} \longrightarrow \text{Gợi ý Real-time} \approx 0.25\text{ms}$$
* **Phạm vi xử lý:** Hệ thống kiểm định đầy đủ 6 tệp CSV của bộ dữ liệu Instacart và tập trung huấn luyện mô hình trên tập lịch sử **Prior** (3.214.874 đơn hàng gốc, 49.688 sản phẩm, thu được 2.066.543 giỏ hàng hợp lệ $\ge 2$ món cốt lõi).

---

## 2. Kịch bản Demo 6 Phân hệ trên Giao diện Streamlit (5 - 6 Phút)

### Phân hệ 1: Tổng quan dữ liệu & Hành vi mua sắm
* **Câu hỏi dẫn dắt:** *"Khách hàng mua gì nhiều nhất và thời điểm nào là cao điểm đặt hàng?"*
* **Thao tác demo:**
  * Chỉ vào 5 thẻ KPI: Tổng đơn hàng gốc (**3.214.874**), Giỏ đưa vào FP-Growth (**2.066.543**), Tổng giao dịch (**32.434.489**), Số luật kết hợp (**102**), và Thời gian huấn luyện FP-Growth (**62.60 s**).
  * Chỉ vào biểu đồ Top 10: "Chuối (`Banana`: 472.565 lượt) và Chuối hữu cơ (`Bag of Organic Bananas`: 379.450 lượt) dẫn đầu toàn sàn."
  * Chỉ vào Biểu đồ giờ & Bản đồ nhiệt 168 ô: "Cao điểm mua sắm tập trung từ **10:00 đến 15:00** (đỉnh 10h đạt 271.885 đơn), đặc biệt vào **Chủ Nhật (Ngày 0: 557.772 đơn) và Thứ Hai (Ngày 1: 556.705 đơn)**. Tổng 24 khung giờ và 168 ô nhiệt đều khớp tuyệt đối 3.214.874 đơn gốc."

### Phân hệ 2: Khám phá & Chất lượng dữ liệu
* **Câu hỏi dẫn dắt:** *"Dữ liệu ban đầu ra sao và đã được kiểm định làm sạch như thế nào?"*
* **Thao tác demo:**
  * Mở Bảng Data Catalog **6 tệp CSV Instacart** và Từ điển dữ liệu (chỉ rõ 5 tệp tham gia pipeline Prior và tệp `order_products__train.csv` là tập đánh giá độc lập).
  * Nhấn mạnh Bảng đối soát **Before / After ETL** và phân rã **1.148.331 đơn hàng bị loại** thành 3 nhóm loại trừ lẫn nhau được đếm trực tiếp từ dữ liệu:
    * **Nhóm 1 ($C_{\text{raw}} = 1$):** **156.748 đơn** ($4{,}88\%$) vốn chỉ mua 1 món ngay từ ban đầu.
    * **Nhóm 2 ($C_{\text{raw}} \ge 2, C_{\text{core}} = 0$):** **469.014 đơn** ($14{,}59\%$) chỉ chứa toàn sản phẩm hiếm ($< 15.000$ lượt mua).
    * **Nhóm 3 ($C_{\text{raw}} \ge 2, C_{\text{core}} = 1$):** **522.569 đơn** ($16{,}25\%$) chỉ còn lại 1 món cốt lõi sau khi lọc tần suất.
    * Tổng 3 nhóm = **1.148.331 đơn**, còn lại đúng **2.066.543 giỏ hàng hợp lệ** ($\ge 2$ món cốt lõi).
  * Chỉ vào bảng truy vết chuỗi 11 đơn hàng liên tiếp (`order_id` từ 2 đến 12).

### Phân hệ 3: Spark ETL & Quy trình xử lý
* **Câu hỏi dẫn dắt:** *"Apache Spark đã thực thi quy trình phân tán thế nào và mất bao lâu?"*
* **Thao tác demo:**
  * Giới thiệu lượt chạy thực tế `RUN-SPARK-BIGDATA-LIVE` trên **Apache Spark 3.5.9** (tổng thời gian E2E 6 bước là **923.27 giây**, trong đó `FPGrowth.fit` là **62.60 giây** và nạp 2.06M giỏ kèm đánh chỉ mục vào MongoDB là **143.10 giây**).
  * Chuyển đổi giữa 3 Tab:
    * **Tab 1:** 6 Bước nghiệp vụ tuần tự.
    * **Tab 2:** Các Stage vật lý trong Spark DAG Scheduler.
    * **Tab 3:** Nhật ký thực thi `logs/spark_execution_prod.log` với dấu thời gian thực tế của từng Job.
  * Nhấn mạnh: Hệ thống vận hành minh bạch ở chế độ **Spark Local Mode (`local[*]`)** trên 1 máy trạm đa luồng, 4GB RAM Driver, G1GC.

### Phân hệ 4: Khai phá FP-Growth & Luật kết hợp
* **Câu hỏi dẫn dắt:** *"Những quy luật mua kèm nào được tìm thấy và bản chất thuật toán ra sao?"*
* **Thao tác demo:**
  * **Tab 1 - Tập mục phổ biến (Pha 1):** Trình bày **350 tập mục phổ biến** (251 tập đơn lẻ $k=1$ và 99 tập đa sản phẩm $k \ge 2$), giải thích cây tiền tố FP-Tree nén dữ liệu.
  * **Tab 2 - Tập luật kết hợp (Pha 2):** Trình bày **102 luật kết hợp có hướng** ($A \rightarrow B$), bộ lọc động theo `Min Lift` và `Min Confidence`.
  * **Tab 3 - Phương pháp luận & Giải thích thuật toán:**
    * Trình bày **Ví dụ thực tế** trên mẫu số $N = 2.066.543$ giỏ hàng: Cặp `{Limes, Large Lemon}` xuất hiện trong **27.403 giỏ** ($\text{Support} = 1{,}33\%$), với `Limes` có **136.407 giỏ** ($\text{Support} = 6{,}60\%$) và `Large Lemon` có **147.434 giỏ** ($\text{Support} = 7{,}13\%$) $\rightarrow$ Luật `{Limes} → {Large Lemon}` đạt $\text{Confidence} = 20{,}09\%$ và $\text{Lift} = 2{,}82$.
    * Nhấn mạnh **Cảnh báo phương pháp luận: Correlation $\neq$ Causation (Tương quan không phải nhân quả)**.

### Phân hệ 5: Ứng dụng gợi ý mua kèm Real-time
* **Câu hỏi dẫn dắt:** *"Khi khách chọn sản phẩm vào giỏ, hệ thống đề xuất gì và tại sao?"*
* **Thao tác demo:**
  * Chọn `Limes` vào giỏ hàng $\rightarrow$ Hệ thống kích hoạt In-memory Rule Engine trong **~0.25 ms**, đề xuất **`Organic Cilantro` (Lift 3.91x, Conf 12.9%)** và **`Large Lemon` (Lift 2.82x, Conf 20.1%)**.
  * Bấm nút **"Món chưa có luật (Fallback)"** (`Cold Brew Coffee Tahitian Vanilla`) $\rightarrow$ Hệ thống tự động chuyển sang cơ chế dự phòng Top Popular toàn sàn, dán nhãn minh bạch `[Fallback: Sản phẩm phổ biến toàn sàn]`.

### Phân hệ 6: Thực nghiệm & Đánh giá chất lượng
* **Câu hỏi dẫn dắt:** *"Mô hình hoạt động hiệu quả đến mức nào khi so sánh với phương pháp truyền thống?"*
* **Thao tác demo:**
  * **Tab 1 - Ma trận tham số:** 5 cấu hình thực nghiệm `EXP-01` đến `EXP-05` đo thực tế trên tập 3.2M đơn (số tập phổ biến tăng từ 162 lên 1.088; số luật tăng từ 24 lên 691).
  * **Tab 2 - Offline Evaluation (Leave-k-out):** Truy vấn **1.000 giỏ hàng thực tế** từ `orders_prior` (**1.000/1.000 giỏ hợp lệ** $\ge 2$ món):
    * Tại $K=10$, FP-Growth vượt Baseline Top Popular trên cả 3 chỉ số: **HitRate@10 đạt 61.1%** (vs 60.5%), **Precision@10 đạt 9.1%** (vs 8.9%), **Recall@10 đạt 26.3%** (vs 25.9%).
    * Tại $K=5$, **Catalog Coverage đạt 6.9%**, cao gấp **3.1 lần** so với Baseline (**2.2%**), tiệm cận trần học thuật 6.93% (19 hậu tố / 274 sản phẩm cốt lõi).
  * **Tab 3 - Benchmark MongoDB:** Đối chiếu 2 tình huống truy vấn trên 2.066.543 bản ghi `orders_prior`: Truy vấn trường có chỉ mục B-Tree `order_id_1` (`FETCH -> IXSCAN`, duyệt 1 key & 1 doc, **< 1 ms**) so với truy vấn trường không có chỉ mục `items_count` (`COLLSCAN`, quét 2.066.543 docs, **~2.283 ms**).
