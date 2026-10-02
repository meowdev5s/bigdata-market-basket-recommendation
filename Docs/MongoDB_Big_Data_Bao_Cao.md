# MongoDB – Nội dung cần trình bày trong báo cáo Big Data

Nếu đề tài của nhóm là **MongoDB** trong môn **Big Data**, không nên chỉ trình bày MongoDB là gì và cách CRUD, mà cần làm nổi bật **tại sao MongoDB phù hợp với Big Data và nó giải quyết vấn đề gì**.

## 1. Giới thiệu MongoDB

- MongoDB là gì?
- Thuộc loại CSDL nào?
- Vì sao MongoDB thuộc **NoSQL**?
- MongoDB khác gì so với CSDL quan hệ như MySQL, SQL Server?
- MongoDB ra đời để giải quyết vấn đề gì?

Có thể dẫn nhập bằng vấn đề:

> Khi dữ liệu ngày càng lớn, thay đổi cấu trúc liên tục và cần xử lý trên nhiều máy, mô hình bảng truyền thống có thể gặp một số hạn chế. MongoDB sử dụng mô hình document để lưu trữ dữ liệu linh hoạt hơn.

---

## 2. MongoDB lưu trữ dữ liệu như thế nào?

Đây là phần **rất quan trọng** vì MongoDB thuộc **Document Data Model**.

Cấu trúc:

```text
Database
   ↓
Collection
   ↓
Document
   ↓
Field
```

Ví dụ:

```json
{
    "_id": 1,
    "name": "Nguyen Van A",
    "age": 20,
    "courses": [
        "Big Data",
        "Database"
    ]
}
```

Giải thích:

- **Database** → cơ sở dữ liệu
- **Collection** → tương tự bảng
- **Document** → tương tự một record
- **Field** → tương tự column
- Document sử dụng **BSON**, có cấu trúc gần giống JSON.

---

## 3. Kiến trúc MongoDB

Nên trình bày các thành phần chính:

```text
                    MongoDB
                       │
        ┌──────────────┴──────────────┐
        │                             │
   Replica Set                    Sharding
        │                             │
 High Availability              Horizontal Scaling
        │                             │
    Replication              Phân tán dữ liệu
```

### Replica Set

- Có nhiều MongoDB server.
- Một node Primary.
- Các node Secondary sao chép dữ liệu.
- Nếu Primary gặp sự cố → có thể bầu Secondary lên Primary.

### Sharding

- Chia dữ liệu thành nhiều phần.
- Phân bố lên nhiều server.
- Giúp MongoDB xử lý **khối lượng dữ liệu lớn** và tăng khả năng mở rộng.

**Với môn Big Data, phần Sharding đặc biệt đáng nói.**

---

## 4. MongoDB giải quyết vấn đề Big Data như thế nào?

Đây là **trọng tâm của bài**.

Có thể liên hệ với 3 đặc điểm lớn:

### Volume – khối lượng dữ liệu lớn

- Có thể phân tán dữ liệu trên nhiều server.
- Sharding giúp mở rộng theo chiều ngang.

### Velocity – tốc độ dữ liệu

- MongoDB có khả năng ghi/đọc nhanh.
- Phù hợp với các hệ thống cần xử lý dữ liệu liên tục.

### Variety – đa dạng dữ liệu

- Document không bắt buộc tất cả record phải có cấu trúc giống hệt nhau.
- Phù hợp dữ liệu JSON, dữ liệu từ web/app, log, IoT...

```text
Big Data
   │
   ├── Volume   → Sharding
   ├── Velocity → High performance
   └── Variety  → Flexible Document Model
```

Đây là phần nên **nhấn mạnh nhất khi thuyết trình**.

---

## 5. Các thao tác cơ bản – CRUD

### Create

```javascript
db.students.insertOne({
    name: "Nguyen Van A",
    age: 20
})
```

### Read

```javascript
db.students.find()
```

### Update

```javascript
db.students.updateOne(
    { name: "Nguyen Van A" },
    { $set: { age: 21 } }
)
```

### Delete

```javascript
db.students.deleteOne({
    name: "Nguyen Van A"
})
```

Có thể gọi đây là **CRUD**.

---

## 6. Index và truy vấn

Nên có một phần ngắn về **Index**:

```javascript
db.students.createIndex({
    name: 1
})
```

Index giúp MongoDB tìm kiếm dữ liệu nhanh hơn thay vì phải quét toàn bộ collection.

Nếu môn Big Data yêu cầu sâu hơn, có thể nói thêm:

- Compound Index
- Text Index
- Geospatial Index
- Aggregation

---

## 7. Aggregation

MongoDB không chỉ dùng để lưu dữ liệu mà còn có thể **phân tích dữ liệu**.

Ví dụ:

```javascript
db.students.aggregate([
    {
        $group: {
            _id: "$class",
            averageAge: {
                $avg: "$age"
            }
        }
    }
])
```

Có thể giải thích:

> Aggregation Pipeline cho phép dữ liệu đi qua nhiều bước xử lý như lọc, nhóm, tính toán và sắp xếp.

Điều này giúp liên hệ MongoDB với **data processing**.

---

## 8. Ưu điểm và nhược điểm

| MongoDB | Ưu điểm | Nhược điểm |
|---|---|---|
| Mô hình Document | Linh hoạt | Không phù hợp mọi bài toán |
| Schema | Flexible Schema | Có thể dẫn đến dữ liệu không đồng nhất |
| Scaling | Hỗ trợ Sharding | Quản lý hệ thống phức tạp hơn |
| Hiệu năng | Đọc/ghi tốt | Không phải workload nào cũng nhanh |
| Phát triển | Dễ làm việc với JSON | Một số quan hệ phức tạp không thuận tiện |

---

## 9. So sánh MongoDB với MySQL

| | MongoDB | MySQL |
|---|---|---|
| Loại | NoSQL | Relational |
| Dữ liệu | Document | Table |
| Schema | Linh hoạt | Chặt chẽ |
| JOIN | Hạn chế hơn | Mạnh |
| Scaling | Horizontal tốt | Thường bắt đầu từ Vertical |
| Dữ liệu lớn/phân tán | Phù hợp | Có thể phù hợp tùy kiến trúc |
| Quan hệ phức tạp | Không phải thế mạnh | Thế mạnh |

Không nên nói MongoDB tốt hơn MySQL tuyệt đối. Nên nói:

> Mỗi hệ quản trị phù hợp với những loại bài toán khác nhau.

---

## 10. Ứng dụng thực tế

Một số ví dụ:

- Website / Web Application
- E-commerce
- Social Network
- IoT
- Log Management
- Real-time analytics
- Content Management
- Big Data systems

Nên đưa **một case study cụ thể**.

Ví dụ:

> Hệ thống thương mại điện tử có hàng triệu sản phẩm và hàng chục triệu lượt truy cập. MongoDB có thể sử dụng document để lưu thông tin sản phẩm linh hoạt, kết hợp index để tìm kiếm và sharding để phân tán dữ liệu.

---

## 11. Demo thực tế

Nên làm một demo nhỏ, ví dụ **hệ thống quản lý sản phẩm / bán hàng**:

```text
                    MongoDB
                       │
                Database: Shop
                       │
              Collection: Products
                       │
       ┌───────────────┼───────────────┐
       ↓               ↓               ↓
    Insert           Query          Update
       │               │               │
       └───────────────┼───────────────┘
                       ↓
                  Aggregation
```

Demo một hệ thống nhỏ sẽ thuyết phục hơn nhiều so với chỉ trình bày slide lý thuyết.

---

# Gợi ý cấu trúc toàn bộ bài thuyết trình

Có thể chia thành khoảng **10 phần**:

1. **MongoDB là gì?**
2. **Vấn đề MongoDB giải quyết**
3. **Document Data Model**
4. **Kiến trúc MongoDB**
5. **CRUD**
6. **Index & Aggregation**
7. **Replication & Sharding**
8. **MongoDB trong Big Data**
9. **Ưu nhược điểm + so sánh MySQL**
10. **Demo + ứng dụng thực tế + kết luận**

---

# Điểm quan trọng nhất

Vì đây là **môn Big Data**, thầy/cô có thể không chỉ muốn nghe:

> "MongoDB là database NoSQL, dùng JSON và có CRUD."

Mà nhóm nên trả lời được câu hỏi:

> **"Tại sao MongoDB lại phù hợp để xử lý dữ liệu lớn và dữ liệu phân tán?"**

Từ đó tập trung vào:

**Document Model → Flexible Schema → Replication → Sharding → Horizontal Scaling → Big Data use cases.**
