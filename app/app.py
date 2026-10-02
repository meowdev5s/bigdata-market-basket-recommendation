import os
import json
import time
import pandas as pd
import streamlit as st
import plotly.express as px
from pymongo import MongoClient

# Page configuration
st.set_page_config(
    page_title="Big Data - Gợi ý sản phẩm mua kèm | Nhóm 11",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Paths
APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_DIR, "data")
RULES_FILE = os.path.join(DATA_DIR, "recommendation_rules.json")
CACHE_FILE = os.path.join(DATA_DIR, "dashboard_cache.json")

# Connect to MongoDB (supports local MongoDB or cloud Atlas via MONGO_URI)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "instacart_bigdata"

@st.cache_resource
def get_db():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500)
        client.server_info()
        return client[DB_NAME]
    except Exception:
        return None

db = get_db()

# Custom CSS for clean, formal styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.1rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 0.2rem;
        letter-spacing: -0.5px;
    }
    .sub-header {
        font-size: 1.05rem;
        color: #4B5563;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background-color: #F8FAFC;
        border-radius: 8px;
        padding: 15px;
        border: 1px solid #E2E8F0;
        border-left: 4px solid #2563EB;
        margin-bottom: 10px;
    }
    .rec-card {
        background-color: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-left: 4px solid #2563EB;
        border-radius: 6px;
        padding: 14px;
        margin-bottom: 10px;
    }
    .cart-tag {
        background: #F1F5F9;
        color: #334155;
        border: 1px solid #CBD5E1;
        padding: 4px 12px;
        border-radius: 4px;
        margin: 3px;
        display: inline-block;
        font-size: 0.9rem;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar
st.sidebar.markdown("**Đề tài: Phân tích hành vi mua sắm & Gợi ý sản phẩm mua kèm**")
st.sidebar.markdown("---")

app_mode = st.sidebar.radio(
    "CHỌN PHÂN HỆ:",
    ["Gợi Ý Sản Phẩm Mua Kèm", "Dashboard Phân Tích Hành Vi", "Danh Sách Luật Kết Hợp"]
)

# Helper function to load all rules (from MongoDB or exported JSON)
@st.cache_data(ttl=600)
def load_all_rules():
    if db is not None:
        try:
            return list(db["recommendation_rules"].find({}, {"_id": 0}))
        except Exception:
            pass
    if os.path.exists(RULES_FILE):
        with open(RULES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

# Helper function to load dashboard cache
@st.cache_data(ttl=600)
def load_dashboard_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

# Load list of products for multiselect
@st.cache_data(ttl=600)
def get_popular_products():
    rules = load_all_rules()
    rule_items = set()
    for r in rules:
        for item in r.get("antecedent", []):
            rule_items.add(item)
            
    if db is not None:
        try:
            col_orders = db["orders"]
            pipeline = [
                {"$unwind": "$items"},
                {"$group": {"_id": "$items", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 100}
            ]
            results = list(col_orders.aggregate(pipeline))
            top_items = [r["_id"] for r in results]
            return sorted(list(rule_items.union(set(top_items))))
        except Exception:
            pass
    return sorted(list(rule_items))

# -------------------------------------------------------------
# PHÂN HỆ 1: DEMO GỢI Ý SẢN PHẨM MUA KÈM (LIVE RECOMMENDATION)
# -------------------------------------------------------------
if app_mode == "Gợi Ý Sản Phẩm Mua Kèm":
    st.markdown('<div class="main-header">HỆ THỐNG GỢI Ý SẢN PHẨM MUA KÈM</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Mô hình phân tán Apache Spark FP-Growth kết hợp cơ sở dữ liệu NoSQL MongoDB</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns([1.2, 1])
    
    popular_products = get_popular_products()
    
    with col1:
        st.subheader("1. Chọn sản phẩm vào giỏ hàng")
        default_items = ["Yellow Bell Pepper", "Banana"] if "Yellow Bell Pepper" in popular_products else popular_products[:2]
        selected_cart = st.multiselect(
            "Tìm và chọn các sản phẩm trong giỏ hàng:",
            options=popular_products,
            default=[item for item in default_items if item in popular_products],
            help="Chọn ít nhất 1 sản phẩm để hệ thống kích hoạt luật gợi ý liên kết."
        )
        
        st.markdown(f"**Số lượng sản phẩm trong giỏ:** `{len(selected_cart)}`")
        
        # Display selected items in clean tags
        if selected_cart:
            cart_html = "".join([f"<span class='cart-tag'>{item}</span>" for item in selected_cart])
            st.markdown(cart_html, unsafe_allow_html=True)
        else:
            st.info("Vui lòng chọn ít nhất 1 sản phẩm vào giỏ hàng để nhận gợi ý.")

    with col2:
        st.subheader("2. Sản phẩm thường được mua kèm")
        
        if selected_cart:
            t_start = time.time()
            all_rules = load_all_rules()
            
            cart_set = set(selected_cart)
            valid_recommendations = {}
            
            for rule in all_rules:
                ant_set = set(rule["antecedent"])
                # Condition: antecedent ⊆ cart_items
                if ant_set.issubset(cart_set):
                    for cons in rule["consequent"]:
                        if cons not in cart_set:
                            if cons not in valid_recommendations:
                                valid_recommendations[cons] = rule
                            else:
                                if rule["lift"] > valid_recommendations[cons]["lift"]:
                                    valid_recommendations[cons] = rule
                                    
            latency_ms = (time.time() - t_start) * 1000
            
            mode_label = "MongoDB NoSQL" if db is not None else "Đồng bộ từ MongoDB"
            st.caption(f"Cơ chế: {mode_label} | Độ trễ phản hồi: {latency_ms:.2f} ms | Số gợi ý: {len(valid_recommendations)}")
            
            if valid_recommendations:
                sorted_recs = sorted(valid_recommendations.values(), key=lambda x: x["lift"], reverse=True)
                
                for i, r in enumerate(sorted_recs[:5], start=1):
                    cons_name = r["consequent"][0] if len(r["consequent"]) == 1 else ", ".join(r["consequent"])
                    ant_name = ", ".join(r["antecedent"])
                    lift_val = r["lift"]
                    conf_val = r["confidence"] * 100
                    supp_val = r["support"] * 100
                    
                    st.markdown(f"""
                    <div class="rec-card">
                        <div style="font-size: 1.1rem; font-weight: 600; color: #1E3A8A;">
                            #{i}. {cons_name}
                        </div>
                        <div style="font-size: 0.88rem; color: #475569; margin-top: 4px;">
                            Sản phẩm liên quan trong giỏ: <b>{ant_name}</b>
                        </div>
                        <div style="margin-top: 8px; font-size: 0.85rem;">
                            <span style="background:#EFF6FF; color:#1D4ED8; padding:3px 8px; border-radius:4px; font-weight:600; border:1px solid #BFDBFE;">Lift: {lift_val:.2f}x</span>
                            <span style="background:#FEFCE8; color:#854D0E; padding:3px 8px; border-radius:4px; font-weight:600; margin-left:6px; border:1px solid #FEF08A;">Confidence: {conf_val:.1f}%</span>
                            <span style="background:#F1F5F9; color:#334155; padding:3px 8px; border-radius:4px; margin-left:6px; border:1px solid #CBD5E1;">Support: {supp_val:.2f}%</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.warning("Chưa có luật mua kèm trực tiếp cho tổ hợp sản phẩm này. Hãy thử chọn các sản phẩm phổ biến như Banana, Bag of Organic Bananas, Organic Strawberries, Yellow Bell Pepper...")

# -------------------------------------------------------------
# PHÂN HỆ 2: DASHBOARD PHÂN TÍCH HÀNH VI MUA SẮM
# -------------------------------------------------------------
elif app_mode == "Dashboard Phân Tích Hành Vi":
    st.markdown('<div class="main-header">DASHBOARD PHÂN TÍCH HÀNH VI MUA SẮM</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Phân tích tần suất giao dịch và khung giờ đặt hàng trên 124.000+ đơn hàng</div>', unsafe_allow_html=True)
    
    # Load metrics from DB or cache
    cache = load_dashboard_cache()
    total_orders = cache.get("total_orders", 124364)
    total_products = cache.get("total_products", 49688)
    total_rules = cache.get("total_rules", 302)
    
    # Metrics Row
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tổng số đơn hàng", f"{total_orders:,}")
    c2.metric("Tổng sản phẩm", f"{total_products:,}")
    c3.metric("Luật kết hợp khai phá", f"{total_rules:,}")
    c4.metric("Công nghệ", "MongoDB + PySpark")
    
    st.markdown("---")
    
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.subheader("Top 10 Sản phẩm bán chạy nhất")
        top_data = cache.get("top_products", [])[:10]
        if top_data:
            df_top = pd.DataFrame(top_data).rename(columns={"_id": "Sản phẩm", "total_sales": "Số lượt mua"})
            fig_bar = px.bar(df_top, x="Số lượt mua", y="Sản phẩm", orientation="h", color="Số lượt mua",
                             color_continuous_scale="Blues", title="Top 10 mặt hàng xuất hiện nhiều nhất trong giỏ hàng")
            fig_bar.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_bar, use_container_width=True)
            
    with col_right:
        st.subheader("Số lượng đơn hàng theo khung giờ trong ngày")
        hour_data = cache.get("hourly_data", [])
        if hour_data:
            df_hour = pd.DataFrame(hour_data).rename(columns={"_id": "Giờ trong ngày", "orders_count": "Số đơn hàng"})
            fig_line = px.line(df_hour, x="Giờ trong ngày", y="Số đơn hàng", markers=True,
                               title="Khung giờ mua sắm cao điểm (0h - 23h)")
            fig_line.update_traces(line_color="#2563EB", line_width=3)
            st.plotly_chart(fig_line, use_container_width=True)
            
    # Heatmap of Day of week vs Hour of day
    st.subheader("Bản đồ nhiệt (Heatmap): Mật độ mua sắm theo Ngày và Giờ")
    heat_data = cache.get("heat_data", [])
    if heat_data:
        rows = []
        dow_names = {0: "Chủ Nhật", 1: "Thứ Hai", 2: "Thứ Ba", 3: "Thứ Tư", 4: "Thứ Năm", 5: "Thứ Sáu", 6: "Thứ Bảy"}
        for r in heat_data:
            rows.append({
                "Ngày": dow_names.get(r["_id"]["dow"], f"Ngày {r['_id']['dow']}"),
                "Giờ": r["_id"]["hour"],
                "Đơn hàng": r["count"]
            })
        df_heat = pd.DataFrame(rows)
        pivot_heat = df_heat.pivot(index="Ngày", columns="Giờ", values="Đơn hàng").fillna(0)
        ordered_days = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]
        pivot_heat = pivot_heat.reindex([d for d in ordered_days if d in pivot_heat.index])
        
        fig_heat = px.imshow(pivot_heat, labels=dict(x="Giờ trong ngày (0 - 23h)", y="Ngày trong tuần", color="Số đơn"),
                             color_continuous_scale="Viridis", aspect="auto")
        st.plotly_chart(fig_heat, use_container_width=True)

# -------------------------------------------------------------
# PHÂN HỆ 3: KHÁM PHÁ LUẬT KẾT HỢP (RULES EXPLORER)
# -------------------------------------------------------------
elif app_mode == "Danh Sách Luật Kết Hợp":
    st.markdown('<div class="main-header">DANH SÁCH CÁC LUẬT KẾT HỢP (ASSOCIATION RULES)</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Toàn bộ các quy luật mua sắm được mô hình Spark FP-Growth khai phá và lưu trữ trên MongoDB</div>', unsafe_allow_html=True)
    
    all_rules = load_all_rules()
    total_rules = len(all_rules)
    
    col_f1, col_f2 = st.columns([1, 2])
    with col_f1:
        min_lift_filter = st.slider("Lọc theo độ nâng tối thiểu (Min Lift):", min_value=1.0, max_value=10.0, value=1.5, step=0.1)
    with col_f2:
        search_kw = st.text_input("Tìm kiếm theo tên sản phẩm:", placeholder="Nhập tên sản phẩm ví dụ: Pepper, Garlic, Milk...")
        
    filtered_rules = []
    kw_lower = search_kw.lower().strip() if search_kw else ""
    for r in all_rules:
        if r.get("lift", 0) >= min_lift_filter:
            if kw_lower:
                ant_text = " ".join(r.get("antecedent", [])).lower()
                cons_text = " ".join(r.get("consequent", [])).lower()
                if kw_lower in ant_text or kw_lower in cons_text:
                    filtered_rules.append(r)
            else:
                filtered_rules.append(r)
                
    st.info(f"Hiển thị {len(filtered_rules)} luật kết hợp thỏa mãn điều kiện (Tổng số luật trong hệ thống: {total_rules:,}):")
    
    if filtered_rules:
        table_rows = []
        for r in filtered_rules:
            table_rows.append({
                "Sản phẩm nguồn (Antecedent)": ", ".join(r["antecedent"]),
                "Sản phẩm mua kèm (Consequent)": ", ".join(r["consequent"]),
                "Độ nâng (Lift)": round(r["lift"], 2),
                "Độ tin cậy (Confidence)": f"{round(r['confidence'] * 100, 1)}%",
                "Độ hỗ trợ (Support)": f"{round(r['support'] * 100, 2)}%"
            })
        st.dataframe(pd.DataFrame(table_rows), use_container_width=True)
    else:
        st.warning("Không có luật nào phù hợp với bộ lọc hiện tại.")
