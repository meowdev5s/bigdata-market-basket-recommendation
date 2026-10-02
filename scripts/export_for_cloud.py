import os
import json
from pymongo import MongoClient

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXPORT_DIR = os.path.join(BASE_DIR, "app", "data")
os.makedirs(EXPORT_DIR, exist_ok=True)

client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
db = client["instacart_bigdata"]

# 1. Export recommendation rules
rules = list(db.recommendation_rules.find({}, {"_id": 0}))
with open(os.path.join(EXPORT_DIR, "recommendation_rules.json"), "w", encoding="utf-8") as f:
    json.dump(rules, f, ensure_ascii=False, indent=2)
print(f"Exported {len(rules)} recommendation rules to app/data/")

# 2. Export dashboard aggregation metrics
top_products = list(db.orders.aggregate([
    {"$unwind": "$items"},
    {"$group": {"_id": "$items", "total_sales": {"$sum": 1}}},
    {"$sort": {"total_sales": -1}},
    {"$limit": 15}
]))

hourly_data = list(db.orders.aggregate([
    {"$group": {"_id": "$order_hour_of_day", "orders_count": {"$sum": 1}}},
    {"$sort": {"_id": 1}}
]))

heat_data = list(db.orders.aggregate([
    {"$group": {"_id": {"dow": "$order_dow", "hour": "$order_hour_of_day"}, "count": {"$sum": 1}}}
]))

summary = {
    "total_orders": db.orders.count_documents({}),
    "total_products": db.products.count_documents({}),
    "total_rules": len(rules),
    "top_products": top_products,
    "hourly_data": hourly_data,
    "heat_data": heat_data
}

with open(os.path.join(EXPORT_DIR, "dashboard_cache.json"), "w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)
print("Exported dashboard cache to app/data/dashboard_cache.json successfully!")
