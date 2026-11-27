import os
import json
from pymongo import MongoClient

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXPORT_DIR = os.path.join(BASE_DIR, "app", "data")
os.makedirs(EXPORT_DIR, exist_ok=True)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "instacart_bigdata")

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
db = client[DB_NAME]

# 1. Export recommendation rules from official Prior collection (3.2M scale)
rules = list(db["recommendation_rules_prior"].find({}, {"_id": 0}))
for fname in ["recommendation_rules_prior.json", "recommendation_rules.json"]:
    with open(os.path.join(EXPORT_DIR, fname), "w", encoding="utf-8") as f:
        json.dump(rules, f, ensure_ascii=False, indent=2)
print(f"[+] Exported {len(rules)} recommendation rules to app/data/ (prior & default)")

# 2. Export dashboard cache from official Prior collection
cache_doc = db["dashboard_cache_prior"].find_one({}, {"_id": 0})
if cache_doc:
    for fname in ["dashboard_cache_prior.json", "dashboard_cache.json"]:
        with open(os.path.join(EXPORT_DIR, fname), "w", encoding="utf-8") as f:
            json.dump(cache_doc, f, ensure_ascii=False, indent=2)
    print("[+] Exported dashboard cache to app/data/dashboard_cache_prior.json & dashboard_cache.json successfully!")
