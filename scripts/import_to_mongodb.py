import os
import time
import json
import pandas as pd
from pymongo import MongoClient

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "InstacartMarketBasketAnalysis")

# MongoDB connection
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "instacart_bigdata"

def get_mongo_db():
    print(f"[*] Connecting to MongoDB at {MONGO_URI}...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Check connection
    client.server_info()
    print("[+] Successfully connected to MongoDB Server!")
    return client[DB_NAME]

def import_products(db):
    print("\n--- [1/2] Importing Products collection ---")
    products_file = os.path.join(DATA_DIR, "products.csv")
    aisles_file = os.path.join(DATA_DIR, "aisles.csv")
    departments_file = os.path.join(DATA_DIR, "departments.csv")
    
    print("Reading products.csv, aisles.csv, departments.csv...")
    df_products = pd.read_csv(products_file)
    df_aisles = pd.read_csv(aisles_file)
    df_departments = pd.read_csv(departments_file)
    
    # Merge to get human-readable aisle and department names
    df_merged = df_products.merge(df_aisles, on="aisle_id", how="left")
    df_merged = df_merged.merge(df_departments, on="department_id", how="left")
    
    col_products = db["products"]
    col_products.drop()  # reset collection
    
    records = []
    for _, row in df_merged.iterrows():
        records.append({
            "_id": int(row["product_id"]),
            "product_id": int(row["product_id"]),
            "product_name": str(row["product_name"]),
            "aisle": str(row["aisle"]),
            "department": str(row["department"])
        })
    
    print(f"Inserting {len(records)} products into MongoDB...")
    col_products.insert_many(records)
    col_products.create_index("product_name")
    print(f"[+] Successfully inserted {col_products.count_documents({})} products!")

def import_orders(db, max_orders=None):
    print(f"\n--- [2/2] Importing Orders collection (Transactions from Prior Big Data) ---")
    orders_file = os.path.join(DATA_DIR, "orders.csv")
    order_products_file = os.path.join(DATA_DIR, "order_products__prior.csv")
    if not os.path.exists(order_products_file):
        order_products_file = os.path.join(DATA_DIR, "order_products__train.csv")
    products_file = os.path.join(DATA_DIR, "products.csv")
    
    print("Reading products mapping...")
    df_prod = pd.read_csv(products_file, usecols=["product_id", "product_name"])
    prod_dict = dict(zip(df_prod["product_id"], df_prod["product_name"]))
    
    print(f"Reading order_products from {os.path.basename(order_products_file)}...")
    df_order_prod = pd.read_csv(order_products_file)
    print(f"Total order-product rows loaded: {len(df_order_prod):,}")
    
    print(f"Reading orders metadata from {os.path.basename(orders_file)}...")
    df_orders = pd.read_csv(orders_file)
    # Filter orders that exist in order_products
    valid_order_ids = set(df_order_prod["order_id"].unique())
    df_orders = df_orders[df_orders["order_id"].isin(valid_order_ids)]
    if max_orders and len(df_orders) > max_orders:
        df_orders = df_orders.head(max_orders)
        valid_order_ids = set(df_orders["order_id"].unique())
        df_order_prod = df_order_prod[df_order_prod["order_id"].isin(valid_order_ids)]
    
    orders_meta = {}
    for _, row in df_orders.iterrows():
        orders_meta[int(row["order_id"])] = {
            "user_id": int(row["user_id"]),
            "order_dow": int(row["order_dow"]),
            "order_hour_of_day": int(row["order_hour_of_day"])
        }
    
    print("Grouping items into baskets (Document format with Set deduplication)...")
    # Group items by order_id using Set to ensure uniqueness
    baskets = {}
    for _, row in df_order_prod.iterrows():
        oid = int(row["order_id"])
        pid = int(row["product_id"])
        pname = prod_dict.get(pid, f"Product_{pid}")
        if oid not in baskets:
            baskets[oid] = set()
        baskets[oid].add(pname)
    
    print(f"Total unique orders (baskets) assembled: {len(baskets):,}")
    
    col_orders = db["orders_prior"]
    col_orders.drop()  # reset collection
    
    batch = []
    batch_size = 10000
    inserted_count = 0
    start_time = time.time()
    
    for oid, items_set in baskets.items():
        # Only take baskets with >= 2 items (for basket analysis)
        if len(items_set) < 2:
            continue
        items = list(items_set)
            
        meta = orders_meta.get(oid, {})
        doc = {
            "_id": oid,
            "order_id": oid,
            "user_id": meta.get("user_id"),
            "order_dow": meta.get("order_dow"),
            "order_hour_of_day": meta.get("order_hour_of_day"),
            "items_count": len(items),
            "items": items
        }
        batch.append(doc)
        
        if len(batch) >= batch_size:
            col_orders.insert_many(batch, ordered=False)
            inserted_count += len(batch)
            batch = []
            print(f"  --> Inserted {inserted_count:,} orders... ({time.time() - start_time:.1f}s)")
            
    if batch:
        col_orders.insert_many(batch, ordered=False)
        inserted_count += len(batch)
    
    print(f"Creating indexes on order_id, compound (order_dow, order_hour_of_day), and items...")
    col_orders.create_index("order_id")
    col_orders.create_index([("order_dow", 1), ("order_hour_of_day", 1)])
    col_orders.create_index("items")
    
    print(f"[+] Finished! Total orders in MongoDB 'orders_prior': {col_orders.count_documents({}):,}")
    print(f"[+] Total elapsed time: {time.time() - start_time:.2f} seconds.")

def import_rules_and_itemsets(db):
    print("\n--- [3/3] Importing Association Rules, Frequent Itemsets & Dashboard Cache ---")
    export_dir = os.path.join(BASE_DIR, "app", "data")
    
    # 1. Rules
    rules_file = os.path.join(export_dir, "recommendation_rules_prior.json")
    if not os.path.exists(rules_file):
        rules_file = os.path.join(export_dir, "recommendation_rules.json")
    if os.path.exists(rules_file):
        with open(rules_file, "r", encoding="utf-8") as f:
            rules_docs = json.load(f)
        col_rules = db["recommendation_rules_prior"]
        col_rules.drop()
        if rules_docs:
            col_rules.insert_many(rules_docs)
            col_rules.create_index("antecedent")
            col_rules.create_index([("lift", -1)])
            col_rules.create_index([("confidence", -1)])
            print(f"[+] Imported and indexed {len(rules_docs)} rules into 'recommendation_rules_prior'!")
            
    # 2. Frequent Itemsets
    itemsets_file = os.path.join(export_dir, "frequent_itemsets.json")
    if os.path.exists(itemsets_file):
        with open(itemsets_file, "r", encoding="utf-8") as f:
            it_data = json.load(f)
        all_it = []
        all_it.extend(it_data.get("itemsets_1", []))
        all_it.extend(it_data.get("itemsets_multi", []))
        col_it = db["frequent_itemsets"]
        col_it.drop()
        if all_it:
            col_it.insert_many(all_it)
            col_it.create_index("k")
            col_it.create_index([("support", -1)])
            col_it.create_index("items")
            print(f"[+] Imported and indexed {len(all_it)} frequent itemsets into 'frequent_itemsets'!")
            
    # 3. Dashboard Cache
    cache_file = os.path.join(export_dir, "dashboard_cache_prior.json")
    if not os.path.exists(cache_file):
        cache_file = os.path.join(export_dir, "dashboard_cache.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            cache_doc = json.load(f)
        col_cache = db["dashboard_cache_prior"]
        col_cache.drop()
        col_cache.insert_one(cache_doc)
        print("[+] Imported dashboard cache into 'dashboard_cache_prior'!")
        
    # 4. Data Quality Reports
    dq_file = os.path.join(export_dir, "data_quality_reports.json")
    if os.path.exists(dq_file):
        with open(dq_file, "r", encoding="utf-8") as f:
            dq_doc = json.load(f)
        col_dq = db["data_quality_reports"]
        col_dq.drop()
        col_dq.insert_one(dq_doc)
        print("[+] Imported data quality reports into 'data_quality_reports'!")

if __name__ == "__main__":
    try:
        db = get_mongo_db()
        import_products(db)
        import_orders(db)
        import_rules_and_itemsets(db)
        print("\n==========================================")
        print("ALL DATA IMPORTED SUCCESSFULLY TO MONGODB!")
        print("==========================================")
    except Exception as e:
        print(f"[!] Error occurred: {e}")
