import os
import time
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

def import_orders(db, max_orders=150000):
    print(f"\n--- [2/2] Importing Orders collection (Transactions) ---")
    orders_file = os.path.join(DATA_DIR, "orders.csv")
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
    
    print("Grouping items into baskets (Document format)...")
    # Group items by order_id
    baskets = {}
    for _, row in df_order_prod.iterrows():
        oid = int(row["order_id"])
        pid = int(row["product_id"])
        pname = prod_dict.get(pid, f"Product_{pid}")
        if oid not in baskets:
            baskets[oid] = []
        baskets[oid].append(pname)
    
    print(f"Total unique orders (baskets) assembled: {len(baskets):,}")
    
    col_orders = db["orders"]
    col_orders.drop()  # reset collection
    
    batch = []
    batch_size = 5000
    inserted_count = 0
    start_time = time.time()
    
    for oid, items in baskets.items():
        # Only take baskets with >= 2 items (for basket analysis)
        if len(items) < 2:
            continue
            
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
            col_orders.insert_many(batch)
            inserted_count += len(batch)
            batch = []
            print(f"  --> Inserted {inserted_count:,} orders... ({time.time() - start_time:.1f}s)")
            
    if batch:
        col_orders.insert_many(batch)
        inserted_count += len(batch)
    
    print(f"Creating index on order_id and order_dow...")
    col_orders.create_index("order_id")
    col_orders.create_index("order_dow")
    col_orders.create_index("order_hour_of_day")
    
    print(f"[+] Finished! Total orders in MongoDB: {col_orders.count_documents({}):,}")
    print(f"[+] Total elapsed time: {time.time() - start_time:.2f} seconds.")

if __name__ == "__main__":
    try:
        db = get_mongo_db()
        import_products(db)
        import_orders(db)
        print("\n==========================================")
        print("ALL DATA IMPORTED SUCCESSFULLY TO MONGODB!")
        print("==========================================")
    except Exception as e:
        print(f"[!] Error occurred: {e}")
