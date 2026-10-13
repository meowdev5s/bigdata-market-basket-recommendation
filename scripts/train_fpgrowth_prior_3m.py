import os
import sys
import json
import time
from pymongo import MongoClient

# Configure PySpark environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "InstacartMarketBasketAnalysis")
EXPORT_DIR = os.path.join(BASE_DIR, "app", "data")
os.makedirs(EXPORT_DIR, exist_ok=True)

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "instacart_bigdata"

def run_fpgrowth_3m_pipeline(min_item_freq=15000, min_confidence=0.08):
    print("=" * 75)
    print("   BIG DATA SCALE: APACHE SPARK FP-GROWTH ON 3.2 MILLION ORDERS (PRIOR)   ")
    print("=" * 75)
    
    start_total = time.time()
    
    # 1. Initialize Apache Spark Session with G1GC & Memory tuning
    print("\n[*] [Step 1/6] Initializing Apache Spark Distributed Engine...")
    t0 = time.time()
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, collect_list, size, broadcast
    from pyspark.ml.fpm import FPGrowth
    
    spark = SparkSession.builder \
        .appName("Instacart_FP_Growth_3M_Prior") \
        .master("local[4]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    print(f"    Spark Session initialized successfully in {time.time() - t0:.2f}s")
    
    # 2. Read CSVs with Spark
    print("\n[*] [Step 2/6] Reading 32.4 Million transactions and order metadata...")
    t0 = time.time()
    products_csv = os.path.join(DATA_DIR, "products.csv")
    prior_csv = os.path.join(DATA_DIR, "order_products__prior.csv")
    orders_csv = os.path.join(DATA_DIR, "orders.csv")
    
    df_products = spark.read.csv(products_csv, header=True, inferSchema=True) \
        .select("product_id", "product_name")
        
    df_prior = spark.read.csv(prior_csv, header=True, inferSchema=True) \
        .select("order_id", "product_id")
        
    df_orders = spark.read.csv(orders_csv, header=True, inferSchema=True) \
        .filter(col("eval_set") == "prior") \
        .select("order_id", "order_dow", "order_hour_of_day")
        
    print(f"    Loaded raw CSVs in {time.time() - t0:.2f}s")
    
    # 3. Compute Dashboard Temporal Metrics for 3.2M Orders
    print("\n[*] [Step 3/6] Computing Big Data Metrics (Hourly & Day-of-Week distribution)...")
    t_dash = time.time()
    hourly_counts = df_orders.groupBy("order_hour_of_day") \
        .count() \
        .sort("order_hour_of_day") \
        .collect()
    hourly_data = [{"_id": int(r["order_hour_of_day"]), "orders_count": int(r["count"])} for r in hourly_counts]
    
    heat_counts = df_orders.groupBy("order_dow", "order_hour_of_day") \
        .count() \
        .collect()
    heat_data = [{"_id": {"dow": int(r["order_dow"]), "hour": int(r["order_hour_of_day"])}, "count": int(r["count"])} for r in heat_counts]
    print(f"    Aggregated 3.2M orders temporal distributions in {time.time() - t_dash:.2f}s")
    
    # 4. Filter Frequent 1-itemsets according to Apriori Theorem
    print(f"\n[*] [Step 4/6] Filtering Frequent Candidates (items appearing >= {min_item_freq:,} times)...")
    t0 = time.time()
    item_counts = df_prior.groupBy("product_id") \
        .count() \
        .filter(col("count") >= min_item_freq)
    
    freq_items_count = item_counts.count()
    print(f"    Discovered {freq_items_count} frequent core products in {time.time() - t0:.2f}s")
    
    # Top products summary extracted directly from item_counts
    top_products_rows = item_counts.join(broadcast(df_products), on="product_id") \
                                  .sort("count", ascending=False) \
                                  .limit(15) \
                                  .collect()
    top_products_3m = [{"_id": r["product_name"], "total_sales": int(r["count"])} for r in top_products_rows]
    
    # Broadcast join candidate items and assemble baskets
    print("    Assembling candidate transactions into Baskets (Group By order_id)...")
    t0 = time.time()
    df_joined = df_prior.join(broadcast(item_counts), on="product_id", how="inner") \
                        .join(broadcast(df_products), on="product_id", how="inner")
                        
    df_baskets = df_joined.groupBy("order_id") \
        .agg(collect_list("product_name").alias("items")) \
        .filter(size(col("items")) >= 2)
        
    total_baskets = df_baskets.count()
    print(f"    Assembled {total_baskets:,} candidate baskets (>=2 items) in {time.time() - t0:.2f}s")
    print("    Sample 5 Baskets:")
    df_baskets.show(5, truncate=80)
    
    # 5. Train FP-Growth Model on the 3.2M dataset
    min_support = min_item_freq / float(total_baskets)
    print(f"\n[*] [Step 5/6] Training Distributed FP-Growth on {total_baskets:,} Baskets...")
    print(f"    Parameters: minSupport = {min_support:.5f} ({min_support*100:.3f}%), minConfidence = {min_confidence:.2f} ({min_confidence*100:.1f}%)")
    t_train = time.time()
    
    fp_growth = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence)
    model = fp_growth.fit(df_baskets)
    training_time = time.time() - t_train
    print(f"    [+] Model training completed in {training_time:.2f} seconds!")
    
    # Extract Association Rules
    rules = model.associationRules
    strong_rules = rules.filter(col("lift") > 1.2).sort("lift", ascending=False)
    strong_count = strong_rules.count()
    print(f"\n    [+] Strong Rules with Lift > 1.2: {strong_count:,}")
    print("\n    Top 15 Association Rules with Highest Lift:")
    strong_rules.show(15, truncate=False)
    
    # 6. Export Rules & Indexes to MongoDB & Local Cache
    print(f"\n[*] [Step 6/6] Indexing and Exporting to MongoDB and Web App cache...")
    t0 = time.time()
    
    # Collect rules to Python dicts
    rules_list = strong_rules.collect()
    rules_docs = []
    for r in rules_list:
        rules_docs.append({
            "antecedent": list(r["antecedent"]),
            "consequent": list(r["consequent"]),
            "confidence": float(r["confidence"]),
            "lift": float(r["lift"]),
            "support": float(r["support"])
        })
        
    # Save to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    # 6.1 Save and index recommendation_rules_prior
    col_rules_prior = db["recommendation_rules_prior"]
    col_rules_prior.drop()
    if rules_docs:
        col_rules_prior.insert_many(rules_docs)
        print(f"    [Index 1/2] Creating indexes on 'recommendation_rules_prior'...")
        col_rules_prior.create_index("antecedent")
        col_rules_prior.create_index([("lift", -1)])
        col_rules_prior.create_index([("confidence", -1)])
        print(f"    [+] Saved and indexed {len(rules_docs):,} rules in MongoDB 'recommendation_rules_prior'!")
        
    # 6.2 Save and index orders_prior
    print(f"    [Index 2/2] Importing 200,000 orders into MongoDB 'orders_prior' with indexes...")
    col_orders_prior = db["orders_prior"]
    col_orders_prior.drop()
    sample_orders = df_orders.limit(200000).collect()
    docs_orders = [{"order_id": int(r["order_id"]), "order_dow": int(r["order_dow"]), "order_hour_of_day": int(r["order_hour_of_day"])} for r in sample_orders]
    col_orders_prior.insert_many(docs_orders)
    col_orders_prior.create_index("order_id")
    col_orders_prior.create_index([("order_dow", 1), ("order_hour_of_day", 1)])
    print(f"    [+] Saved and indexed {len(docs_orders):,} orders in MongoDB 'orders_prior'!")
    
    # 6.3 Local file for cloud fallback
    out_rules_path = os.path.join(EXPORT_DIR, "recommendation_rules_prior.json")
    with open(out_rules_path, "w", encoding="utf-8") as f:
        json.dump(rules_docs, f, ensure_ascii=False, indent=2, default=str)
    print(f"    [+] Saved rules file for cloud: {out_rules_path}")
    
    cache_3m = {
        "dataset_name": "Tập Big Data Đầy Đủ (3.2 Triệu Đơn Hàng)",
        "total_orders": 3214874,
        "total_valid_baskets": total_baskets,
        "total_transactions": 32434489,
        "total_rules": len(rules_docs),
        "min_support_percent": round(min_support * 100, 3),
        "min_confidence_percent": round(min_confidence * 100, 1),
        "training_time_seconds": round(training_time, 2),
        "top_products": top_products_3m,
        "hourly_data": hourly_data,
        "heat_data": heat_data
    }
    
    out_cache_path = os.path.join(EXPORT_DIR, "dashboard_cache_prior.json")
    with open(out_cache_path, "w", encoding="utf-8") as f:
        json.dump(cache_3m, f, ensure_ascii=False, indent=2)
    print(f"    [+] Saved dashboard summary: {out_cache_path}")
    
    col_dash = db["dashboard_cache_prior"]
    col_dash.drop()
    col_dash.insert_one(cache_3m)
    print(f"    [+] Saved dashboard summary into MongoDB 'dashboard_cache_prior'")
    
    spark.stop()
    print(f"\n=============================================================")
    print(f" PIPELINE 3.2M FINISHED SUCCESSFULLY IN {time.time() - start_total:.2f}s!")
    print(f" Database: '{DB_NAME}'")
    print(f" Collections Indexed:")
    print(f"   1. 'recommendation_rules_prior' (antecedent, lift, confidence)")
    print(f"   2. 'orders_prior' (order_id, compound [order_dow, order_hour_of_day])")
    print(f" Cloud Cache Files Generated:")
    print(f"   - {out_rules_path}")
    print(f"   - {out_cache_path}")
    print(f"=============================================================")

if __name__ == "__main__":
    run_fpgrowth_3m_pipeline(min_item_freq=15000, min_confidence=0.08)
