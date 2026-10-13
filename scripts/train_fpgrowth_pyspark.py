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

def run_fpgrowth_sample_pipeline(min_support=0.005, min_confidence=0.15):
    print("=" * 75)
    print("   SAMPLE DATASET PIPELINE: APACHE SPARK DATA CLEANING & FP-GROWTH (TRAIN)   ")
    print("=" * 75)
    
    start_total = time.time()
    
    # 1. Initialize Apache Spark Distributed Engine
    print("\n[*] [Step 1/6] Initializing Apache Spark Engine...")
    t0 = time.time()
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, collect_set, size, broadcast
    from pyspark.ml.fpm import FPGrowth
    
    spark = SparkSession.builder \
        .appName("Instacart_FP_Growth_Sample_Train") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    print(f"    Spark Session initialized successfully in {time.time() - t0:.2f}s")
    
    # 2. Read raw CSV files directly with Spark
    print("\n[*] [Step 2/6] Reading raw CSVs directly via Apache Spark...")
    t0 = time.time()
    products_csv = os.path.join(DATA_DIR, "products.csv")
    train_csv = os.path.join(DATA_DIR, "order_products__train.csv")
    orders_csv = os.path.join(DATA_DIR, "orders.csv")
    
    df_products = spark.read.csv(products_csv, header=True, inferSchema=True) \
        .select("product_id", "product_name")
        
    df_train_raw = spark.read.csv(train_csv, header=True, inferSchema=True) \
        .select("order_id", "product_id")
        
    df_orders_raw = spark.read.csv(orders_csv, header=True, inferSchema=True) \
        .select("order_id", "eval_set", "order_dow", "order_hour_of_day")
        
    raw_tx_count = df_train_raw.count()
    raw_orders_count = df_orders_raw.count()
    print(f"    Loaded raw files in {time.time() - t0:.2f}s")
    print(f"    Raw transactions: {raw_tx_count:,} rows | Raw orders metadata: {raw_orders_count:,} rows")
    
    # 3. Data Cleaning & Preprocessing with Spark (Làm sạch và Tiền xử lý dữ liệu)
    print("\n[*] [Step 3/6] Performing Distributed Data Cleaning with Spark...")
    t_clean = time.time()
    
    # Clean 1: Filter eval_set == 'train' to eliminate out-of-scope metadata
    print("    [Cleaning 1/4] Filtering dataset scope: eval_set == 'train'...")
    df_orders_clean = df_orders_raw.filter(col("eval_set") == "train") \
                                   .select("order_id", "order_dow", "order_hour_of_day")
    
    # Clean 2: Referential Integrity Check - drop records with null / missing product_id
    print("    [Cleaning 2/4] Referential Integrity: Inner join with products to drop invalid product_id...")
    df_joined = df_train_raw.join(broadcast(df_products), on="product_id", how="inner")
    
    # Clean 3: Deduplication & Grouping - collect_set ensures unique items per basket
    print("    [Cleaning 3/4] Deduplication & Grouping: Assembling distinct items into Baskets...")
    df_baskets_all = df_joined.groupBy("order_id") \
                              .agg(collect_set("product_name").alias("items"))
                              
    total_raw_baskets = df_baskets_all.count()
    
    # Clean 4: Filter invalid baskets - drop empty or single-item orders (< 2 items)
    print("    [Cleaning 4/4] Basket Filtering: Dropping single-item orders (< 2 items)...")
    df_baskets = df_baskets_all.filter(size(col("items")) >= 2)
    valid_baskets = df_baskets.count()
    dropped_single_orders = total_raw_baskets - valid_baskets
    
    print(f"    Data Cleaning completed in {time.time() - t_clean:.2f}s:")
    print(f"      - Total Baskets assembled: {total_raw_baskets:,}")
    print(f"      - Single-item orders dropped: {dropped_single_orders:,}")
    print(f"      - Valid Multi-item Baskets (>=2 items): {valid_baskets:,}")
    print("    Sample 5 Cleaned Baskets:")
    df_baskets.show(5, truncate=80)
    
    # Compute Dashboard Temporal Metrics on Cleaned Orders
    print("\n[*] [Step 4/6] Computing Dashboard Metrics with Spark SQL...")
    t_dash = time.time()
    hourly_counts = df_orders_clean.groupBy("order_hour_of_day") \
        .count() \
        .sort("order_hour_of_day") \
        .collect()
    hourly_data = [{"_id": int(r["order_hour_of_day"]), "orders_count": int(r["count"])} for r in hourly_counts]
    
    heat_counts = df_orders_clean.groupBy("order_dow", "order_hour_of_day") \
        .count() \
        .collect()
    heat_data = [{"_id": {"dow": int(r["order_dow"]), "hour": int(r["order_hour_of_day"])}, "count": int(r["count"])} for r in heat_counts]
    
    top_products_rows = df_joined.groupBy("product_name") \
        .count() \
        .sort("count", ascending=False) \
        .limit(15) \
        .collect()
    top_products = [{"_id": r["product_name"], "total_sales": int(r["count"])} for r in top_products_rows]
    print(f"    Aggregated dashboard temporal metrics in {time.time() - t_dash:.2f}s")
    
    # 5. Distributed FP-Growth Model Training with Spark MLlib
    print(f"\n[*] [Step 5/6] Training Distributed FP-Growth on {valid_baskets:,} Cleaned Baskets...")
    print(f"    Parameters: minSupport = {min_support} ({min_support*100:.2f}%), minConfidence = {min_confidence} ({min_confidence*100:.1f}%)")
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
    
    col_rules = db["recommendation_rules"]
    col_rules.drop()
    if rules_docs:
        col_rules.insert_many(rules_docs)
        print(f"    Creating Index on 'antecedent', 'lift', and 'confidence'...")
        col_rules.create_index("antecedent")
        col_rules.create_index([("lift", -1)])
        col_rules.create_index([("confidence", -1)])
        print(f"    [+] Saved and indexed {len(rules_docs):,} rules in MongoDB 'recommendation_rules'!")
        
    # Save local file for cloud fallback
    out_rules_path = os.path.join(EXPORT_DIR, "recommendation_rules.json")
    with open(out_rules_path, "w", encoding="utf-8") as f:
        json.dump(rules_docs, f, ensure_ascii=False, indent=2, default=str)
    print(f"    [+] Saved rules file for cloud: {out_rules_path}")
    
    cache_sample = {
        "dataset_name": "Tập Dữ Liệu Mẫu (124.000+ Đơn Hàng)",
        "total_orders": valid_baskets,
        "total_raw_orders": total_raw_baskets,
        "total_transactions": raw_tx_count,
        "total_rules": len(rules_docs),
        "min_support_percent": round(min_support * 100, 2),
        "min_confidence_percent": round(min_confidence * 100, 1),
        "training_time_seconds": round(training_time, 2),
        "top_products": top_products,
        "hourly_data": hourly_data,
        "heat_data": heat_data
    }
    
    out_cache_path = os.path.join(EXPORT_DIR, "dashboard_cache.json")
    with open(out_cache_path, "w", encoding="utf-8") as f:
        json.dump(cache_sample, f, ensure_ascii=False, indent=2)
    print(f"    [+] Saved dashboard summary: {out_cache_path}")
    
    col_dash = db["dashboard_cache"]
    col_dash.drop()
    col_dash.insert_one(cache_sample)
    print(f"    [+] Saved dashboard summary to MongoDB 'dashboard_cache'")
    
    spark.stop()
    print(f"\n=============================================================")
    print(f" SAMPLE PIPELINE FINISHED SUCCESSFULLY IN {time.time() - start_total:.2f}s!")
    print(f" Database: '{DB_NAME}'")
    print(f" Collections Indexed: 'recommendation_rules' (antecedent, lift, confidence)")
    print(f" Cloud Cache Files Generated:")
    print(f"   - {out_rules_path}")
    print(f"   - {out_cache_path}")
    print(f"=============================================================")

if __name__ == "__main__":
    run_fpgrowth_sample_pipeline(min_support=0.005, min_confidence=0.15)
