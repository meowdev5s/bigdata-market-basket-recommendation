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
DATA_DIR = os.path.join(BASE_DIR, "data")
TEMP_JSONL = os.path.join(DATA_DIR, "baskets_temp.jsonl")

# MongoDB connection
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "instacart_bigdata"

def run_fpgrowth_pipeline(min_support=0.003, min_confidence=0.15):
    print("=============================================================")
    print("   BIG DATA PIPELINE: MONGODB -> PYSPARK FP-GROWTH MODEL     ")
    print("=============================================================")
    
    start_total = time.time()
    
    # 1. Connect to MongoDB and fetch transactions
    print(f"\n[*] [Step 1/5] Extracting orders from MongoDB '{DB_NAME}.orders'...")
    t0 = time.time()
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col_orders = db["orders"]
    
    total_docs = col_orders.count_documents({})
    print(f"    Total orders in MongoDB: {total_docs:,}")
    
    # Export to json lines for high-speed Spark native loading
    print(f"    Streaming orders to native Spark format...")
    cursor = col_orders.find({}, {"_id": 0, "order_id": 1, "items": 1})
    count = 0
    with open(TEMP_JSONL, "w", encoding="utf-8") as f:
        for doc in cursor:
            f.write(json.dumps(doc) + "\n")
            count += 1
            
    print(f"    Exported {count:,} baskets in {time.time() - t0:.2f}s")
    
    # 2. Initialize Apache Spark
    print("\n[*] [Step 2/5] Initializing Apache Spark Session...")
    t0 = time.time()
    from pyspark.sql import SparkSession
    from pyspark.ml.fpm import FPGrowth
    
    spark = SparkSession.builder \
        .appName("Instacart_FP_Growth_Recommendation") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    print(f"    Spark Session initialized successfully in {time.time() - t0:.2f}s")
    
    # 3. Create PySpark DataFrame natively
    print("\n[*] [Step 3/5] Loading baskets into Spark DataFrame natively...")
    t0 = time.time()
    df_spark = spark.read.json(TEMP_JSONL)
    print(f"    Loaded into Spark DataFrame in {time.time() - t0:.2f}s")
    print("    Spark DataFrame Schema:")
    df_spark.printSchema()
    print("    Sample 5 Baskets:")
    df_spark.show(5, truncate=80)
    
    # 4. Train FP-Growth Model
    print(f"\n[*] [Step 4/5] Training FP-Growth Algorithm on Spark...")
    print(f"    Parameters: minSupport = {min_support} ({min_support*100}%), minConfidence = {min_confidence} ({min_confidence*100}%)")
    t0 = time.time()
    
    fp_growth = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence)
    model = fp_growth.fit(df_spark)
    training_time = time.time() - t0
    print(f"    [+] Model training completed in {training_time:.2f} seconds!")
    
    # Display Frequent Itemsets
    freq_itemsets = model.freqItemsets
    freq_count = freq_itemsets.count()
    print(f"\n    [+] Total Frequent Itemsets discovered: {freq_count:,}")
    print("    Top 10 Most Frequent Itemsets:")
    freq_itemsets.sort("freq", ascending=False).show(10, truncate=False)
    
    # Display Association Rules
    rules = model.associationRules
    rules_count = rules.count()
    print(f"\n    [+] Total Association Rules generated: {rules_count:,}")
    
    # Filter rules with lift > 1.2
    strong_rules = rules.filter(rules["lift"] > 1.2).sort("lift", ascending=False)
    strong_count = strong_rules.count()
    print(f"    [+] Strong Rules with Lift > 1.2 (Meaningful Cross-selling): {strong_count:,}")
    print("\n    Top 15 Association Rules with Highest Lift:")
    strong_rules.show(15, truncate=False)
    
    # 5. Export Rules back to MongoDB
    print(f"\n[*] [Step 5/5] Exporting Association Rules to MongoDB collection 'recommendation_rules'...")
    t0 = time.time()
    col_rules = db["recommendation_rules"]
    col_rules.drop()  # reset collection
    
    # Convert PySpark DataFrame to Python dicts for MongoDB insertion
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
        
    if rules_docs:
        col_rules.insert_many(rules_docs)
        print(f"    Creating Index on 'antecedent' for sub-millisecond query latency...")
        col_rules.create_index("antecedent")
        col_rules.create_index([("lift", -1)])
        print(f"    [+] Successfully saved {len(rules_docs):,} rules into MongoDB!")
    else:
        print("    [!] No rules matched the threshold. Try lowering minSupport or minConfidence.")
        
    spark.stop()
    
    # Clean up temp file
    if os.path.exists(TEMP_JSONL):
        try:
            os.remove(TEMP_JSONL)
        except:
            pass
            
    print(f"\n=============================================================")
    print(f" PIPELINE FINISHED SUCCESSFULLY IN {time.time() - start_total:.2f}s!")
    print(f" MongoDB Database: '{DB_NAME}'")
    print(f" Collections ready: 'orders', 'products', 'recommendation_rules'")
    print(f"=============================================================")

if __name__ == "__main__":
    # Support: 0.003 (approx 370 occurrences in 124,364 orders), Confidence: 0.15 (15%)
    run_fpgrowth_pipeline(min_support=0.003, min_confidence=0.15)
