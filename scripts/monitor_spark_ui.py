import os
import sys
import json
import time
from pymongo import MongoClient

# 1. Configure PySpark environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
TEMP_JSONL = os.path.join(DATA_DIR, "baskets_temp.jsonl")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "instacart_bigdata")

def run_spark_with_ui():
    print("=" * 70)
    print("  CHUONG TRINH GIAM SAT HE THONG PHAN TAN VOI SPARK WEB UI")
    print("=" * 70)
    
    # Connect MongoDB
    print("\n[1/4] Ket noi MongoDB de doc danh sach gio hang (orders_prior)...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col_orders = db["orders_prior"]
    total = col_orders.count_documents({})
    print(f"      Tong so don hang hợp lệ trong MongoDB 'orders_prior': {total:,} gio hang")
    
    if not os.path.exists(TEMP_JSONL):
        print("      Dang xuat du lieu sang JSON Lines de Spark doc...")
        cursor = col_orders.find({}, {"_id": 0, "order_id": 1, "items": 1})
        with open(TEMP_JSONL, "w", encoding="utf-8") as f:
            for doc in cursor:
                f.write(json.dumps(doc) + "\n")
        print("      Xuat file thanh cong!")
    else:
        print("      File du lieu tam da san sang!")

    # Start Apache Spark with Web UI explicitly enabled
    print("\n[2/4] Khoi tao Apache Spark Engine (Spark Local Mode - local[*])...")
    from pyspark.sql import SparkSession
    from pyspark.ml.fpm import FPGrowth
    
    spark = SparkSession.builder \
        .appName("Instacart_FP_Growth_3M_Prior") \
        .master("local[*]") \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "4040") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    ui_url = spark.sparkContext.uiWebUrl
    print(f"\n" + "=" * 70)
    print(f"  >>> SPARK WEB UI DA KHOI DONG THANH CONG! (Spark {spark.version})")
    print(f"  >>> TRUY CAP NGAY TAI TRINH DUYET: {ui_url}")
    print("=" * 70)

    # Load data into DataFrame
    print("\n[3/4] Nap du lieu vao Spark DataFrame phan tan...")
    df_spark = spark.read.json(TEMP_JSONL)
    baskets_cnt = df_spark.count()
    print(f"      So luong record: {baskets_cnt:,} | So phan vung (Partitions): {df_spark.rdd.getNumPartitions()}")

    # Train FP-Growth with standard system threshold (15,000 / N, minConfidence = 0.08)
    min_supp = 15000.0 / float(baskets_cnt) if baskets_cnt >= 2000000 else 0.00726
    print(f"\n[4/4] Bat dau thuc thi thuat toan FP-Growth (minSupport={min_supp:.5f}, minConfidence=0.08)...")
    print("      (Hay quan sat giao dien Web UI tren trinh duyet de thay cac Stages chay!)")
    t0 = time.time()
    fp_growth = FPGrowth(itemsCol="items", minSupport=min_supp, minConfidence=0.08)
    model = fp_growth.fit(df_spark)
    print(f"      [+] Thoi gian huan luyen: {time.time() - t0:.2f} giay!")
    
    freq_count = model.freqItemsets.count()
    rules_count = model.associationRules.filter("lift > 1.2").count()
    print(f"      [+] So tap pho bien: {freq_count:,}")
    print(f"      [+] So luat ket hop (Lift > 1.2): {rules_count:,}")

    # KEEP SPARK ALIVE FOR USER INSPECTION
    print("\n" + "=" * 70)
    print("  HE THONG DANG GIU PHIEN LAM VIEC DE THEO DOI SPARK WEB UI!")
    print(f"  Link truy cap: {ui_url}")
    print("  Co the xem:")
    print("    - Tab 'Jobs': Xem toan bo cac cong viec Spark da chay")
    print("    - Tab 'Stages': Xem chi tiet cac phan vung (Partitions), CPU Cores, RAM")
    print("    - Tab 'Executors': Thong tin cac luong tinh toan (Workers)")
    print("    - Muc 'DAG Visualization': So do do thi chay thuat toan FP-Tree")
    print("=" * 70)
    
    try:
        print("\n>>> Spark Web UI dang hoat dong tai http://localhost:4040. Dang giu ket noi...")
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, EOFError):
        pass
        
    print("\nDang dong Spark Session...")
    spark.stop()
    print("Hoan tat!")

if __name__ == "__main__":
    run_spark_with_ui()
