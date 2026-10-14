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

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "instacart_bigdata"

def run_spark_with_ui():
    print("=" * 70)
    print("  CHUONG TRINH GIAM SAT HE THONG PHAN TAN VOI SPARK WEB UI")
    print("=" * 70)
    
    # Connect MongoDB
    print("\n[1/4] Ket noi MongoDB de doc danh sach gio hang...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col_orders = db["orders"]
    total = col_orders.count_documents({})
    print(f"      Tong so don hang trong MongoDB: {total:,} gio hang")
    
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
    print("\n[2/4] Khoi tao Apache Spark Engine (Phan tan da tien trinh)...")
    from pyspark.sql import SparkSession
    from pyspark.ml.fpm import FPGrowth
    
    spark = SparkSession.builder \
        .appName("Instacart_FP_Growth_3M_Prior") \
        .master("local[*]") \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "4040") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    ui_url = spark.sparkContext.uiWebUrl
    print(f"\n" + "=" * 70)
    print(f"  >>> SPARK WEB UI DA KHOI DONG THANH CONG!")
    print(f"  >>> TRUY CAP NGAY TAI TRINH DUYET: {ui_url}")
    print("=" * 70)

    # Load data into DataFrame
    print("\n[3/4] Nap du lieu vao Spark DataFrame phan tan...")
    df_spark = spark.read.json(TEMP_JSONL)
    print(f"      So luong record: {df_spark.count():,} | So phan vung (Partitions): {df_spark.rdd.getNumPartitions()}")

    # Train FP-Growth
    print("\n[4/4] Bat dau thuc thi thuan toan FP-Growth...")
    print("      (Hay quan sat giao dien Web UI tren trinh duyet de thay cac Stages chay!)")
    t0 = time.time()
    fp_growth = FPGrowth(itemsCol="items", minSupport=0.003, minConfidence=0.15)
    model = fp_growth.fit(df_spark)
    print(f"      [+] Thoi gian huan luyen: {time.time() - t0:.2f} giay!")
    
    freq_count = model.freqItemsets.count()
    rules_count = model.associationRules.count()
    print(f"      [+] So tap pho bien: {freq_count:,}")
    print(f"      [+] So luat ket hop: {rules_count:,}")

    # KEEP SPARK ALIVE FOR USER INSPECTION
    print("\n" + "=" * 70)
    print("  HE THONG DANG GIU PHIEN LAM VIEC DE ANH THEO DOI SPARK WEB UI!")
    print(f"  Link truy cap: {ui_url}")
    print("  Anh co the xem:")
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
