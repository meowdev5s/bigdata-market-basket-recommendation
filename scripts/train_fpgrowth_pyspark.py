import os
import sys

# Redirect to the official Big Data PySpark pipeline (3.2M Orders / 32.4M Transactions)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from train_fpgrowth_prior_3m import run_fpgrowth_3m_pipeline

if __name__ == "__main__":
    print("[*] Launching Official Big Data Apache Spark FP-Growth Pipeline...")
    run_fpgrowth_3m_pipeline(min_item_freq=15000, min_confidence=0.08)
