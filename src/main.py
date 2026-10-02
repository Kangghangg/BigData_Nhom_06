"""
MODULE MAIN: INTEGRATED PIPELINE EXECUTION
Thành viên đảm nhận: Thành viên 5
Nhiệm vụ:
Tích hợp và chạy liên hoàn toàn bộ luồng từ TV1 -> TV2 -> TV3 -> TV4.
"""
import os
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
from pyspark.sql import SparkSession

def main():
    print("=================================================================")
    print("=== CHƯƠNG TRÌNH CHẠY PIPELINE TỔNG HỢP BIG DATA - NHÓM 06 ===")
    print("=================================================================")
    # Thành viên 5 tích hợp gọi hàm từ các module tại đây

if __name__ == "__main__":
    main()
