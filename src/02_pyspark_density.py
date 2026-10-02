"""
MODULE 02: PYSPARK VEGETATION DENSITY & MEMORY OPTIMIZATION
Thành viên đảm nhận: Thành viên 2
Nhiệm vụ:
1. Tiếp nhận DataFrame sạch từ TV1 (src/01_data_ingestion.py).
2. Gom nhóm theo 'Region' và 'SoilMoisture' tính Mật độ thực vật trung bình.
3. Phân vùng dữ liệu .partitionBy("Region") để tối ưu xáo trộn (shuffling).
4. Lưu dữ liệu tạm trên RAM bằng cơ chế .persist() / .cache().
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def process_vegetation_density(df_clean):
    print("-> [TV2] Đang xử lý tính toán Mật độ Thực vật Sa mạc...")
    # Thành viên 2 viết code xử lý tại đây
    pass

if __name__ == "__main__":
    print("Module 02 - Khai báo thành công!")
