import math
import os

# Ép PySpark bind vào IP 127.0.0.1 để tránh bị treo mạng trên macOS
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def calculate_hdfs_blocks(file_size_mb: float = 20.0, block_size_mb: float = 128.0, replication_factor: int = 3) -> dict:
    num_blocks = math.ceil(file_size_mb / block_size_mb)
    total_cluster_storage_mb = num_blocks * block_size_mb * replication_factor
    return {
        "file_size_mb": file_size_mb,
        "block_size_mb": block_size_mb,
        "num_blocks": num_blocks,
        "replication_factor": replication_factor,
        "total_cluster_storage_mb": total_cluster_storage_mb
    }

def run_data_ingestion(spark: SparkSession, csv_path: str = "10_desert_vegetation_density.csv"):
    print("=" * 70)
    print("=== [TV1] BẮT ĐẦU TIỀN XỬ LÝ DỮ LIỆU & TÍNH TOÁN HDFS STORAGE ===")
    print("=" * 70)

    if os.path.exists(csv_path):
        print(f"[TV1 - Info]: Đang đọc dữ liệu từ file '{csv_path}'...")
        df_raw = spark.read.csv(csv_path, header=True, inferSchema=True)
    else:
        print(f"[TV1 - Info]: Chưa thấy file '{csv_path}' tại thư mục gốc. Đang tạo dữ liệu giả lập...")
        sample_data = [
            ("Sahara", 38.5, 45.0, 12.0, 8.5, 5.2, 320.0, 0.15, "SC001", "GI_A", 8.5),
            ("Sonoran", 32.0, 120.0, 22.0, 4.1, 3.8, 450.0, 0.35, "SC002", "GI_B", 24.0),
            ("Arabian", 41.0, 30.0, 8.0, 12.0, 6.1, 150.0, 0.08, "SC003", "GI_A", 4.2),
            ("Kalahari", 28.0, 210.0, 31.0, 3.0, 2.9, 850.0, 0.52, "SC004", "GI_C", 48.0),
            ("Atacama", 18.0, 15.0, 5.0, 15.5, 4.5, 2400.0, 0.02, "SC005", "GI_D", 1.8),
            ("Mojave", 35.0, 95.0, 18.0, 5.5, 4.0, 900.0, 0.28, "SC006", "GI_B", 18.5),
            ("Gobi", 22.0, 110.0, 19.0, 6.2, 5.8, 1100.0, 0.31, "SC007", "GI_C", 22.0)
        ]
        columns = [
            "Region", "AvgTemperature", "AnnualRainfall", "SoilMoisture", 
            "SoilSalinity", "WindSpeed", "Altitude", "NDVI", 
            "SurveyCode", "GeologyIndex", "VegetationDensity"
        ]
        df_raw = spark.createDataFrame(sample_data, schema=columns)

    print(f"[TV1 - Raw Data]: Tổng số bản ghi ban đầu = {df_raw.count()} dòng.")

    columns_to_drop = ["SurveyCode", "GeologyIndex"]
    existing_cols_to_drop = [col for col in columns_to_drop if col in df_raw.columns]
    df_dropped = df_raw.drop(*existing_cols_to_drop)
    print(f"[TV1 - Drop Columns]: Đã loại bỏ các cột rác: {existing_cols_to_drop}")

    df_clean = df_dropped.filter(
        (F.col("NDVI").isNotNull()) & 
        (F.col("NDVI") >= 0.0) & 
        (F.col("NDVI") <= 1.0) &
        (F.col("VegetationDensity").isNotNull())
    )

    print(f"[TV1 - Clean Data]: Số bản ghi sau làm sạch = {df_clean.count()} dòng.")
    df_clean.show(5, truncate=False)

    file_size_mb = 20.0
    if os.path.exists(csv_path):
        file_size_mb = round(os.path.getsize(csv_path) / (1024 * 1024), 2)
        if file_size_mb == 0:
            file_size_mb = 20.0

    hdfs_info = calculate_hdfs_blocks(file_size_mb=file_size_mb, block_size_mb=128.0, replication_factor=3)

    print("\n" + "=" * 50)
    print("=== [TV1 - BÁO CÁO CẤU HÌNH HDFS STORAGE] ===")
    print(f" - Dung lượng file                 : {hdfs_info['file_size_mb']} MB")
    print(f" - Kích thước HDFS Block           : {hdfs_info['block_size_mb']} MB")
    print(f" - Số lượng Block HDFS (ceil(D/B)) : {hdfs_info['num_blocks']} Block(s)")
    print(f" - Replication Factor              : {hdfs_info['replication_factor']}")
    print(f" - Dung lượng thực tế toàn cụm     : {hdfs_info['total_cluster_storage_mb']} MB")
    print("=" * 50 + "\n")

    return df_clean, hdfs_info

if __name__ == "__main__":
    print("-> Đang khởi tạo SparkSession...")
    spark_sess = SparkSession.builder \
        .appName("TV1_Test") \
        .master("local[2]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    spark_sess.sparkContext.setLogLevel("ERROR")
    
    run_data_ingestion(spark_sess)
    spark_sess.stop()
    print("-> Đã hoàn thành và đóng SparkSession thành công!")
