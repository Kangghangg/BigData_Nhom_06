"""
MODULE 02: PYSPARK VEGETATION DENSITY & MEMORY OPTIMIZATION
Thành viên đảm nhận: Thành viên 2

Nhiệm vụ:
1. Tiếp nhận DataFrame sạch từ TV1.
2. Tính mật độ thực vật và NDVI trung bình theo Region.
3. Phân loại mật độ thực vật.
4. Tối ưu phân vùng dữ liệu theo Region.
5. Persist kết quả để tái sử dụng ở các bước sau.
"""

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


def process_vegetation_density(df_clean):
    print("=" * 70)
    print("=== [TV2] PYSPARK VEGETATION DENSITY ===")
    print("=" * 70)

    density_df = (
        df_clean
        .groupBy("Region")
        .agg(
            F.avg("VegetationDensity").alias("AvgVegetationDensity"),
            F.avg("NDVI").alias("AvgNDVI"),
            F.avg("SoilMoisture").alias("AvgSoilMoisture"),
            F.count("*").alias("RecordCount")
        )
    )

    density_df = density_df.withColumn(
        "density_category",
        F.when(
            F.col("AvgVegetationDensity") < 20,
            "Rất Thưa (Khô Hạn)"
        )
        .when(
            F.col("AvgVegetationDensity") <= 50,
            "Mật Độ Trung Bình"
        )
        .otherwise("Vùng Ốc Đảo")
    )

    density_df = density_df.repartition("Region")

    
    density_df.persist(StorageLevel.MEMORY_AND_DISK)

   
    density_df.count()

    print("[TV2] Kết quả mật độ thực vật trung bình:")
    density_df.orderBy("Region").show(truncate=False)

    print(
        "[TV2] Storage Level:",
        density_df.storageLevel
    )

    print("=" * 70)
    print("=== [TV2] HOÀN THÀNH ===")
    print("=" * 70)

    return density_df


def calculate_density(df_clean):
    return process_vegetation_density(df_clean)


if __name__ == "__main__":
    print("Module 02 - Khai báo thành công!")