"""
Module: 03_graph_clustering.py
Đảm nhận: Thành viên 3 - Hồ Ngọc Quý
Nhiệm vụ:
  1. Khai phá mạng lưới liên kết vùng sa mạc: PageRank (chuẩn HUIT: (1 - d)), HITS (chuẩn hóa L2 Norm).
  2. Phân cụm K-Means sinh thái (k = 3) bằng PySpark MLlib.
"""

import math
from pyspark.sql import DataFrame
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
import os
import sys

# Ép PySpark sử dụng chính file python.exe đang chạy trong môi trường ảo venv
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# ==============================================================================
# PHẦN 1: XÂY DỰNG ĐỒ THỊ VÀ LINK ANALYSIS (PAGERANK & HITS)
# ==============================================================================

def build_region_similarity_graph(df_density: DataFrame, threshold: float = 0.05):
    """
    Xây dựng danh sách kề có hướng giữa các Region dựa trên độ tương đồng NDVI.
    Hai vùng sa mạc u và v liên kết với nhau nếu độ chênh lệch |NDVI_u - NDVI_v| <= threshold.
    """
    # Thu thập thông tin đại diện từng vùng
    region_stats = (
        df_density.groupBy("Region")
        .agg({"NDVI": "avg"})
        .withColumnRenamed("avg(NDVI)", "avg_ndvi")
        .collect()
    )

    regions = [row["Region"] for row in region_stats]
    ndvi_dict = {row["Region"]: row["avg_ndvi"] for row in region_stats}
    
    # Tạo danh sách liên kết có hướng giữa các vùng
    # Nếu u và v có độ ẩm/thực vật tương đồng thì có cạnh truyền dẫn sinh thái u -> v
    adj_out = {r: [] for r in regions}
    adj_in = {r: [] for r in regions}

    for u in regions:
        for v in regions:
            if u != v:
                diff = abs(ndvi_dict[u] - ndvi_dict[v])
                if diff <= threshold:
                    adj_out[u].append(v)
                    adj_in[v].append(u)

    return regions, adj_out, adj_in


def compute_pagerank(regions, adj_out, adj_in, d: float = 0.85, max_iter: int = 20):
    """
    Tính PageRank chuẩn HUIT (Brin & Page 1998):
    Công thức: PR(p) = (1 - d) + d * sum(PR(q) / L(q))
    LƯU Ý QUY ƯỚC HUIT: KHÔNG chia (1 - d) cho N.
    """
    # Khởi tạo PageRank ban đầu = 1.0 cho mọi nút
    pr = {r: 1.0 for r in regions}

    for _ in range(max_iter):
        next_pr = {}
        for p in regions:
            incoming_sum = 0.0
            for q in adj_in[p]:
                l_q = len(adj_out[q])
                if l_q > 0:
                    incoming_sum += pr[q] / l_q
            # Công thức chuẩn HUIT
            next_pr[p] = (1.0 - d) + d * incoming_sum
        pr = next_pr

    return pr


def compute_hits(regions, adj_out, adj_in, max_iter: int = 10):
    """
    Tính điểm Hub và Authority theo thuật toán HITS.
    QUY ƯỚC HUIT: Chuẩn hóa L2 Norm (chia cho sqrt(sum(x^2))) sau mỗi vòng lặp.
    """
    # Khởi tạo ban đầu
    hub = {r: 1.0 for r in regions}
    auth = {r: 1.0 for r in regions}

    for _ in range(max_iter):
        # 1. Cập nhật Authority: a(p) = tổng h(q) với q -> p
        next_auth = {}
        for p in regions:
            next_auth[p] = sum(hub[q] for q in adj_in[p])

        # Chuẩn hóa L2 Norm cho Authority
        l2_auth = math.sqrt(sum(v ** 2 for v in next_auth.values()))
        if l2_auth > 0:
            auth = {k: v / l2_auth for k, v in next_auth.items()}
        else:
            auth = next_auth

        # 2. Cập nhật Hub: h(p) = tổng a(q) với p -> q
        next_hub = {}
        for p in regions:
            next_hub[p] = sum(auth[q] for q in adj_out[p])

        # Chuẩn hóa L2 Norm cho Hub
        l2_hub = math.sqrt(sum(v ** 2 for v in next_hub.values()))
        if l2_hub > 0:
            hub = {k: v / l2_hub for k, v in next_hub.items()}
        else:
            hub = next_hub

    return auth, hub


# ==============================================================================
# PHẦN 2: PHÂN CỤM K-MEANS SINH THÁI BẰNG PYSPARK MLLIB
# ==============================================================================

def run_kmeans_clustering(df_clean: DataFrame, k: int = 3, seed: int = 42):
    """
    Phân cụm dữ liệu vùng sa mạc thành k = 3 cụm sinh thái bằng PySpark MLlib.
    Đặc trưng sử dụng: AvgTemperature, AnnualRainfall, SoilMoisture, SoilSalinity, NDVI, VegetationDensity.
    """
    feature_cols = [
        "AvgTemperature",
        "AnnualRainfall",
        "SoilMoisture",
        "SoilSalinity",
        "NDVI",
        "VegetationDensity"
    ]

    # Đảm bảo bỏ các dòng null trên các cột feature
    df_features = df_clean.dropna(subset=feature_cols)

    # 1. Gom nhóm vector đặc trưng
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    assembled_data = assembler.transform(df_features)

    # 2. Chuẩn hóa tỷ lệ đặc trưng (StandardScaler)
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(assembled_data)
    scaled_data = scaler_model.transform(assembled_data)

    # 3. Huấn luyện mô hình K-Means (k = 3)
    kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=k, seed=seed)
    kmeans_model = kmeans.fit(scaled_data)

    clustered_df = kmeans_model.transform(scaled_data)

    return kmeans_model, clustered_df


# ==============================================================================
# HÀM ĐIỀU PHỐI CHÍNH CỦA MODULE 3 (TV3 EXECUTION ENTRYPOINT)
# ==============================================================================

def run_graph_and_clustering(df_clean: DataFrame, df_density: DataFrame):
    """
    Hàm đóng gói tổng thể cho Thành viên 3, sẵn sàng để main.py gọi thực thi.
    """
    print("\n--- BẮT ĐẦU MODULE 3: GRAPH MINING & K-MEANS CLUSTERING (QUÝ) ---")

    # 1. Đồ thị và Link Analysis
    print("1. Đang xây dựng mạng lưới tương đồng sinh thái sa mạc...")
    regions, adj_out, adj_in = build_region_similarity_graph(df_density)

    print("2. Đang thực thi PageRank chuẩn HUIT (d = 0.85, (1-d))...")
    pagerank_scores = compute_pagerank(regions, adj_out, adj_in)

    print("3. Đang thực thi HITS chuẩn HUIT (L2 Normalization)...")
    auth_scores, hub_scores = compute_hits(regions, adj_out, adj_in)

    print("\n=== KẾT QUẢ PHÂN TÍCH LIÊN KẾT ĐỒ THỊ VÙNG SA MẠC ===")
    print(f"{'Region':<15} | {'PageRank':<10} | {'Authority (L2)':<15} | {'Hub (L2)':<10}")
    print("-" * 60)
    for r in regions:
        print(f"{r:<15} | {pagerank_scores[r]:<10.4f} | {auth_scores[r]:<15.4f} | {hub_scores[r]:<10.4f}")

    # 2. Phân cụm K-Means
    print("\n4. Đang phân cụm K-Means (k = 3 cụm sinh thái) bằng PySpark MLlib...")
    kmeans_model, clustered_df = run_kmeans_clustering(df_clean, k=3)

    print("\n=== THỐNG KÊ CÁC CỤM SINH THÁI SA MẠC ===")
    clustered_df.groupBy("cluster").agg(
        {"AvgTemperature": "avg", "AnnualRainfall": "avg", "VegetationDensity": "avg", "Region": "count"}
    ).withColumnRenamed("count(Region)", "RecordCount").show()

    print("--- HOÀN TẤT MODULE 3 THÀNH CÔNG ---\n")
    return {
        "pagerank": pagerank_scores,
        "authority": auth_scores,
        "hub": hub_scores,
        "kmeans_model": kmeans_model,
        "clustered_df": clustered_df
    }


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    # Khởi tạo SparkSession cục bộ để test riêng Module 3
    spark = SparkSession.builder \
        .appName("Test_Module_03_Quy") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n--- CHẠY TEST MODULE 3 VỚI FILE CSV MẪU ---")
    
    # Đọc trực tiếp file dữ liệu mẫu (thay đường dẫn nếu bạn để file ở thư mục khác)
    csv_path = "file_mau_sample.csv"
    
    try:
        df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        
        # Tiền xử lý nhanh: lọc bỏ cột rác theo đúng chuẩn TV1
        df_clean = df_raw.drop("SurveyCode", "GeologyIndex").filter("NDVI >= 0.0 AND NDVI <= 1.0")
        
        # Chạy kiểm thử hàm chính
        results = run_graph_and_clustering(df_clean=df_clean, df_density=df_clean)
        print("--- KIỂM THỬ MODULE 3 THÀNH CÔNG RỰC RỠ ---")
    except Exception as e:
        print(f"Lỗi khi đọc file hoặc chạy test: {e}")
    finally:
        spark.stop()