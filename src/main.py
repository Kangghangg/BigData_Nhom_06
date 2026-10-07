import argparse
import importlib
import os
import sys
import time
import traceback

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")          
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)       
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ.setdefault("PYTHONUNBUFFERED", "1")

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
for _p in (CURRENT_DIR, PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)


def _import_module(base_name: str):
    """Import module có tên bắt đầu bằng chữ số (01_, 02_, 03_, 04_)."""
    try:
        return importlib.import_module(base_name)
    except ModuleNotFoundError:
        return importlib.import_module(f"src.{base_name}")


mod01 = _import_module("01_data_ingestion")
mod02 = _import_module("02_pyspark_density")
mod03 = _import_module("03_graph_clustering")
mod04 = _import_module("04_benchmark")

from pyspark.sql import SparkSession  # noqa: E402


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("Nhom06_DesertVegetationDensity_EndToEnd")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def resolve_data_path(cli_path: str) -> str:
    """Tìm file dữ liệu theo thứ tự ưu tiên."""
    candidates = [
        cli_path,
        os.path.join(PROJECT_ROOT, cli_path),
        os.path.join(PROJECT_ROOT, "10_desert_vegetation_density.csv"),
        os.path.join(PROJECT_ROOT, "file_mau_sample.csv"),
    ]
    for c in candidates:
        if c and os.path.isfile(c):
            return c
    return candidates[2] 


def run_end_to_end(data_path: str, replicate: int, repeat: int, out_dir: str) -> dict:
    print("#" * 70)
    print("#  NHOM 06 - CHAY END-TO-END main.py (_desert_vegetation_density)  #")
    print("#" * 70)
    total_t0 = time.perf_counter()

    # ========== PHASE 1 -> 2 -> 3 : chung mot SparkSession ==========
    spark = create_spark_session()
    try:
        t0 = time.perf_counter()
        df_clean, hdfs_info = mod01.run_data_ingestion(spark, csv_path=data_path)   # TV1
        density_df = mod02.process_vegetation_density(df_clean)                    # TV2
        # TV3: tham so thu 2 can cot NDVI muc dong => truyen df_clean 
        graph_results = mod03.run_graph_and_clustering(df_clean, df_clean)         # TV3
        phase123_sec = time.perf_counter() - t0
        density_df.unpersist()   
    finally:
        spark.stop()   # BAT BUOC: dong session de TV4 tao duoc local[p] moi

    # ========== PHASE 4 : TV4 tu quan ly SparkSession local[p] ==========
    bench_df = mod04.run_performance_benchmark(
        data_path=data_path, replicate=replicate, repeat=repeat, out_dir=out_dir
    )

    total_sec = time.perf_counter() - total_t0

    # ========== TONG KET ==========
    top_pr = max(graph_results["pagerank"], key=graph_results["pagerank"].get)
    print("\n" + "=" * 70)
    print("=== [TV5] TONG KET END-TO-END ===")
    print(f" - Phase 1-2-3 (1 SparkSession)   : {phase123_sec:.2f} s")
    print(f" - HDFS: {hdfs_info['num_blocks']} block(s), "
          f"tong luu tru cum = {hdfs_info['total_cluster_storage_mb']} MB (k=3)")
    print(f" - Region co PageRank cao nhat    : {top_pr} "
          f"({graph_results['pagerank'][top_pr]:.4f})")
    print(" - Bang benchmark (TV4):")
    print(bench_df.to_string(index=False))
    print(f" - Bieu do PNG + bang so lieu tai : {out_dir}/")
    print(f" - TONG THOI GIAN CHAY            : {total_sec:.2f} s")
    print("=" * 70)
    return {"hdfs": hdfs_info, "graph": graph_results, "benchmark": bench_df}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Main End-to-End - TV5 Nhom 06")
    parser.add_argument("--data", default="file_mau_sample.csv",
                        help="Duong dan file CSV du lieu sa mac")
    parser.add_argument("--replicate", type=int, default=50,
                        help="He so nhan ban du lieu cho benchmark")
    parser.add_argument("--repeat", type=int, default=2,
                        help="So lan lap moi cau hinh core")
    parser.add_argument("--out", default="docs/benchmark",
                        help="Thu muc xuat bieu do PNG")
    args = parser.parse_args()

    out_abs = args.out if os.path.isabs(args.out) else os.path.join(PROJECT_ROOT, args.out)
    data = resolve_data_path(args.data)

    try:
        run_end_to_end(data, args.replicate, args.repeat, out_abs)
        print("=== MAIN.PY: END-TO-END THANH CONG ===")
        sys.exit(0)
    except Exception:
        traceback.print_exc()
        print("=== MAIN.PY: END-TO-END THAT BAI ===")
        sys.exit(1)