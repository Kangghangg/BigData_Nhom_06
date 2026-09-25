# DỰ ÁN BIG DATA: DESERT VEGETATION DENSITY (\_desert_vegetation_density)

Môn học: Big Data (HUIT)  
Nhóm thực hiện: Nhóm 06  
Nhánh Git: BigData_Nhom_6

---

## 1. TỔNG QUAN ĐỀ TÀI

Đề tài tập trung phân tích mật độ thực vật và chỉ số xanh (NDVI) tại các vùng đất khô hạn sa mạc trên quy mô dữ liệu lớn. Hệ thống xây dựng pipeline xử lý song song từ khâu lưu trữ HDFS, tính toán bằng PySpark RDD/DataFrame, khai phá đồ thị và phân cụm vùng sinh thái, cho đến đo đạc hiệu năng tính toán theo các định luật song song.

### Khái niệm và Quy ước Kỹ thuật Chính

- **Lưu trữ HDFS:** Tính toán số lượng HDFS Block = ceil(Dung lượng file / Block Size) và thiết lập Replication Factor k.
- **Xử lý Song song PySpark:** Tính mật độ thực vật theo ô lưới không gian, sử dụng .persist() lưu RAM và .partitionBy() giảm shuffling.
- **Khai phá Đồ thị & Phân cụm:** Sử dụng thuật toán PageRank (damping factor d = 0.85), HITS (chuẩn hóa L2 Norm cho Authority & Hub) và K-Means phân cụm vùng sa mạc.
- **Đánh giá Hiệu năng Song song:** Đo đạc Speedup, Efficiency và phân tích overhead qua Định luật Amdahl, Định luật Gustafson và Chỉ số Karp-Flatt Metric.

---

## 2. TRUY XUẤT VÀ CẤU TRÚC MÃ NGUỒN

```text
BigData_Project/
├── .gitignore                  # Cấu hình chặn upload dữ liệu thô
├── README.md                   # Tài liệu hướng dẫn dự án
├── requirements.txt            # Danh sách thư viện Python cần thiết
├── docs/                       # Thư mục lưu trữ báo cáo và slide
│   └── Nhóm_06.pptx
└── src/                        # Thư mục chứa mã nguồn chính
    ├── __init__.py
    ├── 01_data_ingestion.py    # TV1: Tiền xử lý dữ liệu & Tính HDFS Block
    ├── 02_pyspark_density.py   # TV2: PySpark RDD/DataFrame Density Pipeline
    ├── 03_graph_clustering.py  # TV3: PageRank, HITS & K-Means
    ├── 04_benchmark.py         # TV4: Đo hiệu năng Amdahl, Gustafson, Karp-Flatt
    ├── main.py                 # TV5: Mã nguồn tổng hợp chạy End-to-End
    └── utils/                  # Thư mục chứa hàm bổ trợ
        └── hdfs_helper.py
```

---

## 3. HƯỚNG DẪN CÀI ĐẶT VÀ CHẠY DỰ ÁN

### Yêu cầu Môi trường

- Python: Phiên bản 3.10 trở lên.
- Java JDK: Phiên bản 11 (Bắt buộc để chạy PySpark).
- PySpark: Phiên bản 3.4.0+.

---

### Trường hợp 1: Thao tác trên MacOS

1. Mở Terminal và clone nhánh code:

   ```bash
   git clone -b BigData_Nhom_6 <URL_REPO_GITHUB>
   cd BigData_Project
   ```

2. Tạo và kích hoạt môi trường ảo Python:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. Cài đặt các thư viện phụ thuộc:

   ```bash
   pip install --upgrade pip
   pip install pyspark pandas numpy matplotlib
   ```

4. Chạy toàn bộ Pipeline End-to-End:
   ```bash
   python3 src/main.py
   ```

---

### Trường hợp 2: Thao tác trên Windows

1. Mở Command Prompt (CMD) / PowerShell và clone nhánh code:

   ```cmd
   git clone -b BigData_Nhom_6 <URL_REPO_GITHUB>
   cd BigData_Project
   ```

2. Tạo và kích hoạt môi trường ảo Python:

   ```cmd
   python -m venv .venv
   .venv\Scripts\activate
   ```

3. Cài đặt các thư viện phụ thuộc:

   ```cmd
   pip install --upgrade pip
   pip install pyspark pandas numpy matplotlib
   ```

4. Chạy toàn bộ Pipeline End-to-End:
   ```cmd
   python src/main.py
   ```

---

## 4. QUY ƯỚC NỘP BÀI (MÔN BIG DATA HUIT)

1. File nộp duy nhất: `Nhóm_06.rar`.
2. Thành phần bên trong file nén:
   - File Slide báo cáo PowerPoint: `Nhóm_06.pptx`.
   - Thư mục Mã nguồn (Source Code) hoàn chỉnh.
3. Quy định nghiêm ngặt: Tuyệt đối **KHÔNG** nộp dữ liệu thô trong file `.rar` (do dữ liệu đã có sẵn trên hệ thống của giảng viên).
