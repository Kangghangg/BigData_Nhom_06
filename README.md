# DỰ ÁN BIG DATA: DESERT VEGETATION DENSITY (\_desert_vegetation_density)

**Môn học:** Big Data  
**Đơn vị:** Khoa CNTT - Trường Đại học Công Thương TP.HCM (HUIT)  
**Giảng viên hướng dẫn:** Thầy Đinh Nguyễn Trọng Nghĩa  
**Nhóm thực hiện:** Nhóm 06  
**Repository:** [BigData_Nhom_06](https://github.com/Kangghangg/BigData_Nhom_06.git)  
**Nhánh Git làm việc chính:** `BigData_Nhom_6`

---

## 1. TỔNG QUAN ĐỀ TÀI & ĐẶC TẢ TẬP DỮ LIỆU

Đề tài tập trung phân tích mật độ thảm thực vật và chỉ số xanh (NDVI) tại các vùng sa mạc khô hạn dựa trên tập dữ liệu thực tế **`10_desert_vegetation_density.csv`** (hơn 128.000 bản ghi). Hệ thống xây dựng pipeline xử lý song song phân tán từ khâu quản lý lưu trữ HDFS, tính toán bằng PySpark RDD/DataFrame, khai phá đồ thị mạng lưới không gian và phân cụm sinh thái, đến đo đạc hiệu năng tính toán song song.

### Cấu trúc 11 thuộc tính trong Tập dữ liệu thực tế:

1. **`Region`**: Khu vực sa mạc khảo sát (_Sahara, Sonoran, Arabian, Kalahari, Atacama, Mojave, Gobi_).
2. **`AvgTemperature`**: Nhiệt độ trung bình năm (°C).
3. **`AnnualRainfall`**: Lượng mưa trung bình năm (mm).
4. **`SoilMoisture`**: Độ ẩm trung bình của đất (%).
5. **`SoilSalinity`**: Nồng độ muối trong đất (g/kg).
6. **`WindSpeed`**: Tốc độ gió trung bình (m/s).
7. **`Altitude`**: Độ cao khu vực (m so với mực nước biển).
8. **`NDVI`**: Chỉ số thực vật đo từ ảnh vệ tinh (\\(0.0 \rightarrow 1.0\\)).
9. **`SurveyCode`**: Mã khảo sát _(Cột rác/ít ảnh hưởng - Lọc bỏ ở TV1)_.
10. **`GeologyIndex`**: Chỉ số địa chất _(Cột rác/ít ảnh hưởng - Lọc bỏ ở TV1)_.
11. **`VegetationDensity`** _(Biến mục tiêu)_: Mật độ thảm thực vật (%).

---

## 2. NGUYÊN LÝ KỸ THUẬT VÀ QUY ƯỚC MÔN HỌC (HUIT)

- **Lưu trữ HDFS Storage (TV1):** Tính toán số lượng HDFS Block bằng công thức \\(\text{Block Count} = \left\lceil \frac{\text{Dung lượng file}}{\text{Block Size (128 MB)}} \right\rceil\\). Thiết lập hệ số nhân bản Replication Factor \\(k = 3\\), tính tổng dung lượng vật lý toàn cụm \\(\text{Total Storage} = \text{Block Count} \times 128\text{ MB} \times 3\\).
- **Xử lý Song song PySpark & Tối ưu RAM (TV2):** Tính mật độ thực vật trung bình theo vùng và độ ẩm đất. Sử dụng `.persist()` lưu bộ nhớ RAM và `.partitionBy("Region")` giảm chi phí tráo đổi dữ liệu qua mạng (shuffling).
- **Khai phá Đồ thị & Phân cụm Sinh thái (TV3):**
  - **PageRank:** Áp dụng công thức Brin & Page (1998) với \\(d = 0.85\\), \\(PR(A) = (1-d) + d \sum \frac{PR(T_i)}{C(T_i)}\\) (theo quy ước HUIT: không chia \\(1-d\\) cho \\(N\\)).
  - **HITS Algorithm:** Tính điểm Authority và Hub, thực hiện chuẩn hóa **L2 Norm** sau mỗi vòng lặp.
  - **K-Means Clustering:** Phân cụm các vùng sa mạc thành 3 nhóm sinh thái dựa trên bộ đặc trưng môi trường.
- **Đánh giá Hiệu năng Tính toán Song song (TV4):** Thực nghiệm đo thời gian \\(T(p)\\) trên \\(p = 1, 2, 4, 8\\) cores. Phân tích Tốc độ tăng tốc \\(S(p)\\), Hiệu suất \\(E(p)\\), Định luật Amdahl, Định luật Gustafson và chỉ số Karp-Flatt Metric \\(e = \frac{1/S - 1/p}{1 - 1/p}\\).

---

## 3. BẢNG PHÂN CÔNG NHIỆM VỤ 6 THÀNH VIÊN (PIPELINE MATRIX)

|  STT  | Vai trò                    | Nhiệm vụ kỹ thuật chính                                                                                                                          | Sản phẩm bàn giao                                     |
| :---: | :------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------- |
| **1** | **Lê Dư Bảo Khang**        | Tiền xử lý dữ liệu sa mạc (lọc null, loại bỏ `SurveyCode` & `GeologyIndex`, giữ \\(0 \le \text{NDVI} \le 1\\)) và Tính toán cấu hình HDFS Block. | File `src/01_data_ingestion.py` & Báo cáo HDFS.       |
| **2** | **Nguyễn Dương Thục Uyên** | Lập trình PySpark RDD/DataFrame tính mật độ thực vật trung bình, tối ưu bộ nhớ `.persist()` và phân vùng `.partitionBy()`.                       | File `src/02_pyspark_density.py` & Spark DataFrame.   |
| **3** | **Hồ Ngọc Quý**            | Khai phá đồ thị liên kết không gian (PageRank \\(d=0.85\\), HITS L2 Norm) và Phân cụm sa mạc bằng K-Means (PySpark MLlib).                       | File `src/03_graph_clustering.py` & Kết quả phân cụm. |
| **4** | **Trần Nhã Phương**        | Thực nghiệm đo hiệu năng song song trên \\(p = 1, 2, 4, 8\\) cores, tính Amdahl, Gustafson, Karp-Flatt Metric và vẽ biểu đồ Matplotlib.          | File `src/04_benchmark.py` & Biểu đồ PNG.             |
| **5** | **Nguyễn Thái Bảo**        | Chuẩn hóa mã nguồn tích hợp End-to-End (`main.py`) và Biên soạn toàn văn Báo cáo Word tổng hợp.                                                  | File `src/main.py` & File `docs/BaoCao_BigData.docx`. |
| **6** | **Trần Ngô Trọng Trường**  | Thiết kế Slide thuyết trình PowerPoint, xây dựng kịch bản báo cáo 6 người, lọc bỏ data thô và Đóng gói file `Nhóm_06.rar`.                       | File `presentation/Nhóm_06.pptx` & `Nhóm_06.rar`.     |

---

## 4. CẤU TRÚC MÃ NGUỒN VÀ THƯ MỤC DỰ ÁN

```text
BigData_Project/
├── .gitignore                  # Cấu hình chặn upload dữ liệu thô (.csv, .rar, .venv)
├── README.md                   # Tài liệu hướng dẫn dự án (File này)
├── requirements.txt            # Danh sách thư viện Python (pyspark, pandas, numpy, matplotlib)
├── 10_desert_vegetation_density.csv # Tập dữ liệu thô thực tế (Chỉ lưu ở local)
├── docs/                       # Thư mục lưu trữ tài liệu báo cáo & workflow
│   ├── BaoCao_BigData.docx     # Báo cáo tổng hợp toàn văn
│   └── desert_vegetation_density_workflow_v3.docx # Tài liệu Workflow chi tiết
├── presentation/               # Thư mục lưu trữ slide thuyết trình
│   └── Nhóm_06.pptx            # Slide báo cáo báo vệ môn học
└── src/                        # Thư mục chứa mã nguồn Python
    ├── 01_data_ingestion.py    # TV1: Tiền xử lý dữ liệu & HDFS Storage Calculation
    ├── 02_pyspark_density.py   # TV2: PySpark Density Pipeline & Memory Persist
    ├── 03_graph_clustering.py  # TV3: PageRank, HITS & K-Means Clustering
    ├── 04_benchmark.py         # TV4: Benchmarking (Amdahl, Gustafson, Karp-Flatt)
    └── main.py                 # TV5: Main Integrated End-to-End Pipeline
```
