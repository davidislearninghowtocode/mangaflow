# MangaFlow: Modern Data Lakehouse Platform

MangaFlow là một nền tảng Data Platform hoàn chỉnh được xây dựng dựa theo kiến trúc **Medallion (Modern Data Lakehouse)**. Dự án mô phỏng cách một doanh nghiệp xử lý dòng dữ liệu lớn từ tầng thu thập thời gian thực (Streaming Ingestion), lưu trữ phi tập trung (Data Lake), biến đổi dữ liệu có quản trị (Data Transformation & Governance) trên hạ tầng Cloud, và cuối cùng là phục vụ phân tích (Business Intelligence).

---

## 🏗️ Architecture System

Hệ thống được thiết kế hoàn toàn theo mô hình **ELT (Extract - Load - Transform)** nhằm tối ưu hóa năng lực tính toán phân tán:

1. **Ingestion Layer (Streaming):** `Jikan API` -> Kafka Producer -> Kafka Broker -> Kafka Consumer.
2. **Storage Layer (Data Lake - Bronze):** Dữ liệu được Kafka Consumer đóng gói thành định dạng cột nén (Parquet) và lưu trữ tại MinIO (giả lập AWS S3).
3. **Orchestration Layer:** **Apache Airflow** điều phối lịch trình, thực hiện nạp dữ liệu lớn (Bulk Load) từ MinIO lên Cloud Data Warehouse thông qua lệnh `COPY INTO`.
4. **Data Warehouse & Transformation Layer (Silver & Gold):** **Snowflake** đóng vai trò lưu trữ và tính toán tập trung. **dbt (Data Build Tool)** thực hiện chuẩn hóa cấu trúc thành mô hình **Star Schema (Fact/Dimension)**.
5. **Data Quality & Governance:** Tích hợp **dbt test** để kiểm tra toàn vẹn dữ liệu tự động. Quản lý dòng chảy dữ liệu bằng **dbt docs (Data Lineage)**.
6. **Analytics Layer:** **Metabase** kết nối vào tầng Gold phục vụ trực quan hóa dữ liệu.

---

## 📊 Visualization

### 1. Data Lineage Graph (Dòng chảy dữ liệu trong dbt)

*Sơ đồ Lineage minh họa tiến trình biến đổi từ bảng thô hệ thống nguồn đến các bảng Fact/Dim:*

![dbt Lineage](./images/dbt_lineage.png)

### 2. MangaFlow Business Intelligence Dashboard (Metabase)

*Giao diện báo cáo phân tích hiệu năng và phân phối dữ liệu truyện được kết nối trực tiếp từ tầng Gold của Snowflake:*

![Metabase Dashboard](./images/metabase_dashboard.png)

---

## 🚀 Hướng Dẫn Triển Khai Chạy Local

### 1. Khởi động hạ tầng
```bash
cd docker
docker compose up -d

# Bật môi trường ảo tại thư mục gốc dự án
source venv/bin/activate

# Chạy consumer trước để đợi lệnh từ producer
python3 streaming/consumer.py

# MỞ MỘT CỬA SỔ TERMINAL MỚI, kích hoạt venv và chạy Producer để đổ data từ API về Kafka
python3 streaming/producer.py

cd dbt_mangaflow
dbt run --profiles-dir .
dbt test --profiles-dir .
```