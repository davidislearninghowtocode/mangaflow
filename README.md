# MangaFlow: Modern Data Lakehouse Platform
MangaFlow là một nền tảng Data Platform hoàn chỉnh được xây dựng dựa theo kiến trúc Medallion (Modern Data Lakehouse). Dự án mô phỏng cách một doanh nghiệp xử lý dòng dữ liệu lón từ tầng thu thập thời gian thực (streaming ingestion), lưu trữ phi tập trung (data lake), biến đổi dữ liệu có quản trị (data transformation & governance) trên hạ tầng Cloud, cuối cùng là phục vụ phân tích (Business Intelligence).

## Architecture System
Hệ thống được thiết kế hoàn toàn theo mô hình ELT (Extract - Load - Transform) nhằm tối ưu hoá năng lực tính toán phân tán:
    1. **Ingestion layer (streaming)**: **Jikan API** -> Kafka Producer -> Kafka Broker -> Kafka Consumer.
    2. **Storage layer (Data lake - Bronze)**: dữ liệu được Kafka Consumer đóng gói thành  định dạng cột nén (parquet) và lưu trữ tại MinIO (giả lập AWS S3).
    3. **Orchestration layer: Apache Airflow** điều phối lịch trình, thực hiện nạp dữ liệu lớn (bulk load) từ MinIO lên Cloud Data Warehouse thôgn qua 'COPY INTO'.
    4. **Data Warehouse & Transformation layer (Silver & Gold)**: **Snowflake** đóng vai trò lưu trữ và tính toán tập trung. **dbt (Data Build Tool)** thực hiện chuẩn hoá cấu trúc thành mô hình **Star Schema (Fact/Dimension)**.
    5. **Data Quality & Governance**: Tích hợp **dbt test** để kiểm tra toàn vẹn dữ liệu tự động. Quản lý dòng chảy dữ liệu bằng **dbt docs (Data Lineage)**.
    6. **Analytics layer**: **Metabase** kết nối vào tầng Gold phục vụ trực quan hoá dữ liệu.

## Visualization
### 1. Data Lineage Graph (Dòng chảy dữ liệu trong dbt)
*Dưới đây là sơ đồ Lineage minh họa tiến trình biến đổi từ bảng thô hệ thống nguồn đến các bảng Fact/Dim:*
![dbt Lineage](./images/dbt_lineage.png)

### 2. MangaFlow Business Intelligence Dashboard (Metabase)
*Giao diện báo cáo phân tích hiệu năng và phân phối dữ liệu truyện được kết nối trực tiếp từ tầng Gold của Snowflake:*
![Metabase Dashboard](./images/metabase_dashboard.png)

## Hướng dẫn triển khai chạy Local
### 1. Khởi động hạ tầng
cd docker
docker compose up -d

### 2. Khởi chạy Streaming (Ingestion)
# bật môi trường ảo
source venv/bin/activate 

# chạy consumer trước để đợi lệnh
python3 streaming/consumer.py

# mở terminal mới và chạy Producer để đổ data từ API về Kafka
python3 streaming/producer.py

### 3. Biến đổi dữ liệu và kiểm thử (dbt)
cd dbt_mangaflow
dbt run
dbt test



