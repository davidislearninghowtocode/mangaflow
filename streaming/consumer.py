import time
import json
import io
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3

# Cấu hình Kết nối
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'raw_manga'
GROUP_ID = 'mangaflow_consumer_group'

# Cấu hình MinIO (S3 Local)
MINIO_ENDPOINT = 'http://localhost:9000'
ACCESS_KEY = 'mangaflow_admin'
SECRET_KEY = 'mangaflow_password'
BUCKET_NAME = 'bronze-manga'

# Khởi tạo MinIO Client bằng thư viện boto3
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

# Tự động tạo Bucket trong MinIO nếu chưa có
try:
    s3_client.head_bucket(Bucket=BUCKET_NAME)
except:
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    print(f"📁 Đã tạo bucket mới: '{BUCKET_NAME}' trên MinIO")

# Khởi tạo Kafka Consumer
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe([TOPIC_NAME])

def upload_parquet_to_minio(data_batch):
    """ Chuyển đổi list JSON thành file Parquet và upload lên MinIO """
    if not data_batch:
        return

    # 1. Chuyển đổi dữ liệu thành Pandas DataFrame để dễ xử lý cấu trúc
    df = pd.DataFrame(data_batch)
    
    # Bổ sung các trường Metadata (Vô cùng quan trọng trong Data Lake để truy vết lỗi)
    df['ingested_at'] = datetime.utcnow().isoformat()
    
    # 2. Chuyển đổi sang PyArrow Table (Để lưu định dạng Parquet)
    table = pa.Table.from_pandas(df)
    
    # 3. Ghi file Parquet vào bộ nhớ RAM (Buffer) thay vì ghi ra đĩa cứng local để tối ưu tốc độ
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression='snappy')
    buffer.seek(0)
    
    # 4. Định nghĩa đường dẫn lưu trữ trên Data Lake theo cấu trúc phân cấp thời gian (Partitioning)
    # Ví dụ: bronze-manga/year=2026/month=05/day=19/manga_1715900000.parquet
    now = datetime.utcnow()
    file_name = f"year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}/manga_{int(time.time())}.parquet"
    
    # 5. Đẩy file lên MinIO
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=file_name,
        Body=buffer.getvalue()
    )
    print(f"🚀 [DATA LAKE] Đã upload thành công file Parquet lên Bronze: {file_name}")

def main():
    print("📥 Kafka Consumer đã khởi động và đang đợi dữ liệu...")
    batch = []
    BATCH_SIZE = 50       # Gom đủ 50 bộ truyện thì đóng file Parquet một lần
    last_flush_time = time.time()
    MAX_WAIT_TIME = 15    # Hoặc nếu quá 15 giây chưa đủ 50 bộ cũng đóng file để tránh delay dữ liệu

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                # Kiểm tra xem có dữ liệu tồn đọng trong batch đã quá thời gian chờ chưa
                if batch and (time.time() - last_flush_time) > MAX_WAIT_TIME:
                    print("⏱️ Đã quá thời gian chờ. Tiến hành đóng gói batch hiện tại...")
                    upload_parquet_to_minio(batch)
                    batch = []
                    last_flush_time = time.time()
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Lỗi Consumer: {msg.error()}")
                    break

            # Giải mã tin nhắn nhận được từ Kafka
            manga_data = json.loads(msg.value().decode('utf-8'))
            batch.append(manga_data)
            
            # Nếu batch đủ kích thước mong muốn, tiến hành ghi file
            if len(batch) >= BATCH_SIZE:
                print(f"📦 Batch đã đạt {BATCH_SIZE} records. Đang đóng gói file Parquet...")
                upload_parquet_to_minio(batch)
                batch = []
                last_flush_time = time.time()
                
    except KeyboardInterrupt:
        if batch:
            print("💾 Đang lưu nốt dữ liệu cuối cùng trước khi tắt...")
            upload_parquet_to_minio(batch)
    finally:
        consumer.close()

if __name__ == '__main__':
    main()