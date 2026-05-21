from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import snowflake.connector
import os

#config connection
MINIO_ENDPOINT = 'http://minio:9000' #dùng 'minio' vì airflow chạy trong mạng Docker chung
MINIO_ACCESS_KEY = 'mangaflow_admin'
MINIO_SECRET_KEY = 'mangaflow_password'
BUCKET_NAME = 'bronze-manga'

SNOWFLAKE_CONN_PARAMS = {
"account": "BBCUPFA-YK61016",
    "user": "VMDAVIDSITE",
    "password": "Vuminhduc2001!", 
    "warehouse": "mangaflow_wh",
    "database": "mangaflow_db",
    "schema": "bronze"
}

def load_minio_parquet_to_snowflake():
    """ Đọc file parquet từ MinIO và nạp trực tiếp vào bảng Variant của Snowflake"""
    #kết nối MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    
    #kết nối Snowflake
    ctx = snowflake.connector.connect(**SNOWFLAKE_CONN_PARAMS)
    cs = ctx.cursor()
    
    #lấy danh sách các file trong bucket
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    if 'Contents' not in response:
        print("Khong tim thay file moi nao trong MinIO Data Lake")
        return
    
    for obj in response['Contents']:
        file_key = obj['Key']
        if file_key.endswith('.parquet'):
            print(f"Processing file: {file_key}")
            
            #đọc file dữ liệu từ MinIO về bộ nhớ tạm của Airflow
            file_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
            local_tmp_path = f"/tmp/{os.path.basename(file_key)}"
            
            with open(local_tmp_path, "wb") as f:
                f.write(file_obj['Body'].read())
            
            #đẩy file lên tầng Staging nội bộ của Snowflake
            cs.execute(f"PUT file://{local_tmp_path} @%stg_raw_manga AUTO_COMPRESS=TRUE;")
            
            #nạp dữ liệut ừ file Parquet (vừa put lên) vào bảng thô dưới dạng Variant
            cs.execute("""
                       COPY INTO mangaflow_db.bronze.stg_raw_manga (raw_content)
                       FROM @%stg_raw_manga
                       FILE_FORMAT = (TYPE = PARQUET)
                       PURGE = TRUE;
                       """)
            
            #xoá file tạm ở local airflow sau khi nạp xong
            os.remove(local_tmp_path)
            print(f"Đã nạo thành công {file_key} vào Snowflake Bronze")
    cs.close()
    ctx.close()
with DAG(
    dag_id="mangaflow_lakehouse_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:
    load_to_snowflake_task = PythonOperator(
        task_id="load_minio_to_snowflake",
        python_callable=load_minio_parquet_to_snowflake
    )