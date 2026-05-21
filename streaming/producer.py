import time
import json
import requests
from confluent_kafka import Producer

# Cấu hình Kafka Broker (Chạy trong Docker hoặc local)
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'raw_manga'
API_URL = 'https://api.jikan.moe/v4/manga'

# Khởi tạo Kafka Producer
conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
producer = Producer(conf)

def delivery_report(err, msg):
    """ Hàm callback báo cáo trạng thái gửi tin nhắn vào Kafka """
    if err is not None:
        print(f"❌ Gửi tin nhắn thất bại: {err}")
    else:
        print(f"✅ Tin nhắn đã gửi vào topic {msg.topic()} [Partition: {msg.partition()}]")

def fetch_manga_data(page=1):
    """ Lấy dữ liệu từ API với cơ chế Retry cơ bản nếu gặp lỗi """
    params = {'page': page, 'limit': 25}
    try:
        response = requests.get(API_URL, params=params, timeout=10)
        
        # Nếu bị dính Rate Limit (HTTP 429), ngủ một lúc rồi thử lại
        if response.status_code == 429:
            print("⚠️ Bị giới hạn lượt gọi (Rate Limit). Đang tạm dừng 5 giây...")
            time.sleep(5)
            return fetch_manga_data(page)
            
        if response.status_code == 200:
            return response.json().get('data', [])
        else:
            print(f"❌ Lỗi API: HTTP {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Lỗi kết nối mạng: {e}")
        return []

def main():
    current_page = 1
    print("🚀 Kafka Producer đã khởi động. Bắt đầu cào dữ liệu từ Jikan API...")
    
    try:
        while True:
            manga_list = fetch_manga_data(page=current_page)
            
            if not manga_list:
                print("🏁 Không còn dữ liệu hoặc gặp lỗi liên tiếp. Tạm dừng...")
                time.sleep(60) # Chờ 1 phút trước khi thử lại vòng lặp mới
                current_page = 1 # Reset trang để cập nhật lại dữ liệu mới
                continue

            print(f"📦 Đang xử lý trang {current_page} với {len(manga_list)} manga...")
            
            for manga in manga_list:
                # Gửi từng bản ghi Manga dạng JSON vào Kafka
                payload = json.dumps(manga).encode('utf-8')
                
                # Dùng id của manga làm Kafka Key để đảm bảo dữ liệu cùng 1 manga luôn vào cùng 1 partition
                kafka_key = str(manga.get('mal_id')).encode('utf-8')
                
                producer.produce(
                    topic=TOPIC_NAME, 
                    key=kafka_key, 
                    value=payload, 
                    callback=delivery_report
                )
            
            # Đẩy tất cả tin nhắn đang đợi trong hàng đợi đi
            producer.flush()
            
            # Sang trang tiếp theo và nghỉ 3 giây tuân thủ Rate Limit của Jikan API
            current_page += 1
            time.sleep(3)
            
    except KeyboardInterrupt:
        print("🛑 Producer đã dừng bởi người dùng.")

if __name__ == '__main__':
    main()