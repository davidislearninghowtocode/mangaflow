{{ config(materialized='table') }} -- Tầng Gold nên lưu dạng TABLE để tăng tốc độ truy vấn cho BI

with staging_data as (
    select * from {{ ref('stg_manga') }}
)

select
    -- Tạo một Surrogate Key (Khóa thay thế) hoặc dùng chính manga_id nếu đảm bảo duy nhất
    manga_id,
    title,
    manga_type,
    status,
    is_publishing,
    synopsis,
    processed_at as updated_at
from staging_data
-- Lọc trùng để đảm bảo mỗi manga chỉ xuất hiện một dòng duy nhất trong bảng Dimension
qualify row_number() over (partition by manga_id order by processed_at desc) = 1