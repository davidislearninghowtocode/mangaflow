{{ config(materialized='view') }}

with raw_data as (
    select 
        raw_content,
        ingested_at
    from {{ source('mangaflow_raw', 'stg_raw_manga') }}
)

select
    -- Trích xuất các trường dữ liệu từ cột VARIANT (raw_content) và ép kiểu dữ liệu chuẩn xác
    (raw_content:mal_id)::int as manga_id,
    (raw_content:title)::varchar as title,
    (raw_content:type)::varchar as manga_type,
    (raw_content:chapters)::int as total_chapters,
    (raw_content:volumes)::int as total_volumes,
    (raw_content:status)::varchar as status,
    (raw_content:publishing)::boolean as is_publishing,
    (raw_content:score)::float as score,
    (raw_content:scored_by)::int as total_voters,
    (raw_content:rank)::int as global_rank,
    (raw_content:synopsis)::varchar as synopsis,
    
    -- Giữ lại trường thời gian nạp để phục vụ cho việc kiểm tra (Audit)
    ingested_at as processed_at
from raw_data