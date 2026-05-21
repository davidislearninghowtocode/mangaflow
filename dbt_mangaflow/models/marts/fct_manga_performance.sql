{{ config(materialized='table') }}

with staging_data as (
    select * from {{ ref('stg_manga') }}
)

select
    -- Khóa ngoại để JOIN với bảng DIM
    manga_id,
    score,
    total_voters,
    global_rank,
    total_chapters,
    total_volumes,
    processed_at as snapshot_date
from staging_data