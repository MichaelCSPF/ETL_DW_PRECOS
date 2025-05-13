with source_data as (
    select * from {{ source('db_price_yqq1', 'st_precos_emporio_rosa') }}
)
select * from source_data
