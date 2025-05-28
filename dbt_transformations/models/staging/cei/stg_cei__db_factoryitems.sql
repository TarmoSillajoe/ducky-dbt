{{ config(materialized='table') }}

with

source as (
    select
        id as item_id,
        code,
        manufacturer_id
    from pgdb.item
    where manufacturer_id = 44
),

normalized as (
    select
        item_id,
        replace(replace(code,' ',''),'.','') as code,
        manufacturer_id
    from source
)

select * from normalized
