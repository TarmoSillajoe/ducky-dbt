{{ config(materialized='view') }}

with pricelist_items as (
    select 
        material as code,
        price_unit as purchase_price,
        coalesce(scale, 1) as min_order
    from {{ ref('stg_zf__prices') }}
),
------------
db_merchantitems as (
    select id, 
    code, 
    min_order
    from {{ ref('stg_zf__db_merchanditems') }}
),
---------
result as (
    select db_merchantitems.id,
        pricelist_items.purchase_price
    from db_merchantitems
    join pricelist_items using(code, min_order)
)
from result