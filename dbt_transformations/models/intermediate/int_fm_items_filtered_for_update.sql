{{ config(materialized='view') }}

with pricelist_items as (
    select 
        code,
        purchase_price,
        1 as min_order
    from {{ ref('stg_fm__prices') }}
),
------------
db_merchantitems as (
    select id, 
    item_id,
    code, 
    min_order
    from {{ ref('stg_fm__merchantitems') }}
),
---------
result as (
    select db_merchantitems.id,
        pricelist_items.purchase_price,
        db_merchantitems.item_id,
        db_merchantitems.min_order
    from db_merchantitems
    join pricelist_items using(code, min_order)
)
from result