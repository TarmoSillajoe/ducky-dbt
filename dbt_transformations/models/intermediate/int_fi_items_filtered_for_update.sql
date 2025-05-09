{{ config(materialized='view') }}

with pricelist_items as (
    select 
        mekofi_code as code,
        purch_price_excl_vat as purchase_price,
        coalesce(sales_quantity, 1) as min_order
    from {{ ref('stg_fi__stocks') }}
),
------------
db_items as (
    select id, code, min_order
    from {{ ref('stg_fi__db_items') }}
),
---------
result as (
    select db_items.id,
        pricelist_items.purchase_price
    from db_items
    join pricelist_items using(code, min_order)
)
from result