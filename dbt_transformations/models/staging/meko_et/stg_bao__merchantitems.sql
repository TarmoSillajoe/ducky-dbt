{{ config(materialized='view') }}

select 
    -- ids
    id,
    item_id,
    merchant_id,

    --varchars
    code, 
    "description",

    -- doubles
    purchase_price

    -- integers
    min_order,

    -- timestamps
    modified_at

from pgdb.merchant_item