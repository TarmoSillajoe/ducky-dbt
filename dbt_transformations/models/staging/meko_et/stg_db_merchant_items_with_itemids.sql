{{ config(materialized='table') }}

select
-- ids
    id, --primary key
    item_id,
    merchant_id,

    -- varchars
    code, --sku of the product
    description,

    -- integers
    min_order,

    -- doubles
    purchase_price,

    --date
    modified_at

from pgdb.merchant_item

