{{ config(materialized='view') }}

with

pricelist as (
    select
        code,
        44 as manufacturer_id,
        description,
        ean,
        min_order,
        purchase_price,
        description
    from {{ ref('stg_cei__prices') }}
)

select
    pricelist.code,
    pricelist.manufacturer_id,
    pricelist.ean,
    pricelist.min_order,
    pricelist.purchase_price,
    pricelist.description
from pricelist
anti join {{ ref('stg_cei__db_factoryitems') }}
    using (code, manufacturer_id)
