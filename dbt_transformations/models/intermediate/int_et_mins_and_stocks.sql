{{ config(
        materialized='table',
    ) }}
--------------
with
    stocks as (
        select
            siffer,
            tulek,
            minek,
            stock,
            khind
        from
            {{ ref('stg_bao__stocks') }}
    ),
--------
    min_levels as (
        select
            mi.code as siffer,
            items.code as manuf_code,
            manufacturers.name as manufacturer,
            qty as stock_min_level,
            mi.item_id
        from
            {{ ref('stg_bao__min_levels') }}
            join {{ ref('stg_bao__merchantitems') }} mi on stg_bao__min_levels.merchant_item_id = mi.id
            join {{ ref('stg_bao__manufactureritems') }} items on mi.item_id=items.id
            join {{ ref('stg_bao__manufacturers') }} manufacturers on items.manufacturer_id=manufacturers.id
    ),
-- product descriptions
    descriptions as (
        select 
            siffer, 
            nimi as description
        from {{ ref('stg_bao__rvsoft_products') }}
    )
----------
select
    stocks.siffer,
    stocks.tulek,
    stocks.minek,
    stocks.stock,
    min_levels.manuf_code,
    min_levels.manufacturer,
    min_levels.stock_min_level,
    descriptions.description,
    min_levels.item_id
from
    min_levels
    join  stocks using(siffer)
    left join descriptions using(siffer)