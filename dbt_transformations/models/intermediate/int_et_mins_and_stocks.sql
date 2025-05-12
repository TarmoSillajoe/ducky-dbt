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
------------tr
    min_levels as (
        select
            mi.code as siffer,
            items.code as manuf_code,
            manufacturers.name as manufacturer,
            qty as stock_min_level,
            mi.item_id
        from
            {{ ref('stg_bao__min_levels') }}
            join {{ ref('stg_db_merchant_items_with_itemids') }} mi on merchant_item_id = mi.id
            join {{ ref('stg_db_manufactureritems') }} items on mi.item_id=items.id
            join {{ ref('stg_db_manufacturers') }} manufacturers on items.manufacturer_id=manufacturers.id
    ),
-- product descriptions
    descriptions as (
        select 
            siffer, 
            nimi as description
        from {{ ref('stg_bao__products') }}
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