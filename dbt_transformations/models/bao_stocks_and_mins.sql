{{ config(
        materialized='table',
    ) }}
--------------
with
    stock as (
        select
            siffer,
            try_cast (coalesce(kak, 0) as int) as kak,
            try_cast (coalesce(tulek, 0) as int) as tulek,
            try_cast (coalesce(minek, 0) as int) as minek,
            try_cast (
                coalesce(kak, 0) + coalesce(tulek, 0) - coalesce(minek, 0) as int
            ) as stock,
            khind
        from
            {{ ref('stg_bao_stocks') }}
    ),
    ------------tr
    min_levels as (
        select
            mi.code as siffer,
            item.code as manuf_code,
            manufacturer.name as manufacturer,
            mi.item_id,
            qty as stock_min_level
        from
            pgdb.min_level
            join pgdb.merchant_item mi on merchant_item_id = mi.id
            join pgdb.item on mi.item_id=item.id
            join pgdb.manufacturer on item.manufacturer_id=manufacturer.id
    )
    -----------
select
    *
from
    min_levels
    join stock using(siffer)