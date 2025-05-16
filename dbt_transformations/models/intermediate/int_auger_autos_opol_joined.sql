{{ config(materialized='view') }}

with auger as (
    select item_id,
    code,
    min_order,
    purchase_price,
    description
    from {{ ref('stg_auger__db_merchantitems') }}
    where modified_at > '2025-01-01'
),
-------------
autos as (
    select item_id,
    purchase_price as autos_price,
    strftime(modified_at, '%Y-%m-%d') as autos_time,
    from {{ ref('stg_autos__db_merchant_items') }}
),
---------------------
opol as (
    select item_id,
    purchase_price as opol_price,
    strftime(modified_at, '%Y-%m-%d') as opol_time,
    from {{ ref('stg_opol__db_merchantitems') }}
),
-----
joined as
(select
    auger.code,
    auger.purchase_price,
    auger.min_order,
    autos_price,
    autos_time,
    opol_price,
    opol_time,
    auger.description
from auger
left join autos using(item_id)
left join opol using(item_id))
--------
select * from joined
where autos_price is not null or opol_price is not null

