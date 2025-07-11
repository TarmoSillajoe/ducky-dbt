{{ config(materialized='view') }}

with purchases as (select * from purchases),

    purchases_agg as (
    select
        date_part('year', purchases.kuup) as year,
        siffer as no_pim_article_id,
        sum(ssumma) as purchase_volume,
        sum(skogus) as quantity,
    from {{ ref('int_et_main_stock_purchases') }} purchases
   
    group by no_pim_article_id, year
    order by purchase_volume desc
) 
from purchases  where no_pim_article_id ilike '%69935%' 
    and purchase_volume > 5000