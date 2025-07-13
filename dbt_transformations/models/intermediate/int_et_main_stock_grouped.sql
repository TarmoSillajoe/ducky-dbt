{{ config(
        materialized='table'
    ) }}

with purchases as (
    select 
        round(sum(ssumma), 2) as quantity,
        first(supplier_article),
        first(nimetus) as description_est,
        siffer,
    from {{ ref('int_et_main_stock_purchases') }}
    group by siffer
    order by quantity desc
)
select * from purchases
