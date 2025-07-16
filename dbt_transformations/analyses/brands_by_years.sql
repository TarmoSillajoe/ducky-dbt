load spatial;
attach '{{ env_var('HOME') }}/ducky-dbt/data/data.duckdb';



create or replace view purch as
with purch as (
    select * from {{ ref('int_et_main_stock_purchases') }}
)

select *
from purch;




with years as (
    pivot purch
    on date_part('year',date_purchased)
    using try_cast(sum(purchase_qty_eur) as integer)
    group by brand
    )
select * from years order by "2024" desc
;

detach data;

