load spatial;
attach '{{ env_var('HOME') }}/ducky-dbt/data/data.duckdb';




with purchases as (
    select * from {{ ref('int_et_main_stock_purchases') }}
)




select *
from bosch_cv limit 40;


