load spatial;
attach '{{ env_var('HOME') }}/ducky-dbt/data/data.duckdb';



-- create or replace view purch as
-- with purch as (
--     select * from {{ ref('int_et_main_stock_purchases') }}
-- )

-- select *
-- from purch;

select
    customer,
    date_part('year', kuup) as year,
    try_cast(sum(vsumma) as integer) as turnover,
    try_cast(sum(vsumma) as integer) as quantity_eur,
    try_cast((avg((vsumma-voma)/voma) * 100) as integer) as profitability,
from {{ ref('int_et_main_stock_sales') }}
group by customer, year
order by year desc
;

from {{ ref('stg_bao__rvsoft_firma') }} using sample 40;
summarize from {{ ref('stg_bao__rvsoft_firma') }};

detach data;

