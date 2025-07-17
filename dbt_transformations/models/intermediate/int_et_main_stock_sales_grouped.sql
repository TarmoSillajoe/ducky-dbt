-- firma_id,
-- vsumma,
-- vkogus,
-- voma,
-- kuup


with grouped_by_customer_id as (

    select
        firma_id,
        date_part('year', kuup) as year,
        try_cast(sum(vsumma) as integer) as quantity_eur,
        cast(avg((vsumma-voma)/voma) filter (voma > 0) * 100 as integer) as avg_profitability,
        --cast(median((vsumma-voma)/voma) filter (voma > 0) * 100 as integer) as med_profitability,
 
    from {{ ref('int_et_main_stock_sales') }} history
    
    group by firma_id, year
    order by year desc
)

select * from grouped_by_customer_id

