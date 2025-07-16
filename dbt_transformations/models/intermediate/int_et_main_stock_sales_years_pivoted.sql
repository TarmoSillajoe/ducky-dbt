
with 
    years_pivoted as (
        pivot {{ ref('int_et_main_stock_sales_grouped') }}
        on year in (2023, 2024, 2025)
        using first(quantity_eur) as quantity_eur, first(profitability) as avg_profitability
    ),

    names_and_ids as (
        select 
            years_pivoted.*,
            companies.nimi as name,
            companies.kood
        from years_pivoted 
        join {{ ref('stg_bao__rvsoft_firma') }} companies on years_pivoted.firma_id=companies.kood
    ),

    final as (
        select 
            *
        from names_and_ids
        order by "2025_quantity_eur" desc
    )
from final