

with 
    years_pivoted as (
        pivot {{ ref('int_et_main_stock_sales_grouped') }}
        on year in (2023, 2024, 2025)
        using first(quantity_eur) as quantity_eur, first(avg_profitability) as avg_profitability_percent
    ),

    names_and_ids as (
        select 
            years_pivoted.*,
            companies.nimi as name,
            category_description,
            companies.kood
        from years_pivoted 
        join {{ ref('stg_bao__rvsoft_firma') }} companies on years_pivoted.firma_id=companies.kood
        left join {{ ref('rvsoft_company_categories') }} cust_cats on companies.tyyp=cust_cats.category_id
    ),

    final as (
        select 
            name,
            category_description,
            "2023_quantity_eur",
            "2023_avg_profitability_percent",
            "2024_quantity_eur",
            "2024_avg_profitability_percent",
            "2025_quantity_eur",
            "2025_avg_profitability_percent"
        from names_and_ids
        order by "2024_quantity_eur" desc
    )
from final