load spatial;
attach '{{ env_var('HOME') }}/ducky-dbt/data/data.duckdb';


with 
    articles_grouped as (
    select
        supplier,
        brand,
        string_agg(distinct no_pim_article_id) as no_pim_article_id,
        supplier_article,
        first(article_description) as article_description,
        sum(purchase_volume_pcs) as purchase_volume_pcs,
        try_cast(sum(purchase_qty_eur) as integer) as purchase_qty_eur,
        date_part('year', date_purchased) as for_year,
    from {{ ref('int_et_main_stock_purchases') }}
    group by 
        supplier_article,
        supplier,
        brand,
        for_year
),

    final as (
        from articles_grouped
        where supplier_article is not null and supplier_article <> '' and brand is not null
        order by
            supplier_article,
            brand,
            purchase_qty_eur desc,
            supplier
    )

pivot final on for_year
using sum(purchase_qty_eur) as purchase_qty_eur
    ,sum(purchase_volume_pcs) as purchase_volume_pcs
;

detach data;

