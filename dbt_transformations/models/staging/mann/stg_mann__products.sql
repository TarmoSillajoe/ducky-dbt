with final as (
    select
        *,
        upper(regexp_replace(mannfilter, '\s|[.]|[-/]', '', 'ig')) as code,
        'mann_hummel_cv' as arbitrary_brand,

    from
        read_xlsx(
            {{ source('mann_products', 'stg_mann__products') }},
            range = 'A1:E',
            normalize_names = true,
            stop_at_empty = true,
            all_varchar = true
        )

    where segment ilike '%truck%'
)

select * from final
