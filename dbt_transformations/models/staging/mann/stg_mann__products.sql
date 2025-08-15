with final as (
    select
        mannfilter,
        new,
        price_net_in,
        filtertype,
        segment,
        upper(regexp_replace(mannfilter, '\s|[.]|[-/]', '', 'ig')) as code,
        case
            when segment ilike '%truck%' then 'mann_hummel_cv' else null end as arbitrary_brand 
    from
        read_xlsx(
            {{ source('mann_products', 'stg_mann__products') }},
            range = 'A1:E',
            normalize_names = true,
            stop_at_empty = true,
            all_varchar = true
        )

)

select * from final
