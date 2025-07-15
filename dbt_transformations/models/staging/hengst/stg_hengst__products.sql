with final as (
    select
        *,
        upper(
            regexp_replace(hengsttype, '\s|[.]|[-/]', '', 'ig')
        ) as code,
        'hengst_cv' as arbitrary_brand
    from
        read_xlsx(
            {{ source('hengst_products', 'stg_hengst__products') }},
            range = 'A7:G',
            normalize_names = true,
            stop_at_empty = true
        )
    where
        cv_buses ilike '%x%'
)

select *
from final
