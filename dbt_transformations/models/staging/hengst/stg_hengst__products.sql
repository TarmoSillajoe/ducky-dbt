with final as (
    select
        edv,
        status_country_exclusion,
        hengsttype,
        filter_type,
        cv_buses,
        pc_transporter,
        offhighway,
        upper(
            regexp_replace(hengsttype, '\s|[.]|[-/]', '', 'ig')
        ) as code,
        case
            when cv_buses ilike '%x%' then 'hengst_cv'
            when pc_transporter ilike '%x%' then 'hengst_pv'
            when offhighway ilike '%x%' then 'hengst_offhighway'
            else null
        end as arbitrary_brand
    from
        read_xlsx(
            {{ source('hengst_products', 'stg_hengst__products') }},
            range = 'A7:G',
            normalize_names = true,
            stop_at_empty = true
        )
)

select *
from final
