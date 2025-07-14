with final as (
        select
            material_number_10 as siffer,
            upper(
                regexp_replace (material_number_10, '\s|[.]|[-/]', '', 'ig')
            ) as code,
            material_description,
            pg1,
            pg2,
            pg3
        from
            read_xlsx (
                {{ source('bosch_cv_products', 'stg_bosch__cv_products') }},
                normalize_names=true
            )
    )
select * from final