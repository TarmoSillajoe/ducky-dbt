{{ config(
        materialized='table'
    ) }}

with sales as (
    select 
        history.siffer,
        history.kuup,
        history.vsumma,
        history.vkogus,
        history.firmanimi,
        history.firma_id,
        history.voma,
        lmmat.nimi as nimetus,
        coalesce(cvgroups.grupinimi, 
                    bosch_cv.arbitrary_category, 
                    hengst_cv.arbitrary_category,
                    mann_cv.arbitrary_category) as cv_group_name,

        from {{ ref('stg_bao__sales_and_purchases') }} history
        left join {{ ref('stg_bao__rvsoft_products') }} lmmat using (siffer)
        left join {{ ref('rvsoft_cv_groups') }} cvgroups using (grupp)
        left join {{ ref('stg_bosch__cv_products') }} bosch_cv using(siffer)
        left join {{ ref('stg_hengst__products') }} hengst_cv on lmmat.tkood_normalized=hengst_cv.code
        left join {{ ref('stg_mann__products') }} mann_cv on lmmat.tkood_normalized=mann_cv.code
        left join {{ ref('stg_fi__stocks') }} fi on history.siffer=fi.mekofi_code


        where history.liik = 'M' and history.firma_id not in ('MEIE') and history.firma_id not ilike 'E80%'
            and history.kuup between '2018-01-01' and '2025-12-31'
)
,

    final as (
        select
            firma_id,
            vsumma,
            vkogus,
            voma,
            kuup

        from sales
        where cv_group_name is not null

    )

    from final
