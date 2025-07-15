{{ config(
        materialized='table'
    ) }}

with purchases as (
    select 
        history.siffer,
        history.kuup,
        history.ssumma,
        history.skogus,
        history.firmanimi,
        history.firma_id,
        history.jrk,
        lmmat.nimi as nimetus,
        lmmat.tkood_normalized as supplier_article,
        lmmat.kamark,
        coalesce(cvgroups.grupinimi, 
                    bosch_cv.arbitrary_brand, 
                    hengst_cv.arbitrary_brand) as cv_group_name,
        
        from {{ ref('stg_bao__sales_and_purchases') }} history
        left join {{ ref('stg_bao__rvsoft_products') }} lmmat using (siffer)
        left join {{ ref('rvsoft_cv_groups') }} cvgroups using (grupp)
        left join {{ ref('stg_bosch__cv_products') }} bosch_cv using(siffer)
        left join {{ ref('stg_hengst__products') }} hengst_cv on lmmat.tkood_normalized=hengst_cv.code
        where history.liik = 'O' and history.firma_id not in ('MEIE') and history.firma_id not ilike 'E80%'
            and history.kuup between '2023-01-01' and '2025-12-31'
)
select * from purchases
