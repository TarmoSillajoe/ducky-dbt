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
        replace(lmmat.tkood, ' ', '') as supplier_article,
        cvgroups.grupinimi,
        lmmat.kamark
        from {{ ref('stg_bao__sales_and_purchases') }} history
        left join {{ ref('stg_bao__rvsoft_products') }} lmmat using (siffer)
        left join {{ ref('rvsoft_cv_groups') }} cvgroups using (grupp)
        where history.liik = 'O' and history.firma_id not in ('MEIE') and history.firma_id not ilike 'E80%' and grupinimi is not null
            and history.kuup between '2017-01-01' and '2025-12-31'
)
select * from purchases
