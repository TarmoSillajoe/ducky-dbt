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
        history.jrk,
        history.voma,
        lmmat.nimi as nimetus,
        cvgroups.grupinimi
        from {{ ref('stg_bao__sales_and_purchases') }} history
        left join {{ ref('stg_bao__rvsoft_products') }} lmmat using (siffer)
        left join {{ ref('rvsoft_cv_groups') }} cvgroups using (grupp)
        where history.liik = 'M' and history.firma_id not in ('MEIE') and history.firma_id not ilike 'E80%'
            and history.kuup between '2017-01-01' and today()
)
select * from sales
