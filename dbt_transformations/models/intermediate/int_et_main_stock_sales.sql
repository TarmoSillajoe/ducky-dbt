{{ config(
        materialized='table'
    ) }}

with sales as (
    select h.siffer,
        h.kuup,
        h.vsumma,
        h.vkogus,
        h.firmanimi,
        h.firma_id,
        h.jrk,
        lmmat.nimi as nimetus,
        cvgroups.grupinimi
        from {{ ref('stg_bao__sales_and_purchases') }} h
        left join {{ ref('stg_bao__rvsoft_products') }} lmmat using (siffer)
        left join {{ ref('rvsoft_cv_groups') }} cvgroups using (grupp)
        where h.liik = 'M' and h.firma_id not in ('MEIE') and h.firma_id not ilike 'E80%'
            and h.kuup between '2018-01-01' and today()
)
select * from sales
