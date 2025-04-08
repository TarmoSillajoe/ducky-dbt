{{ config(
        materialized='external',
        location='output/sales_history.parquet',
        format='parquet'
    ) }}

with sales as (
    select h.siffer,
        h.kuup,
        h.vsumma,
        h.vkogus,
        h.firmanimi,
        h.firma_id,
        h.jrk,
        lmmat.nimi as nimetus
        from {{ source('downloads', 'sales_history') }} h
        left join lmmat using (siffer)
        where h.liik = 'M' and h.firma_id not in ('MEIE') and h.firma_id not ilike 'E80%'
            and h.kuup between '2025-03-01' and '2025-03-31'
)
select * from sales
