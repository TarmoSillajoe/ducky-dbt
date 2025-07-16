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
        coalesce(lmmat.tkood_normalized, fi.code) as supplier_article,
        coalesce(lmmat.kamark, fi.tecdoc_dlnr) as tecdoc_brand_id,
        coalesce(cvgroups.grupinimi, 
                    bosch_cv.arbitrary_brand, 
                    hengst_cv.arbitrary_brand,
                    mann_cv.arbitrary_brand) as cv_group_name,

        from {{ ref('stg_bao__sales_and_purchases') }} history
        left join {{ ref('stg_bao__rvsoft_products') }} lmmat using (siffer)
        left join {{ ref('rvsoft_cv_groups') }} cvgroups using (grupp)
        left join {{ ref('stg_bosch__cv_products') }} bosch_cv using(siffer)
        left join {{ ref('stg_hengst__products') }} hengst_cv on lmmat.tkood_normalized=hengst_cv.code
        left join {{ ref('stg_mann__products') }} mann_cv on lmmat.tkood_normalized=mann_cv.code
        left join {{ ref('stg_fi__stocks') }} fi on history.siffer=fi.mekofi_code
        where history.liik = 'O' and history.firma_id not in ('MEIE') and history.firma_id not ilike 'E80%'
            and history.kuup between '2023-01-01' and '2025-12-31'
),

    final as (
        select 
            purchases.firmanimi as supplier,
            tecdoc.brand,
            purchases.siffer as no_pim_article_id,
            purchases.nimetus as article_description,
            purchases.kuup as date_purchased,
            purchases.firma_id as supplier_id,
            purchases.ssumma as purchase_qty_eur,
            purchases.skogus as purchase_volume_pcs,
            purchases.supplier_article,
            tecdoc_brand_id,

        from
        purchases
        left join {{ ref('tecdoc_ids') }} tecdoc on purchases.tecdoc_brand_id=cast(data_supplier_no as varchar)
        where purchases.cv_group_name is not null
    )
select * from final
