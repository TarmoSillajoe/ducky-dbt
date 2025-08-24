with
    hengst_cv as (
        SELECT
            hengst.code,
            hengst.arbitrary_category,
        from {{ ref('stg_hengst__products') }} hengst
        where arbitrary_category='hengst_cv'
    ),

    mann_cv as (
        SELECT
            mann.code,
            mann.arbitrary_category,
        from {{ ref('stg_mann__products') }} mann
        where arbitrary_category='mann_hummel_cv'
    ),


    product_data_enriched as (
    SELECT
        products.siffer,
        products.tkood,
        products.tkood_normalized,
        try_cast(products.grupp as integer) as grupp,
        coalesce (
            cvgroups.grupinimi,
            bosch_cv.arbitrary_category,
            hengst_cv.arbitrary_category,
            mann_cv.arbitrary_category,
        ) as cv_category
    from {{ ref('stg_bao__rvsoft_products') }} products
    left join {{ ref('rvsoft_cv_groups') }} cvgroups using (grupp)
    left join {{ ref('stg_bosch__cv_products') }} bosch_cv using(siffer)
    left join hengst_cv on products.tkood_normalized=hengst_cv.code
    left join mann_cv on products.tkood_normalized=mann_cv.code
),

    final as (
        select
            siffer,
            tkood,
            tkood_normalized,
            cv_category,
        from product_data_enriched
        where cv_category is not null
    )

select * from final
