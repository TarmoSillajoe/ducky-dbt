with products as (
    SELECT
        products.siffer,
        products.tkood,
        try_cast(products.grupp as integer) as grupp,
        cvgroups.grupinimi,
        bosch_cv.arbitrary_brand,
        hengst_cv.arbitrary_brand,
    from {{ ref('stg_bao__rvsoft_products') }} products
    left join {{ ref('rvsoft_cv_groups') }} cvgroups using (grupp)
    left join {{ ref('stg_bosch__cv_products') }} bosch_cv using(siffer)
    left join {{ ref('stg_hengst__products') }} hengst_cv on products.tkood_normalized=hengst_cv.code
    left join {{ ref('stg_mann__products') }} mann_cv on products.tkood_normalized=mann_cv.code


)

select * from products
