with products as (
    SELECT
        p.siffer,
        p.grupp,
        p.tkood
    from {{ ref('stg_bao__rvsoft_products') }}
)

select * from products