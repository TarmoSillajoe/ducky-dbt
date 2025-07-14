{{ config(
        materialized='view'
    ) }}


select
-- dates
    kuup,

    -- doubles
    allahp,
    hind,
    skogus,
    ssumma,
    soma,
    vkogus,
    vsumma,
    voma,

    -- integers
    ladu,
    kirjel,
    recnr,

    -- varchars
    siffer,
    firma_id,
    firmanimi,
    liik,
    jrk
from {{ source('sales_and_purchases', 'stg_sales_history') }}
where liik not in ('P')
