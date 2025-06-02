{{
    config(materialized='table')
}}


with
raw as (
    select *
    from
        read_xlsx(
            {{ source('fm_pricelist', 'stg_fm__prices') }},
            range = 'A5:O',
            stop_at_empty = true,
            normalize_names = true,
            ignore_errors = true
        )
)

-------
select
    brand,
    product_category,
    part_no as code,
    sub_segment,
    lvcv,
    cluster_rebates,
    invoice as purchase_price,
    invoice_1,
    increase_pep,
    remarks,
    setpc,
    status,
    invoice_2,
    invoice_3,
    ds_range
from raw
