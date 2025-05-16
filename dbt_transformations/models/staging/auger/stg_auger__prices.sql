{{ config(materialized='view') }}

select
    material,
    material_oem_number,
    material_description,
    customer,
    net_price_doc_curr,
    currency,
    tax_number,
    pallet_price,
    try_cast(price_unit as integer) as price_unit,
    try_cast(qty_in_box as integer) as qty_in_box,
    round(gross_weight, 2) as gross_weight

from read_xlsx(
    {{ source('auger_pricelist', 'stg_auger__prices') }},
    normalize_names = true
)
