{{ config(materialized='view') }}

with 

source as (select
        group_product,
        cei_no,
        oem_no,
        description,
        original_manufacturers,
        applications,
        gearbox_differential_model,
        gearbox_differential_version,
        lcv_parts,
        nomenclatura_combinata,
        peso,
        min_qty,
        ean13,
        net_price
    from 
        {{ source('cei_pricelist', 'stg_cei__prices') }}
        ),


renamed as (
    select * rename(cei_no as code, min_qty as min_order, ean13 as ean, net_price as purchase_price)
    from source
    )


select * from renamed