{{ config(materialized='view') }}

select 
    * exclude(purch_price_excl_vat, ean_code),
    round(purch_price_excl_vat, 2) as purch_price_excl_vat,
    try_cast(ean_code as bigint) as ean_code
from
    read_csv (
        {{ source('fi_stocks','stg_fi_stocks') }},
        delim = '|',
        ignore_errors = true,
        normalize_names = true,
        decimal_separator = ','
    )