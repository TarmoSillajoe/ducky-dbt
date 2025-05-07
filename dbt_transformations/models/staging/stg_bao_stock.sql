{{ config(
        materialized='view',
    ) }}
-- This model is used to stage the bao_stock data

select * from st_read(
    {{ source('bao_stock', 'stg_bao_stock') }},
        open_options = ARRAY ['ENCODING=ISO-8859-1']
    )