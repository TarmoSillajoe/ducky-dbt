{{ config(
        materialized='view',
    ) }}
-- This model is used to stage the bao_stock data

select * from st_read(
    {{ source('bao_stocks', 'stg_bao__stocks') }},
        open_options = ARRAY ['ENCODING=ISO-8859-1']
    )