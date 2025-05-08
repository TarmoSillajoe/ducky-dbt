{{ config(
        materialized='view',
    ) }}
-- This model is used to stage the RV Soft's replacements data

select * from st_read(
    {{ source('bao_replacements', 'stg_bao_replacements') }},
        open_options = ARRAY ['ENCODING=ISO-8859-1']
    )