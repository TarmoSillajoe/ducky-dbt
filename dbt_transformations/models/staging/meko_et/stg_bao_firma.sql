{{ config(
        materialized='view',
    ) }}
-- This model is used to stage the bao_firma data


select * exclude('nimi', 'taadr1', 'taadr2'),
    replace(nimi, chr(132), '_') as nimi,
    replace(taadr1, chr(132), '_') as taadr1,
    replace(taadr2, chr(132), '_') as taadr2
from st_read(
    {{ source('bao_firma', 'stg_bao_firma') }},
        open_options = ARRAY ['ENCODING=ISO-8859-1']
    )
