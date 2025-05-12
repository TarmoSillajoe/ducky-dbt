{{ config(
        materialized='view',
    ) }}
-- This model is used to stage the bao_stock data

select * exclude(kak, tulek, minek),
    try_cast (coalesce(kak, 0) as int) as kak,
    try_cast (coalesce(tulek, 0) as int) as tulek,
    try_cast (coalesce(minek, 0) as int) as minek,
    try_cast (
        coalesce(kak, 0) + coalesce(tulek, 0) - coalesce(minek, 0) as int
    ) as stock,
from st_read(
    {{ source('bao_stocks', 'stg_bao__stocks') }},
        open_options = ARRAY ['ENCODING=ISO-8859-1']
    )