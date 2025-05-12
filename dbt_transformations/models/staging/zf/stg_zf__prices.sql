with
    raw as (
        select
            trim(columns(*))
        from
            read_xlsx (
                {{ source('zf_pricelist', 'stg_zf__prices') }},
                range = 'A2:H',
                header = true,
                normalize_names = true,
                all_varchar = true
            ) where material is not null
    ) 
---------------------
select
    material, --varchar
    market, --varchar
    lower(description), --varchar
    try_cast(scale as integer) as scale,
    case
        when regexp_matches (price_unit, '^\d+[.]\d+[,]\d+$') then 
            try_cast(
                regexp_replace (replace (price_unit, ',', '.'), '[.]', '')
                as double)
        else try_cast(price_unit as double)
    end as price_unit,  --double
    curr, --varchar
    try_cast(valid_from as integer) + '1899-12-30'::date  as valid_from, --date
    try_cast(valid_to as integer) + '1899-12-30'::date  as valid_to --date
from raw


