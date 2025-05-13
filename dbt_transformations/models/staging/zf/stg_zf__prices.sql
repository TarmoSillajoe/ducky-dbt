{{
    config(materialized='table')
}}

with
raw as (
    select trim(columns(*))
    from
        read_xlsx(
            {{ source('zf_pricelist', 'stg_zf__prices') }},
            range = 'A2:H',
            header = true,
            normalize_names = true,
            all_varchar = true
        )
    where material is not null
),

---------------------
cleaned as (
    select
        material, --varchar
        market, --varchar
        curr, --varchar
        lower(description) as description,
        try_cast(scale as integer) as scale,  --double
        case
            when regexp_matches(price_unit, '^\d+[.]\d+[,]\d+$')
                then
                    try_cast(
                        regexp_replace(replace(price_unit, ',', '.'), '[.]', '')
                        as double
                    )
            else try_cast(price_unit as double)
        end as price_unit, --varchar
        --date
        try_cast(valid_from as integer) + '1899-12-30'::date as valid_from,
        try_cast(valid_to as integer) + '1899-12-30'::date as valid_to --date
    from raw
)

-------
select
    -- varchars
    material,
    market,
    curr,
    description,

    -- integers
    scale,

    -- doubles
    price_unit,

    -- dates
    valid_from,
    valid_to
from cleaned
