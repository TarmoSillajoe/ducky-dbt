{{ config(materialized='table') }}

select

-- integers
    cast(tecdoc_dlnr as varchar) as tecdoc_dlnr,
    sales_quantity,
    stock_malmi,
    stock_kouvola,
    stock_hakkila,
    stock_hakkila_lc,
    customs_code,

    -- doubles
    weight,
    core_price_excl_vat,
    tecdoc_artnr,
    round(purch_price_excl_vat, 2) as purch_price_excl_vat,

    -- varchars
    upper(
        regexp_replace (tecdoc_artnr, '\s|[.]|[-/]', '', 'ig')
        ) as code,
    mekofi_code,
    article_descr,
    alternatives,
    orig_country,
    brandtype,
    core_code,
    tecdoc_genartnr,
    manufacturer,
    non_returnable,
    try_cast(ean_code as bigint) as ean_code,

from
    read_csv(
        {{ source('fi_stocks','stg_fi__stocks') }},
        delim = '|',
        ignore_errors = true,
        normalize_names = true,
        decimal_separator = ','
    )
