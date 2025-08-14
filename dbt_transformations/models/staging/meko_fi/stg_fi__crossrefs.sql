{{ config(materialized='table') }}

SELECT
    product_code, -- varchar
    product_name, -- varchar
    product_code_supplier, -- varchar
    supplier, -- varchar
    name_of_supplier, -- varchar
    country_code_supplier, -- varchar
    tecdoc_article_code, -- varchar
    tecdoc_supplier_code, -- varchar
    net_weight, -- double
    country_code_product, --varchar
    package, --double
    commodity_code, -- varchar
    eancode, -- varchar
from
    read_xlsx (
        {{ source('fi_crossrefs','stg_fi__crossrefs') }},
        normalize_names = true
    )