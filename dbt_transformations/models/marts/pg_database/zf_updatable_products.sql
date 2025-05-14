{{ config(materialized='table') }}

select
    id, -- primary key field of merchants_items table
    purchase_price --purchase price in Euros

from {{ ref('int_zf_items_filtered_for_update') }}
