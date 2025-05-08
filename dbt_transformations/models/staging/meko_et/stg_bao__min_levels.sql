{{ config(materialized='view') }}

select
-- ids
    merchant_item_id,
    merchant_id,
    branch_id
        -- integers
        as qty,
    -- datetimes
    modified_at

from pgdb.min_level
