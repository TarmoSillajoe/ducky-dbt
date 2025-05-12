{{ config(materialized='table') }}

select
    id, --integer, primary key
    manufacturer_id, --integer
    base_item_id, -- foreing key to table "base_item"

    code, --varchar
from pgdb.item