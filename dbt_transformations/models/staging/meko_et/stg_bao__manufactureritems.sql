{{ config(materialized='view') }}

select
    id, --int, primary key
    base_item_id, --int foreign key to "base_item"
    manufacturer_id, --int foreign key to "manufacturer"
    code --varchar
from pgdb.item