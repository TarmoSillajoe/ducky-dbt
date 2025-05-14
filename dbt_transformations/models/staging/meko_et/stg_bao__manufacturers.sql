{{ config(materialized='table') }}

select
    id, --int, primary key
    merchant_id, --int foreign key to "merchant"
    name --varchar
from pgdb.manufacturer