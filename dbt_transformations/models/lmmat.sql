{{ config(
        materialized='view',
    ) }}

select siffer,nimi from baodb.lmmat
where nimi is not null