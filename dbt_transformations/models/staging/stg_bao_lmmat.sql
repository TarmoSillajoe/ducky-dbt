{{ config(
        materialized='view',
    ) }}


select 
       siffer,
        replace(nimi, chr(132), '_') as nimi,
        riiul,
        grupp,
        allgrupp,
        kategr,
        liik,
        oshind,
        mghind,
        allah,
        keeld,
        firma,
        tootja,
        minhind,
        kuup,
        kassak,
        mkogus,
        ovhind,
        mvhind,
        kamark,
        tkood,
        kmp,
        kaal,
        tollk,
        triik,
        nmass,
        pikkus,
        laius,
        korgus,
        tecdoc,
        sisestas,
        sisaeg,
        modaeg
from st_read(
        {{ source('bao_lmmat', 'stg_bao_lmmat') }},
        open_options = ARRAY ['ENCODING=ISO-8859-1']
    )
