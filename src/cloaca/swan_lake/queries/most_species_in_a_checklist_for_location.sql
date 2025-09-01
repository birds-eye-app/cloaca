with d as (
    select
        "SAMPLING EVENT IDENTIFIER",
        "DURATION MINUTES",
        "EFFORT DISTANCE KM",
        "OBSERVATION DATE",
        "OBSERVER ID",
        "GROUP IDENTIFIER",
        count(distinct "SCIENTIFIC NAME") as number_of_unique_species,
        array_agg("COMMON NAME") as species_list
    from
        ebird_ny.ebird_ny
    where
        "LOCALITY ID" = 'L1293732'
        and "ALL SPECIES REPORTED" = 1
        and category not in ('slash', 'spuh')
        and APPROVED = 1 -- no escapees here!
        and (
            "EXOTIC CODE" is null
            or "EXOTIC CODE" = 'N'
        )
        and "PROJECT CODE" = 'EBIRD'
    group by
        1,
        2,
        3,
        4,
        5,
        6
)
select
    "GROUP IDENTIFIER",
    d."OBSERVATION DATE",
    d.number_of_unique_species,
    array_agg("SAMPLING EVENT IDENTIFIER")
from
    d,
    (
        select
            "OBSERVATION DATE",
            max(number_of_unique_species) number_of_unique_species
        from
            d
        group by
            1
    ) as l
where
    d."OBSERVATION DATE" = l."OBSERVATION DATE"
    and d.number_of_unique_species = l.number_of_unique_species
group by
    1,
    2,
    3
order by
    d.number_of_unique_species desc