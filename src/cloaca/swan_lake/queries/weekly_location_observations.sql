create table parsed_ebd.weekly_species_observations as (
    select
        coalesce("SUBSPECIES SCIENTIFIC NAME", "SCIENTIFIC NAME") as species_id,
        date_trunc('week', "OBSERVATION DATE") as week,
        "LOCALITY ID" as locality_id,
        "EXOTIC CODE" as exotic_code,
        count(*) as number_of_checklists -- sum("OBSERVATION COUNT") as number_of_observations
    from
        ebd_full."full"
    where
        "OBSERVATION DATE" > current_date - interval '5 years'
    group by
        1,
        2,
        3,
        4
)