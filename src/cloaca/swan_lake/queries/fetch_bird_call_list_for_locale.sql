with input_data as (
    select
        distinct -- distinct is here to exclude duplicate (shared) checklists
        extract (
            month
            from
                observation_date
        ) as month,
        locality_id,
        sampling_event_identifier as checklist_id,
        common_name as common_name
    from
        ebd_sorted
    where
        observation_date > current_date - interval '5 year'
        and category = 'species'
        and protocol_name in ('Stationary', 'Traveling')
        and effort_distance_km < 10
        and locality_id = 'L2987624'
        and approved
        and (
            exotic_code is null
            or exotic_code not in ('X')
        )
),
total_checklists as (
    select
        month,
        locality_id,
        count(distinct checklist_id) as total_checklists
    from
        input_data
    group by
        1,
        2
),
species_checklists as (
    select
        month,
        locality_id,
        common_name,
        count(distinct checklist_id) as species_checklists
    from
        input_data
    group by
        1,
        2,
        3
),
d as (
    select
        locality_id,
        common_name,
        max(species_checklists),
        max(total_checklists),
        max(species_checklists / total_checklists) as max_presence_rate,
        sum(species_checklists) as total_appearances
    from
        total_checklists
        join species_checklists using (locality_id, month)
    group by
        1,
        2
    order by
        total_appearances desc
)
select
    common_name,
    SPECIES_CODE as species_code,
    "FAMILY" as family,
    SPECIES_GROUP as species_group,
    total_appearances,
    row_number() over (
        order by
            total_appearances desc
    ) as total_appearances_rank
from
    d
    join taxonomy on common_name = taxonomy.PRIMARY_COM_NAME
order by
    total_appearances desc