-- in case you have the txt or .gz file directly
create
or replace TABLE ebd_sorted AS
SELECT
    "GLOBAL UNIQUE IDENTIFIER" as global_unique_identifier,
    "CATEGORY" as category,
    "COMMON NAME" as common_name,
    "SCIENTIFIC NAME" as scientific_name,
    "SUBSPECIES COMMON NAME" as subspecies_common_name,
    "SUBSPECIES SCIENTIFIC NAME" as subspecies_scientific_name,
    "EXOTIC CODE" as exotic_code,
    "OBSERVATION COUNT" as observation_count,
    "COUNTRY CODE" as country_code,
    "STATE CODE" as state_code,
    "COUNTY CODE" as county_code,
    "LOCALITY" as locality,
    "LOCALITY ID" as locality_id,
    "LOCALITY TYPE" as locality_type,
    "LATITUDE" as latitude,
    "LONGITUDE" as longitude,
    "OBSERVATION DATE" as observation_date,
    "SAMPLING EVENT IDENTIFIER" as sampling_event_identifier,
    "PROTOCOL NAME" as protocol_name,
    "DURATION MINUTES" as duration_minutes,
    "EFFORT DISTANCE KM" as effort_distance_km,
    "NUMBER OBSERVERS" as number_observers,
    cast("ALL SPECIES REPORTED" as boolean) as all_species_reported,
    "GROUP IDENTIFIER" as group_identifier,
    cast("APPROVED" as boolean) as approved,
    cast("REVIEWED" as boolean) as reviewed
FROM
    read_csv(
        '/Users/davidmeadows_1/Downloads/ebd_US-NY_smp_relJul-2025.txt',
        store_rejects = true,
        quote = ''
    )
ORDER BY
    observation_date;