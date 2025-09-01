-- This SQL query identifies the best locations for observing Eastern Phoebes in March, based on the percentage of observations relative to total observations at each locality.
with base as (
    select
        date_part('month', "OBSERVATION DATE") as month,
        "COMMON NAME",
        "SAMPLING EVENT IDENTIFIER",
        "LOCALITY"
    from
        ebd.full
    where
        approved = 1
        and category not in ('slash', 'spuh')
        and "ALL SPECIES REPORTED" = 1
),
s as (
    select
        month,
        "COMMON NAME",
        "LOCALITY",
        count(distinct "SAMPLING EVENT IDENTIFIER") as observations
    from
        base
    group by
        1,
        2,
        3
),
t as (
    select
        month,
        "LOCALITY",
        count(distinct "SAMPLING EVENT IDENTIFIER") as total_observations
    from
        base
    group by
        1,
        2
)
select
    *,
    100.00 * observations / total_observations as percentage
from
    s
    join t using (month, "LOCALITY")
where
    "COMMON NAME" = 'Eastern Phoebe'
    and month = 3
    and total_observations > 100
order by
    percentage desc
limit
    1000