select
    "SAMPLING EVENT IDENTIFIER",
    country,
    state,
    "OBSERVATION DATE",
    count(distinct "SCIENTIFIC NAME") as no_of_ducks_seen,
    array_agg("COMMON NAME"),
    array_agg("OBSERVATION DATE")
from
    ebd.full
where
    "COMMON NAME" ilike '%duck%'
    and category not in ('slash', 'spuh')
    and APPROVED = 1 -- no escapees here!
    and (
        "EXOTIC CODE" is null
        or "EXOTIC CODE" = 'N'
    )
group by
    1,
    2,
    3,
    4
order by
    no_of_ducks_seen desc
limit
    100