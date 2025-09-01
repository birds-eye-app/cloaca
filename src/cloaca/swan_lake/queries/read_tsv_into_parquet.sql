-- This SQL script reads a TSV file and writes it into a Parquet file with partitioning by year and month.
COPY (
    select
        *,
        date_trunc('year', "OBSERVATION DATE") as year,
        date_trunc('month', "OBSERVATION DATE") as month
    from
        read_csv(
            'ebd_US-NY_smp_relJan-2025.tsv',
            store_rejects = true,
            quote = ''
        )
) TO 'partitioned_ebird_full' (
    FORMAT parquet,
    PARTITION_BY (year, month)
);

-- read that parquet file back into a table
create table ebird_ny as
select
    *
FROM
    read_parquet(
        'partitioned_ebird_ny/*/*/*.parquet',
        hive_partitioning = true
    );