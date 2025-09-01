create TABLE ebd.full AS
SELECT
    *
FROM
    read_csv(
        '/Users/davidmeadows_1/Downloads/ebd_US-NY-061_relJul-2025/ebd_US-NY-061_relJul-2025.txt.gz',
        store_rejects = true,
        quote = ''
    );