-- use superset/presto and apache hive to create tables

-- bronze / existing raw csv data
CREATE TABLE IF NOT artists_raw_hive
(
    artist_id   VARCHAR,
    name        VARCHAR,
    nationality VARCHAR,
    gender      VARCHAR,
    birth_year  VARCHAR,
    death_year  VARCHAR
)
WITH (FORMAT = 'CSV',
    CSV_SEPARATOR = '|',
    CSV_QUOTE = '"',
    EXTERNAL_LOCATION = 's3a://<your_s3_bucket_name_here>/artists_raw/');

-- silver / create refined partitioned parquet data
CREATE TABLE IF NOT EXISTS artists_refined_hive
WITH (
    FORMAT = 'PARQUET',
    PARTITIONED_BY = ARRAY ['nationality'])
AS
SELECT cast(artist_id AS INTEGER) AS artist_id,
    name,
    CASE
       WHEN gender = ''
           THEN NULL
       ELSE gender
    END AS gender,
    try_cast(birth_year AS INTEGER) AS birth_year,
    try_cast(death_year AS INTEGER) AS death_year,
    CASE
       WHEN nationality IN (NULL, '', 'Nationality unknown', 'nationality unknown')
           THEN 'Nationality Unknown'
       ELSE nationality
    END AS nationality
FROM artists_raw_hive
WHERE artist_id != 'artist_id'
  AND name != '';

SHOW COLUMNS IN artists_refined_hive;

-- analyze refined data (hive)
SELECT nationality, count(*) AS artists
FROM artists_refined_hive
GROUP BY nationality
ORDER BY artists DESC;

-- analyze refined data (aws glue)
SELECT classification,
    count(*) AS pieces,
    min(try_cast(date AS INTEGER)) AS min_date,
    year(current_date) - min(try_cast(date AS INTEGER)) AS oldest_yrs,
    max(try_cast(date AS INTEGER)) AS max_date,
    year(current_date) - max(try_cast(date AS INTEGER)) AS newest_yrs,
    cast (avg(try_cast(date AS INTEGER)) AS INTEGER) AS avg_date
FROM artwork_refined_glue
GROUP BY classification
ORDER BY pieces DESC;
