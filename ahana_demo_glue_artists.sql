-- use aws glue and amazon athena to create tables

-- create aws glue database
CREATE SCHEMA IF NOT EXISTS moma;

-- bronze / existing raw csv data
CREATE EXTERNAL TABLE IF NOT EXISTS `artists_raw_glue`(
    `artist_id` bigint,
    `name` string,
    `nationality` string,
    `gender` string,
    `birth_year` string,
    `death_year` string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'escapeChar'='\\',
    'quoteChar'='\"',
    'separatorChar'='|')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://<your_s3_bucket_name_here>/artists_raw'
TBLPROPERTIES (
  'classification'='csv',
  'compressionType'='gzip',
  'skip.header.line.count'='1')

-- preview bronze data
SELECT * FROM "moma"."artists_raw_glue" LIMIT 10;

-- silver / create refined partitioned parquet data
CREATE TABLE IF NOT EXISTS artists_refined_glue
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['nationality'],
    external_location = 's3://<your_s3_bucket_name_here>/artists_refined_glue/')
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
FROM artists_raw_glue
WHERE name != ''
LIMIT 7500;

-- because data contains over 100 partitions
INSERT INTO artists_refined_glue
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
FROM artists_raw_glue
WHERE name != ''
OFFSET 7500;

-- show parquet-format file partitions in S3
SHOW PARTITIONS artists_refined_glue;

-- gold / analyze refined data (aws glue)
CREATE TABLE artists_totals
WITH (
    format = 'TEXTFILE',
    external_location = 's3://<your_s3_bucket_name_here>/artists_totals/',
    field_delimiter = ',')
AS
SELECT nationality, count(*) AS artists
FROM artists_refined_glue
GROUP BY nationality
ORDER BY artists DESC
LIMIT 10;

-- analyze refined data (aws glue)
SELECT name, nationality, gender, birth_year, death_year,
    (death_year - birth_year) AS length_life
FROM artists_refined_glue
WHERE nationality = 'American'
    AND gender IS NOT NULL
ORDER BY name
LIMIT 25;
