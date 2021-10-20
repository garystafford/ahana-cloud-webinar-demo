-- use aws glue and amazon athena to create tables

-- bronze / existing raw csv data
CREATE EXTERNAL TABLE `artwork_raw_glue`(
  `artwork_id` bigint,
  `title` string, 
  `artist_id` bigint,
  `name` string, 
  `date` string, 
  `medium` string, 
  `dimensions` string, 
  `acquisition date` string, 
  `credit` string, 
  `catalogue` string, 
  `department` string, 
  `classification` string, 
  `object number` string, 
  `diameter_cm` string, 
  `circumference_cm` string, 
  `height_cm` string, 
  `length_cm` string, 
  `width_cm` string, 
  `depth_cm` string, 
  `weight_kg` string, 
  `durations` string)
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
  's3://<your_s3_bucket_name_here>/artwork_raw'
TBLPROPERTIES (
  'skip.header.line.count'='1',
  'classification'='csv',
  'compressionType'='none');

-- silver / create refined partitioned parquet data
CREATE EXTERNAL TABLE artwork_refined_glue
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY ['classification'],
    external_location = 's3://<your_s3_bucket_name_here>/artwork_refined_glue/')
AS
SELECT cast(artwork_id AS INTEGER) AS artwork_id,
       cast(artist_id AS INTEGER)  AS artist_id,
       title,
       CASE
           WHEN date = '' THEN NULL ELSE date
       END AS date,
       CASE
           WHEN medium = '' THEN NULL ELSE medium
       END AS medium,
       CASE
           WHEN catalogue = '' THEN NULL ELSE catalogue
       END AS catalogue,
       CASE
           WHEN department = '' THEN NULL ELSE department
       END AS department,
       CASE
           WHEN classification IN (NULL, '', '(not assigned)')
               THEN 'Not Assigned'
           WHEN classification IN ('Photography Research/Reference')
               THEN 'Photography Research and Reference'
           ELSE classification
END
AS classification
FROM artwork_raw_glue
WHERE title != '';

-- analyze refined data (aws glue)
SELECT classification, count(*) AS pieces
FROM artwork_refined_glue
GROUP BY classification
ORDER BY pieces DESC;

SELECT classification,
    count(*) AS pieces,
    min(try_cast(date AS INTEGER)) AS min_date,
    year(current_date) - min(try_cast(date AS INTEGER)) AS oldest_yrs,
    max(try_cast(date AS INTEGER)) AS max_date,
    year(current_date) - max(try_cast(date AS INTEGER)) AS newest_yrs,
    cast (avg(try_cast(date AS INTEGER)) AS INTEGER) AS avg_date
FROM artwork_refined_glue
GROUP BY classification
ORDER BY pieces DESC
LIMIT 10;
