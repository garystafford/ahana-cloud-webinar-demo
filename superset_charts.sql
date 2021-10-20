-- superset chart queries
SELECT nationality, COUNT(*) AS artists
FROM artists_refined_glue
GROUP BY nationality
ORDER BY artists DESC;

SELECT classification,
    count(*) AS pieces,
    min(try_cast(date AS INTEGER)) AS min_date,
    max(try_cast(date AS INTEGER)) AS max_date,
    cast(avg(try_cast(date AS INTEGER)) AS INTEGER) AS avg_date
FROM artworks_refined_glue
GROUP BY classification
ORDER BY pieces DESC;