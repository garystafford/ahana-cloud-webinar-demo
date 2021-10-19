-- example of inner join using pushdown predicates
SELECT
        artists.nationality,
        artwork.classification,
        artists.name,
        artwork.date,
        artwork.title
    FROM
        artists_refined_glue AS artists
    INNER JOIN
        artwork_refined_glue AS artwork
    USING (artist_id)
    WHERE
        artists.nationality = 'American'
        AND artwork.classification = 'Photograph'
    ORDER BY
        artists.name,
        artwork.title,
        artwork.date;