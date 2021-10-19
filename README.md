# 0 to Presto in 30 minutes with AWS & Ahana Cloud

Source SQL code for the Ahana.io
webinar: [0 to Presto in 30 minutes with AWS & Ahana Cloud](https://ahana.io/events/webinars/0-to-presto-oct21/)

## Datasource

The original datasource use in the
demonstration: [Museum of Modern Art Collection](https://www.kaggle.com/momanyc/museum-collection).

## Files

* `ahana_demo_glue_artists.sql`: Artists SQL statements using AWS Glue and Amazon Athena
* `ahana_demo_glue_artwork.sql`: Artwork SQL statements using AWS Glue and Amazon Athena
* `ahana_demo_hive.sql`: Artists SQL statements using Apache Superset/Presto and Apache Hive
* `joins.sql`: Example of joins using Apache Superset/Presto
* `moma_public_artists.txt.zip`: ZIP-compressed, pipe-delimited raw artists data from Kaggle
* `moma_public_artworks.txt.zip`: ZIP-compressed, pipe-delimited raw artwork data from Kaggle
* `superset_charts.sql`: SQL statements used to create Superset charts for dashboard

## Setup

1. Replace `<your_s3_bucket_name_here>` in the source code with the actual name of your Amazon S3 bucket;
2. Create an AWS Glue Data Catalog, named `moma`;
3. Create a Data Source in the Ahana SaaS Console for the AWS Glue Data Catalog, `moma`, and attach to your Ahana Cloud
   cluster;

---

<i>The contents of this repository represent my viewpoints and not of my past or current employers, including Amazon Web
Services (AWS). All third-party libraries, modules, plugins, and SDKs are the property of their respective owners.</i>