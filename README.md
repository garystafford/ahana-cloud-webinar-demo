# 0 to Presto in 30 minutes with AWS & Ahana Cloud

Source SQL code for the Ahana.io
webinar: [0 to Presto in 30 minutes with AWS & Ahana Cloud](https://ahana.io/events/webinars/0-to-presto-oct21/)

## Datasource

The original datasource use in the
demonstration: [Museum of Modern Art Collection](https://www.kaggle.com/momanyc/museum-collection).

## SQL Statements

* [`ahana_demo_glue_artists.sql`](ahana_demo_glue_artists.sql): Artists SQL statements using AWS Glue and Amazon Athena
* [`ahana_demo_glue_artworks.sql`](ahana_demo_glue_artworks.sql): Artworks SQL statements using AWS Glue and Amazon Athena
* [`ahana_demo_hive.sql`](ahana_demo_hive.sql): Artists SQL statements using Apache Superset/Presto and Apache Hive
* [`joins.sql`](joins.sql): Example of joins using Apache Superset/Presto
* [`superset_charts.sql`](superset_charts.sql): SQL statements used to create Superset charts for dashboard
   
## Data
* `moma_public_artists.txt.gz`: GZIP-compressed, pipe-delimited raw artists data from Kaggle
* `moma_public_artworks.txt.gz`: GZIP-compressed, pipe-delimited raw artwork data from Kaggle

## Setup

1. Replace `<your_s3_bucket_name_here>` in the source code with the actual name of your Amazon S3 bucket;
2. Upload both GZIP-compressed CSV files to Amazon S3, do not uncompress;
3. Create an AWS Glue Data Catalog, named `moma`;
4. Create a Data Source in the Ahana SaaS Console for the AWS Glue Data Catalog, `moma`, and attach to your Ahana Cloud
   cluster;

---

<i>The contents of this repository represent my viewpoints and not of my past or current employers, including Amazon Web
Services (AWS). All third-party libraries, modules, plugins, and SDKs are the property of their respective owners.</i>