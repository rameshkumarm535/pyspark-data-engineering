# Data Pipeline Architecture

Source → Kafka → S3 (Raw) → Glue ETL → Delta Lake → Athena → BI Dashboard

Components:
- AWS S3 (Storage)
- AWS Glue (ETL)
- Delta Lake (ACID Layer)
- Spark (Processing)
- Athena (Query)
