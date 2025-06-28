# BigQuery-to-AzureBlob

Use Azure Data Factory to export and sink BigQuery Tables to Azure Blob

## Export table data from CSV and save as JSON

Use this example query to generate each table name, creation/updated timestamp, and table size. This will be used later by the script to create the Azure Data Factory datasets and pipelines. Make sure to update your project name and dataset name first.

```sql
SELECT
  table_id,
  TIMESTAMP_MILLIS(creation_time) AS creation_timestamp,
  TIMESTAMP_MILLIS(last_modified_time) AS last_modified_timestamp,
  row_count,
  size_bytes / 1024 / 1024 AS size_mb
FROM `my_project_name.my_dataset_name.__TABLES__`
ORDER BY last_modified_timestamp DESC;
```
