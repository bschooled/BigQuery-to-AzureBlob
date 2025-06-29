# BigQuery-to-AzureBlob

Use Azure Data Factory to export and sink BigQuery Tables to Azure Blob

## 1. Export table data from CSV and save as JSON

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

After successfully running the query use BigQuery "Save Results" -> CSV(Local File) option. You will need this later.

## 2. Cloning Repo into Azure Cloud Shell

This script was designed to be easily executed directly from Azure Cloud Shell.

From portal.azure.com in the top bar, to the right of the search bar find the cloud shell icon and select to open a cloud shell. If it prompts you for an Azure Storage Account for persistent storage you can skip this step and use ephemeral instead.

Now that we have the Cloud Shell opened, you will first need to clone the repository:

```powershell
git clone https://github.com/bschooled/BigQuery-to-AzureBlob.git
cd .\Bigquery-to-AzureBlob
```

## 3. Upload the CSV to the CloudShell

At the top of the Cloud Shell pane, there is a "Manage Files" option. Select this and choose "Upload File" and choose your exported CSV from step 1.

## 4. Run Powershell script with parameters

Now that we have the required CSV uploaded, you will need to run the script with a number of parameters

You can either run the command like below and replace the text;

```powershell
.\New-BigQueryToADFPipeline.ps1 -ResourceGroupName "resource_group_name" ` 
    -StorageAccountName "storage_account_name" `
    -AzureRegion "Azure_deployment_region" `
    -DataFactoryName "data_factory_name" `
    -BQDatasetID "your_BQ_dataset_Name" `
    -BQProjectID "project_name_containing_dataset" `
    -OutputFormat "json_or_parquet" `
    -CSVFile "path_to_exported_tables_csv"
```

Or use the params.txt and pass it to the powershell script;

```powershell
$parameters = Get-Content -Path "params.txt"
.\New-BigQueryToADFPipeline.ps1 @parameters
```


## 5. Setup Linked Services (Optional if existing)

### a. Create Service Account and download key
