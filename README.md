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
$parameters = Import-PowerShellDataFile -Path "params.psd1"
.\New-BigQueryToADFPipeline.ps1 @parameters
```

## 5. Setup Linked Services (Optional if existing)

If you received a message from the script that it could not find a linked service for either Blob or BigQuery you will need to take additional steps to continue.

### BigQuery Link Service Missing: Create Service Account in GCP and download key

From the Google Cloud Console, open a cloud shell.

Once opened you will need to run the following commands.

1. Create the Service Account

  ```bash
  gcloud iam service-accounts create adf-bq-sa \
    --description="ADF BigQuery access" \
    --display-name="ADF BigQuery SA"
  ```

  2. Add IAM Permissions to the Service Account. Replace "YOUR-GCP-PROJECT" with your project ID.

  ```bash
  gcloud projects add-iam-policy-binding YOUR-GCP-PROJECT \
    --member=serviceAccount:adf-bq-sa@YOUR-GCP-PROJECT.iam.gserviceaccount.com \
    --role=roles/bigquery.jobUser

  gcloud projects add-iam-policy-binding YOUR-GCP-PROJECT \
    --member=serviceAccount:adf-bq-sa@YOUR-GCP-PROJECT.iam.gserviceaccount.com \
    --role=roles/bigquery.dataViewer
  ```

  3. Create and export the key to JSON

  ```bash
  gcloud iam service-accounts keys create key.json \
    --iam-account=adf-bq-sa@YOUR-GCP-PROJECT.iam.gserviceaccount.com
  ```

  4. Download the key from Cloud Shell

  From the Cloud Shell panel, click the three dots next to session information and select "Download File". In the path add key.json to the end. Make sure to only keep this key locally until after we have successfully uploaded to the linked service and then delete it.

#### Create Linked Service in Data Factory Studio

    1. From portal.azure.com search "Data Factories" and open.
    2. Choose the Data Factory name that was created by the script (or a prior created one you specified)
    3. In the Overview panel, click "Launch Studio"
    4. In the left side panel, click "Manage"
    5. In the new flyout panel, click "Linked Services", and then click "New".
    6. In the search panel, search "BigQuery" and select "Google BigQuery" and continue.
    7. Name the linked service and note the name as you will need to input this in the script.
    8. You will need to input your Google Project ID, change "Authentication Type" to "Service Authentication" and upload the key.json you created earlier. 
    9. Once done click "Create".
    10. To validate, click the newly created Linked Service, and in the bottom right select "Test Connection". Ensure the connection is successful before continuing.

### Azure Storage Linked Service Missing

Note that the script will automatically try to create this linked service for you. If for some reason it fails then here are the manual instructions.

#### Assign IAM permissions to System-managed Identity

Open a new portal.azure.com tab, and open Cloud shell (powershell)

Get the system-managed identity

```powershell
$rg = "your_rg_name"
$adfName = "your_data_factory_name"
$saName = "your_storage_account_name"

$adf = Get-AzDataFactoryV2 -ResourceGroupName $rg -DataFactoryName $adfName
$adfIdentity = $adf.Identity.PrincipalId
```

Assign the permissions

```powershell
New-AzRoleAssignment `
  -ObjectId $adfIdentity `
  -RoleDefinitionName "Storage Blob Data Contributor" `
  -Scope "/subscriptions/$((Get-AzContext).Subscription.Id)/resourceGroups/$rg/providers/Microsoft.Storage/storageAccounts/$saName"
```

#### Create the linked service

    1. From portal.azure.com search "Data Factories" and open.
    2. Choose the Data Factory name that was created by the script (or a prior created one you specified)
    3. In the Overview panel, click "Launch Studio"
    4. In the left side panel, click "Manage"
    5. In the new flyout panel, click "Linked Services", and then click "New".
    6. In the search panel, search "Blob" and select "Azure Blob Storage" and continue.
    7. Name the linked service and note the name as you will need to input this in the script.
    8. Change authentication type to "System-assigned managed identity"
    9. Select your Azure Subscription and Storage account name from the drop downs.
    10. Choose create.
    11. Open the newly created linked service and select Test Connection before proceeding
