# Prereqs:
#  - Az PowerShell module installed & logged in (Connect-AzAccount)
# —— CONFIG —— #
param(
    [Parameter(Mandatory = $true, HelpMessage = "Azure Resource Group name.")]
    [string]$ResourceGroupName,

    [Parameter(Mandatory = $true, HelpMessage = "Globally unique Azure Storage Account name.")]
    [string]$StorageAccountName,

    [Parameter(Mandatory = $true, HelpMessage = "Azure region/location (e.g., eastus).")]
    [string]$AzureRegion,

    [Parameter(Mandatory = $true, HelpMessage = "Azure Data Factory name.")]
    [string]$DataFactoryName,

    [Parameter(Mandatory = $true, HelpMessage = "Path to CSV file with BigQuery table metadata.")]
    [string]$CSVFile,

    [Parameter(Mandatory = $true, HelpMessage = "Output format: 'json' or 'parquet'.")]
    [ValidateSet("json", "parquet")]
    [string]$OutputFormat,

    [Parameter(Mandatory = $true, HelpMessage = "Dataset ID for the BigQuery tables.")]
    [string]$BQDatasetID,

    [Parameter(Mandatory = $true, HelpMessage = "BigQuery project ID.")]
    [string]$BQProjectID
)

# Ensure Az PowerShell module is installed
if (-not $(Get-Module -Name Az -ErrorAction SilentlyContinue)) {
    Write-Host "Az module not found. Installing..." -ForegroundColor Yellow
    Install-Module -Name Az -AllowClobber -Force -SkipPublisherCheck -Scope CurrentUser
}

# Ensure Az powerhsell module is authenticated
if (-not $(Get-AzContext -ErrorAction SilentlyContinue)) {
    Write-Host "You are not logged in to Azure. Please log in using Connect-AzAccount." -ForegroundColor Red
    Connect-AzAccount -UseDeviceAuthentication
}

# Check if resource group exists, if not create it
if (-not $(Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction SilentlyContinue)) {
    Write-Host "Resource group '$ResourceGroupName' does not exist. Please create it first." -ForegroundColor Red
    # 1) CREATE RESOURCE GROUP
    New-AzResourceGroup -Name $ResourceGroupName -Location $AzureRegion -Force
}

# Check if storage account exists, if not create it
if (-not $(Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $StorageAccountName -ErrorAction SilentlyContinue)) {
    Write-Host "Storage account '$StorageAccountName' does not exist. Creating it now..." -ForegroundColor Yellow

    try {
        New-AzStorageAccount `
            -ResourceGroupName $ResourceGroupName `
            -Name $StorageAccountName `
            -Location $AzureRegion `
            -SkuName Standard_LRS `
            -Kind StorageV2 `
            -EnableHttpsTrafficOnly $true

        # Deny all networks except trusted Azure services (Bypass=AzureServices)
        Update-AzStorageAccountNetworkRuleSet `
            -ResourceGroupName $ResourceGroupName `
            -Name $StorageAccountName `
            -DefaultAction Deny `
            -Bypass AzureServices
    }
    catch {
        Write-Host "An error occurred while creating the storage account: $_" -ForegroundColor Red
        exit 1
    }

}
else {
    Write-Host "Storage account '$StorageAccountName' already exists." -ForegroundColor Green
    try {
        # Deny all networks except trusted Azure services (Bypass=AzureServices)
        Update-AzStorageAccountNetworkRuleSet `
            -ResourceGroupName $ResourceGroupName `
            -Name $StorageAccountName `
            -DefaultAction Deny `
            -Bypass AzureServices
    }
    catch {
        Write-Host "An error occurred while updating the storage account network rules: $_" -ForegroundColor Red
        exit 1
    }
}

if (-not $(Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $DataFactoryName -ErrorAction SilentlyContinue)) {
    Write-Host "Data Factory '$DataFactoryName' does not exist. Creating it now..." -ForegroundColor Yellow

    try {
        New-AzDataFactoryV2 `
            -ResourceGroupName $ResourceGroupName `
            -Name $DataFactoryName `
            -Location $AzureRegion `
            -IdentityType SystemAssigned
    }
    catch {
        Write-Host "An error occurred while creating the Data Factory: $_" -ForegroundColor Red
        exit 1
    }
}
else {
    Write-Host "Data Factory '$DataFactoryName' already exists, ensuring managed identity is set up..." -ForegroundColor 
    Set-AzDataFactoryV2 `
        -ResourceGroupName $ResourceGroupName `
        -Name $DataFactoryName `
        -IdentityType SystemAssigned
}

if (-not $(Test-Path -Path $CSVFile)) {
    Write-Host "CSV file '$CSVFile' does not exist. Please provide a valid path." -ForegroundColor Red
    Write-Host "If running from Cloud Shell, click the 'Upload/Download files' icon to upload your CSV file.`n" -ForegroundColor Yellow
    $CSVFile = Read-Host "Enter the path to your BigQuery tables CSV file"
}

$tables = Import-Csv -Path $CSVFile
if (-not $tables.table_Id) {
    Write-Host "CSV file does not contain 'table_Id' column. Please ensure the CSV is formatted correctly." -ForegroundColor Red
    exit 1
}

# 3) CREATE CONTAINERS per table 📦 using Entra ID authentication
$ctx = New-AzStorageContext -StorageAccountName $StorageAccountName -UseConnectedAccount

$tableList = foreach ($t in $tables) {
    $cname = $t.table_Id
    $container = Get-AzStorageContainer -Name $cname -Context $ctx -ErrorAction SilentlyContinue
    if (-not $container) {
        Write-Host "Creating container: $cname"
        New-AzStorageContainer -Name $cname -Context $ctx | Out-Null
    }
    else {
        Write-Host "Container '$cname' already exists." -ForegroundColor Green
    }

    [PSCustomObject]@{
        tableName     = $cname
        bqDatasetId   = $BQDatasetID
        containerName = $cname
        blobFileName  = "$cname"                  # file extension and timestamp are set dynamically in the ADF pipeline
        outputFormat  = $OutputFormat
    }
}

#── 4. Pre-req variables ────────────────────────────────────────────────
# (assumes $rg, $dataFactory, $saName, $ctx already defined)
# Linked services already exist:
#   • GoogleBigQueryLinkedService
#   • AzureBlobStorageLinkedService
# 4) CREATE LINKED SERVICES

# setup the Azure Blob Storage linked service
$adf = Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName
$adfIdentity = $adf.Identity.PrincipalId

# Assign Storage Blob Data Contributor role
New-AzRoleAssignment `
  -ObjectId $adfIdentity `
  -RoleDefinitionName "Storage Blob Data Contributor" `
  -Scope "/subscriptions/$((Get-AzContext).Subscription.Id)/resourceGroups/$ResourceGroupName/providers/Microsoft.Storage/storageAccounts/$StorageAccountName"

# get the blob service endpoint
$blobEndpoint = (Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $StorageAccountName).PrimaryEndpoints.Blob

# Create the Azure Blob Storage linked service using system-assigned managed identity
$blobStorageLs = @{
    name       = "AzureBlobStorageLinkedService"
    properties = @{
        type           = "AzureBlobStorage"
        typeProperties = @{
            serviceEndpoint = $blobEndpoint
            authentication = "ManagedIdentity"
        }
    }
}

# Deploy the linked service if it doesn't exist
$existingBlobLs = Get-AzDataFactoryV2LinkedService -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $blobStorageLs.name -ErrorAction SilentlyContinue
$blobStorageLsObj = ($blobStorageLs | ConvertTo-Json -Depth 100) | ConvertFrom-Json
if (-not $existingBlobLs) {
    Set-AzDataFactoryV2LinkedService -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $blobStorageLs.name -Definition $blobStorageLsObj
}

$allLs = Get-AzDataFactoryV2LinkedService `
    -ResourceGroupName $ResourceGroupName `
    -DataFactoryName    $DataFactoryName

# Find the BigQuery linked service (type: GoogleBigQueryV2 or GoogleBigQuery)
$bigQueryLsName = ($allLs | Where-Object {
        $_.Properties.Type -match 'BigQuery'
    }).Name

if (-not $bigQueryLsName){
    $i = 0
    While (-not $bigQueryLsName -and $i -lt 3) {
        Write-Host "No BigQuery linked service found." -ForegroundColor Red
        Write-Host "Please create one in Data Factory first.`n
        You can follow the instructions in the README to setup, the script will wait for you to complete" -ForegroundColor Yellow
        $bigQueryLsName = Read-Host "Enter the name of the BigQuery linked service to use"
        $bigQueryLsName = ($allLs | Where-Object {
            $_.Properties.Type -match 'BigQuery'
        }).Name
        $i++
    }
    if (-not $bigQueryLsName) {
        Write-Host "No BigQuery linked service found after 3 attempts. Exiting." -ForegroundColor Red
        exit 1
    }
}
elseif ($bigQueryLsName.Count -gt 1) {
    Write-Host "Multiple BigQuery linked services found" -ForegroundColor Yellow
    $matchBQName = Read-Host "Please specify the BigQuery linked service name to use"
    $bigQueryLsName = $matchBQName
}

# Find the Azure Blob Storage linked service
$blobStorageLsName = ($allLs | Where-Object {
        $_.Properties.Type -match 'AzureBlobStorage'
    }).Name

if (-not $blobStorageLsName) {
    $i = 0
    While (-not $blobStorageLsName -and $i -lt 3) {
        Write-Host "No Azure Blob Storage linked service found" -ForegroundColor Red
        Write-Host "Please create an Azure Blob Storage linked service in Data Factory first.`n
        You can follow the instructions in the README to setup, the script will wait for you to complete" -ForegroundColor Yellow
        $blobStorageLsName = Read-Host "Enter the name of the Azure Blob Storage linked service to use"
        # Find the Azure Blob Storage linked service
        $blobStorageLsName = ($allLs | Where-Object {
                $_.Properties.Type -match 'AzureBlobStorage'
            }).Name
        $i++
    }
    if(-not $blobStorageLsName) {
        Write-Host "No Azure Blob Storage linked service found after 3 attempts. Exiting." -ForegroundColor Red
        exit 1
    }
}
elseif ($blobStorageLsName.Count -gt 1) {
    Write-Host "Multiple Azure Blob Storage linked services found" -ForegroundColor Yellow
    $matchBlobName = Read-Host "Please specify the Blob Storage linked service name to use"
    $matchBlobName = Read-Host "Please specify the Blob Storage linked service name to use"
    $blobStorageLsName = $matchBlobName
}

Write-Host "Using BigQuery LS:     $bigQueryLsName"
Write-Host "Using BlobStorage LS:  $blobStorageLsName"


#── 4.1. BUILD & DEPLOY BIGQUERY SOURCE DATASET ─────────────────────────
$bqDs = @{
    name       = "BigQueryDataset"
    properties = @{
        type              = "GoogleBigQuery"
        linkedServiceName = @{ referenceName = $bigQueryLsName; type = "LinkedServiceReference" }
        parameters        = @{
            datasetName = @{ type = "String" }
            tableName   = @{ type = "String" }
        }
        typeProperties    = @{
            tableName = "@concat(dataset().datasetName, '.', dataset().tableName)"
        }
    }
}

# Deploy BigQuery dataset
try {
    Write-Host "Deploying BigQuery dataset: $($bqDs.name)" -ForegroundColor Green
    $bqObj = ($bqDs | ConvertTo-Json -Depth 100) | ConvertFrom-Json
    Set-AzDataFactoryV2Dataset `
        -ResourceGroupName $ResourceGroupName `
        -DataFactoryName    $DataFactoryName `
        -Name               $bqDs.name `
        -Definition         $bqObj
}
catch {
    Write-Host "Failed to deploy BigQuery dataset: $($bqDs.name)" -ForegroundColor Red
    Write-Host "Error details: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}


#── 4.2. BUILD & DEPLOY BLOB SINK DATASET (JSON) ────────────────────────
#── 4.2. BUILD & DEPLOY BLOB SINK DATASET (JSON) ────────────────────────
if ($OutputFormat -ieq "json") {
    $jsonDs = @{
        properties = @{
            type              = "Json"
            linkedServiceName = @{ referenceName = $blobStorageLsName; type = "LinkedServiceReference" }
            parameters        = @{
                containerName = @{ type = "String" }
                blobFileName  = @{ type = "String" }
            }
            typeProperties    = @{
                location    = @{
                    type      = "AzureBlobStorageLocation"
                    container = "@dataset().containerName"
                    blobPath  = "@dataset().blobFileName"
                }
                compression = @{
                    type  = "Gzip"
                    level = "Optimal"
                }
            }
            format            = @{
                type = "JsonFormat"
            }
        }
    }

    # Deploy JSON sink dataset
    try {
        $jsonObj = ($jsonDs | ConvertTo-Json -Depth 100) | ConvertFrom-Json
        Set-AzDataFactoryV2Dataset `
            -ResourceGroupName $ResourceGroupName `
            -DataFactoryName    $DataFactoryName `
            -Name               $jsonDs.name `
            -Definition         $jsonObj
    }
    catch {
        Write-Host "Failed to deploy JSON sink dataset: $($jsonDs.name)" -ForegroundColor Red
        Write-Host "Error details: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }

}

#── 4.3. BUILD & DEPLOY BLOB SINK DATASET (PARQUET) ─────────────────────
#── 4.3. BUILD & DEPLOY BLOB SINK DATASET (PARQUET) ─────────────────────
if ($OutputFormat -ieq "parquet") {
    $parquetDs = @{
        properties = @{
            type              = "Parquet"
            linkedServiceName = @{ referenceName = $blobStorageLsName; type = "LinkedServiceReference" }
            parameters        = @{
                containerName = @{ type = "String" }
                blobFileName  = @{ type = "String" }
            }
            typeProperties    = @{
                location = @{
                    type      = "AzureBlobStorageLocation"
                    container = "@dataset().containerName"
                    blobPath  = "@dataset().blobFileName"
                }
            }
            format            = @{
                type = "ParquetFormat"
            }
        }
    }

    # Deploy Parquet sink dataset
    try {
        $parquetObj = ($parquetDs | ConvertTo-Json -Depth 100) | ConvertFrom-Json
        Set-AzDataFactoryV2Dataset `
            -ResourceGroupName $ResourceGroupName `
            -DataFactoryName    $DataFactoryName `
            -Name               $parquetDs.name `
            -Definition         $parquetObj
    }
    catch {
        Write-Host "Failed to deploy Parquet sink dataset: $($parquetDs.name)" -ForegroundColor Red
        Write-Host "Error details: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }

}

# 5) BUILD & DEPLOY ADF PIPES
$parent = @{
    name       = "MasterPipeline"
    properties = @{ activities = @() }
}

foreach ($row in $tableList) {
    $childName = "Copy_$($row.tableName)"
    $child = @{
        name       = $childName
        properties = @{
            parameters = @{
                bqDatasetId   = @{ type = "String" }
                tableName     = @{ type = "String" }
                containerName = @{ type = "String" }
                blobFileName  = @{ type = "String" }
                outputFormat  = @{ type = "String" }
            }
            activities = @(
                @{
                    name           = "If_Parquet"
                    type           = "IfCondition"
                    typeProperties = @{
                        expression        = @{
                            type  = "Expression"
                            value = "@equals(toLower(pipeline().parameters.outputFormat),'parquet')"
                        }
                        ifTrueActivities  = @(
                            @{
                                name           = "Copy_Parquet"
                                type           = "Copy"
                                inputs         = @(@{ referenceName = "BigQueryDataset"; type = "DatasetReference"; parameters = @{
                                            datasetName = "@pipeline().parameters.bqDatasetId"
                                            tableName   = "@pipeline().parameters.tableName"
                                        }
                                    })
                                outputs        = @(@{ referenceName = "BlobSink_Parquet"; type = "DatasetReference"; parameters = @{
                                            containerName = "@pipeline().parameters.containerName"
                                            blobFileName  = "@concat(pipeline().parameters.blobFileName,'_',formatDateTime(utcnow(),'yyyyMMddHHmmss'),'.parquet')"
                                        }
                                    })
                                typeProperties = @{
                                    source = @{ type = "GoogleBigQuerySource" }
                                    sink   = @{ type = "ParquetSink" }
                                }
                            }
                        )
                        ifFalseActivities = @(
                            @{
                                name           = "Copy_JSON"
                                type           = "Copy"
                                inputs         = @(@{ referenceName = "BigQueryDataset"; type = "DatasetReference"; parameters = @{
                                            datasetName = "@pipeline().parameters.datasetName"
                                            tableName   = "@pipeline().parameters.tableName"
                                        }
                                    })
                                outputs        = @(@{ referenceName = "BlobSink_JSON"; type = "DatasetReference"; parameters = @{
                                            containerName = "@pipeline().parameters.containerName"
                                            blobFileName  = "@concat(pipeline().parameters.blobFileName,'_',formatDateTime(utcnow(),'yyyyMMddHHmmss'),'.json')"
                                        }
                                    })
                                typeProperties = @{
                                    source = @{ type = "GoogleBigQuerySource" }
                                    sink   = @{
                                        type           = "JsonSink"
                                        storeSettings  = @{ type = "AzureBlobStorageWriteSettings" }
                                        formatSettings = @{
                                            type            = "JsonWriteSettings"
                                            fileCompression = @{ type = "Gzip"; level = "Optimal" }
                                        }
                                    }
                                }
                            }
                        )
                    }
                }
            )
        }
    }
    # Check if child pipeline exists
    $existingChild = Get-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $childName -ErrorAction SilentlyContinue

    $childObj = ($child | ConvertTo-Json -Depth 100) | ConvertFrom-Json

    if ($existingChild) {
        # Update existing pipeline
        Write-Host "Updating existing pipeline: $childName" -ForegroundColor Yellow
        Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $childName -Definition $childObj
    }
    else {
        # Create new pipeline
        Write-Host "Creating new pipeline: $childName" -ForegroundColor Green
        Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $childName -Definition $childObj
    }

    # add child to parent
    $parent.properties.activities += @{
        name           = "Run_$childName"
        type           = "ExecutePipeline"
        typeProperties = @{
            pipeline   = @{ referenceName = $childName; type = "PipelineReference" }
            parameters = @{
                bqDatasetId   = $row.bqDatasetId
                tableName     = $row.tableName
                containerName = $row.containerName
                blobFileName  = $row.tableName
                outputFormat  = $row.outputFormat
            }
        }
    }
}
# deploy parent
$parentObj = $parent | ConvertTo-Json -Depth 100 | ConvertFrom-Json
Set-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name $parent.name -Definition $parentObj
