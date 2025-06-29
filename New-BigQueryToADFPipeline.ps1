# Prereqs:
#  - Az PowerShell module installed & logged in (Connect-AzAccount)
# —— CONFIG —— #
param(
    [Parameter(Mandatory=$true, HelpMessage="Azure Resource Group name.")]
    [string]$rg,

    [Parameter(Mandatory=$true, HelpMessage="Globally unique Azure Storage Account name.")]
    [string]$saName,

    [Parameter(Mandatory=$true, HelpMessage="Azure region/location (e.g., eastus).")]
    [string]$location,

    [Parameter(Mandatory=$true, HelpMessage="Azure Data Factory name.")]
    [string]$dataFactory,

    [Parameter(Mandatory=$true, HelpMessage="Path to CSV file with BigQuery table metadata.")]
    [string]$CSVFile,

    [Parameter(Mandatory=$true, HelpMessage="Output format: 'json' or 'parquet'.")]
    [ValidateSet("json", "parquet")]
    [string]$OutputFormat
)

if(-not $(Get-Module -Name Az -ErrorAction SilentlyContinue)) {
    Write-Host "Az module not found. Installing..." -ForegroundColor Yellow
    Install-Module -Name Az -AllowClobber -Force -SkipPublisherCheck -Scope CurrentUser
}

if(-not $(Get-AzResourceGroup -Name $rg -ErrorAction SilentlyContinue)) {
    Write-Host "Resource group '$rg' does not exist. Please create it first." -ForegroundColor Red
    # 1) CREATE STORAGE ACCOUNT + FIREWALL
    New-AzResourceGroup -Name $rg -Location $location -Force
}

if(-not $(Get-AzStorageAccount -ResourceGroupName $rg -Name $saName -ErrorAction SilentlyContinue)) {
    Write-Host "Storage account '$saName' does not exist. Creating it now..." -ForegroundColor Yellow
    New-AzStorageAccount `
    -ResourceGroupName $rg `
    -Name $saName `
    -Location $location `
    -SkuName Standard_LRS `
    -Kind StorageV2 `
    -EnableHttpsTrafficOnly $true

# Deny all networks except trusted Azure services (Bypass=AzureServices)
    Update-AzStorageAccountNetworkRuleSet `
    -ResourceGroupName $rg `
    -Name $saName `
    -DefaultAction Deny `
    -Bypass AzureServices
} else {
    Write-Host "Storage account '$saName' already exists." -ForegroundColor Green
}

if(-not $(Get-AzDataFactoryV2 -ResourceGroupName $rg -Name $dataFactory -ErrorAction SilentlyContinue)) {
    Write-Host "Data Factory '$dataFactory' does not exist. Creating it now..." -ForegroundColor Yellow
    New-AzDataFactoryV2 `
    -ResourceGroupName $rg `
    -Name $dataFactory `
    -Location $location `
    -IdentityType SystemAssigned
} else {
    Write-Host "Data Factory '$dataFactory' already exists." -ForegroundColor Green
}

if(-not $(Test-Path -Path $CSVFile)) {
    Write-Host "CSV file '$CSVFile' does not exist. Please provide a valid path." -ForegroundColor Red
    Write-Host "If running from Cloud Shell, click the 'Upload/Download files' icon to upload your CSV file.`n" -ForegroundColor Yellow
    $CSVFile = Read-Host "Enter the path to your BigQuery tables CSV file"
} 

$tables = Import-Csv -Path $CSVFile

# 3) CREATE CONTAINERS per table 📦 using Entra ID authentication
$ctx = New-AzStorageContext -StorageAccountName $saName -UseConnectedAccount

$tableList = foreach ($t in $tables) {
    $cname = $t.tableId
    $container = Get-AzStorageContainer -Name $cname -Context $ctx -ErrorAction SilentlyContinue
    if (-not $container) {
        Write-Host "Creating container: $cname"
        New-AzStorageContainer -Name $cname -Context $ctx | Out-Null
    } else {
        Write-Host "Container '$cname' already exists." -ForegroundColor Green
    }

    [PSCustomObject]@{
        tableName    = $cname
        datasetName  = $datasetId
        containerName= $cname
        blobFileName = "$cname"                  # actual extension & timestamp via ADF dynamic expr
        outputFormat = $OutputFormat                  # or "parquet"
    }
}

# 4) BUILD & DEPLOY ADF PIPES
$parent = @{
    name       = "MasterPipeline"
    properties = @{ activities = @() }
}

foreach ($row in $tableList) {
    $childName = "Copy_$($row.tableName)"
    $child     = @{
        name       = $childName
        properties = @{
            parameters = @{
                datasetName   = @{ type="String" }
                tableName     = @{ type="String" }
                containerName = @{ type="String" }
                blobFileName  = @{ type="String" }
                outputFormat  = @{ type="String" }
            }
            activities = @(
                @{
                    name           = "If_Parquet"
                    type           = "IfCondition"
                    typeProperties = @{
                        expression = @{
                            type  = "Expression"
                            value = "@equals(toLower(pipeline().parameters.outputFormat),'parquet')"
                        }
                        ifTrueActivities = @(
                            @{
                                name       = "Copy_Parquet"
                                type       = "Copy"
                                inputs     = @(@{ referenceName="BigQueryDataset"; type="DatasetReference"; parameters=@{
                                    datasetName="@pipeline().parameters.datasetName"
                                    tableName  ="@pipeline().parameters.tableName"
                                }})
                                outputs    = @(@{ referenceName="BlobSink_Parquet"; type="DatasetReference"; parameters=@{
                                    containerName="@pipeline().parameters.containerName"
                                    blobFileName ="@concat(pipeline().parameters.blobFileName,'_',formatDateTime(utcnow(),'yyyyMMddHHmmss'),'.parquet')"
                                }})
                                typeProperties = @{
                                    source = @{ type="GoogleBigQuerySource" }
                                    sink   = @{ type="ParquetSink" }
                                }
                            }
                        )
                        ifFalseActivities = @(
                            @{
                                name       = "Copy_JSON"
                                type       = "Copy"
                                inputs     = @(@{ referenceName="BigQueryDataset"; type="DatasetReference"; parameters=@{
                                    datasetName="@pipeline().parameters.datasetName"
                                    tableName  ="@pipeline().parameters.tableName"
                                }})
                                outputs    = @(@{ referenceName="BlobSink_JSON"; type="DatasetReference"; parameters=@{
                                    containerName="@pipeline().parameters.containerName"
                                    blobFileName ="@concat(pipeline().parameters.blobFileName,'_',formatDateTime(utcnow(),'yyyyMMddHHmmss'),'.json')"
                                }})
                                typeProperties = @{
                                    source = @{ type="GoogleBigQuerySource" }
                                    sink   = @{
                                        type          = "JsonSink"
                                        storeSettings = @{ type="AzureBlobStorageWriteSettings" }
                                        formatSettings= @{
                                            type            = "JsonWriteSettings"
                                            fileCompression = @{ type="Gzip"; level="Optimal" }
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
    $existingChild = Get-AzDataFactoryV2Pipeline -ResourceGroupName $rg -DataFactoryName $dataFactory -Name $childName -ErrorAction SilentlyContinue

    $childObj = ($child | ConvertTo-Json -Depth 100) | ConvertFrom-Json

    if ($existingChild) {
        # Update existing pipeline
        Write-Host "Updating existing pipeline: $childName" -ForegroundColor Yellow
        Set-AzDataFactoryV2Pipeline -ResourceGroupName $rg -DataFactoryName $dataFactory -Name $childName -Definition $childObj
    } else {
        # Create new pipeline
        Write-Host "Creating new pipeline: $childName" -ForegroundColor Green
        Set-AzDataFactoryV2Pipeline -ResourceGroupName $rg -DataFactoryName $dataFactory -Name $childName -Definition $childObj
    }

    # add child to parent
    $parent.properties.activities += @{
        name           = "Run_$childName"
        type           = "ExecutePipeline"
        typeProperties = @{
            pipeline   = @{ referenceName=$childName; type="PipelineReference" }
            parameters = @{
                datasetName   = $row.datasetName
                tableName     = $row.tableName
                containerName = $row.containerName
                blobFileName  = $row.tableName
                outputFormat  = $row.outputFormat
            }
        }
    }
}

# deploy parent
$parentObj = (($parent | ConvertTo-Json -Depth 100) | ConvertFrom-Json)
Set-AzDataFactoryV2Pipeline -ResourceGroupName $rg -DataFactoryName $dataFactory -Name $parent.name -Definition $parentObj
